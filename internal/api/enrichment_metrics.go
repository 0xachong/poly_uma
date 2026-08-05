package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/polymas/poly_uma/internal/store"
)

type enrichmentDimension struct {
	Key          string  `json:"key"`
	Label        string  `json:"label"`
	Total        int     `json:"total"`
	Misses       int     `json:"misses"`
	MappingMiss  int     `json:"mapping_misses"`
	SnapshotMiss int     `json:"snapshot_misses"`
	MissRate     float64 `json:"miss_rate"`
}

type enrichmentMiss struct {
	ObservedAtMS int64             `json:"observed_at_ms"`
	EventType    string            `json:"event_type"`
	Kind         string            `json:"kind"`
	MarketID     string            `json:"market_id"`
	ConditionID  string            `json:"condition_id,omitempty"`
	TxHash       string            `json:"transaction_hash"`
	LogIndex     int               `json:"log_index"`
	BlockNumber  uint64            `json:"block_number"`
	Tags         []store.MarketTag `json:"tags"`
}

type enrichmentMetricsResponse struct {
	Hours       int                   `json:"hours"`
	GeneratedAt int64                 `json:"generated_at_ms"`
	Overall     enrichmentDimension   `json:"overall"`
	ByEvent     []enrichmentDimension `json:"by_event"`
	ByTag       []enrichmentDimension `json:"by_tag"`
	RecentMiss  []enrichmentMiss      `json:"recent_misses"`
}

type enrichmentCounter struct {
	total, misses, mappingMiss, snapshotMiss int
}

func makeEnrichmentMetricsHandler(db *store.SQLite, marketDB *store.MarketSQLite) gin.HandlerFunc {
	type cacheEntry struct {
		at   time.Time
		data enrichmentMetricsResponse
	}
	var mu sync.Mutex
	cache := make(map[int]cacheEntry)
	return func(c *gin.Context) {
		hours := 5
		if value, err := strconv.Atoi(c.Query("hours")); err == nil && value >= 1 && value <= 168 {
			hours = value
		}
		mu.Lock()
		cached, ok := cache[hours]
		mu.Unlock()
		if ok && time.Since(cached.at) < 15*time.Second {
			c.JSON(http.StatusOK, cached.data)
			return
		}
		result, err := buildEnrichmentMetrics(db, marketDB, hours, time.Now())
		if err != nil {
			jsonError(c, http.StatusInternalServerError, err.Error())
			return
		}
		mu.Lock()
		cache[hours] = cacheEntry{at: time.Now(), data: result}
		mu.Unlock()
		c.JSON(http.StatusOK, result)
	}
}

func buildEnrichmentMetrics(db *store.SQLite, marketDB *store.MarketSQLite, hours int, now time.Time) (enrichmentMetricsResponse, error) {
	since := now.Add(-time.Duration(hours) * time.Hour)
	events, err := db.ScanEventsSince(since.Unix())
	if err != nil {
		return enrichmentMetricsResponse{}, fmt.Errorf("query enrichment events: %w", err)
	}
	incidents, err := marketDB.ListMarketEnrichmentIncidentsSince(since.Add(-10 * time.Minute).UnixMilli())
	if err != nil {
		return enrichmentMetricsResponse{}, fmt.Errorf("query enrichment incidents: %w", err)
	}
	misses := make(map[string]store.MarketEnrichmentIncident)
	missKinds := make(map[string]string)
	for _, incident := range incidents {
		if incident.Stage != "miss_observed" || incident.ObservedAtMS < since.UnixMilli() {
			continue
		}
		key := fmt.Sprintf("%s:%d", strings.ToLower(incident.TxHash), incident.LogIndex)
		if _, exists := misses[key]; exists {
			continue
		}
		misses[key] = incident
		var detail struct {
			Kind string `json:"kind"`
		}
		_ = json.Unmarshal([]byte(incident.DetailJSON), &detail)
		missKinds[key] = detail.Kind
	}
	marketSet := make(map[string]struct{})
	for _, event := range events {
		if (event.EventType == "propose" || event.EventType == "dispute") && event.MarketID != "" {
			marketSet[event.MarketID] = struct{}{}
		}
	}
	for _, miss := range misses {
		if miss.MarketID != "" {
			marketSet[miss.MarketID] = struct{}{}
		}
	}
	marketIDs := make([]string, 0, len(marketSet))
	for id := range marketSet {
		marketIDs = append(marketIDs, id)
	}
	snapshots, err := marketDB.LoadMarketSnapshotsByIDs(marketIDs)
	if err != nil {
		return enrichmentMetricsResponse{}, fmt.Errorf("query market tags: %w", err)
	}

	overall := &enrichmentCounter{}
	byEvent := make(map[string]*enrichmentCounter)
	byTag := make(map[string]*enrichmentCounter)
	tagLabels := make(map[string]string)
	for _, event := range events {
		if event.EventType != "propose" && event.EventType != "dispute" {
			continue
		}
		key := fmt.Sprintf("%s:%d", strings.ToLower(event.TxHash), event.LogIndex)
		kind, missed := missKinds[key]
		updateEnrichmentCounter(overall, missed, kind)
		if byEvent[event.EventType] == nil {
			byEvent[event.EventType] = &enrichmentCounter{}
		}
		updateEnrichmentCounter(byEvent[event.EventType], missed, kind)
		for tagKey, label := range metricTags(snapshots[event.MarketID]) {
			if byTag[tagKey] == nil {
				byTag[tagKey] = &enrichmentCounter{}
			}
			tagLabels[tagKey] = label
			updateEnrichmentCounter(byTag[tagKey], missed, kind)
		}
	}
	recent := make([]enrichmentMiss, 0, len(misses))
	for key, incident := range misses {
		snapshot := snapshots[incident.MarketID]
		recent = append(recent, enrichmentMiss{ObservedAtMS: incident.ObservedAtMS, EventType: incident.EventType,
			Kind: missKinds[key], MarketID: incident.MarketID, ConditionID: incident.ConditionID, TxHash: incident.TxHash,
			LogIndex: incident.LogIndex, BlockNumber: incident.BlockNumber, Tags: snapshot.Tags})
	}
	sort.Slice(recent, func(i, j int) bool { return recent[i].ObservedAtMS > recent[j].ObservedAtMS })
	if len(recent) > 100 {
		recent = recent[:100]
	}
	return enrichmentMetricsResponse{Hours: hours, GeneratedAt: now.UnixMilli(), Overall: dimension("all", "全部事件", overall),
		ByEvent: dimensions(byEvent, nil), ByTag: dimensions(byTag, tagLabels), RecentMiss: recent}, nil
}

func metricTags(snapshot store.MarketSnapshot) map[string]string {
	out := make(map[string]string)
	for _, tag := range snapshot.Tags {
		key := strings.TrimSpace(tag.ID)
		if key == "" {
			continue
		}
		out[key] = key
	}
	if len(out) == 0 {
		out["unknown"] = "无 Tag / 快照已退出"
	}
	return out
}

func updateEnrichmentCounter(counter *enrichmentCounter, missed bool, kind string) {
	counter.total++
	if !missed {
		return
	}
	counter.misses++
	if kind == "condition_mapping_miss" {
		counter.mappingMiss++
	} else {
		counter.snapshotMiss++
	}
}

func dimension(key, label string, counter *enrichmentCounter) enrichmentDimension {
	rate := 0.0
	if counter.total > 0 {
		rate = float64(counter.misses) * 100 / float64(counter.total)
	}
	return enrichmentDimension{Key: key, Label: label, Total: counter.total, Misses: counter.misses,
		MappingMiss: counter.mappingMiss, SnapshotMiss: counter.snapshotMiss, MissRate: rate}
}

func dimensions(values map[string]*enrichmentCounter, labels map[string]string) []enrichmentDimension {
	out := make([]enrichmentDimension, 0, len(values))
	for key, counter := range values {
		label := key
		if labels != nil && labels[key] != "" {
			label = labels[key]
		}
		out = append(out, dimension(key, label, counter))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Misses != out[j].Misses {
			return out[i].Misses > out[j].Misses
		}
		if out[i].Total != out[j].Total {
			return out[i].Total > out[j].Total
		}
		return out[i].Label < out[j].Label
	})
	return out
}
