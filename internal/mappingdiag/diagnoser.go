package mappingdiag

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/polymas/poly_uma/internal/uma"
)

type Event struct {
	EventType           string          `json:"event_type"`
	ConditionID         string          `json:"condition_id"`
	MarketID            string          `json:"market_id"`
	TransactionHash     string          `json:"transaction_hash"`
	LogIndex            int             `json:"log_index"`
	BlockNumber         uint64          `json:"block_number"`
	Source              string          `json:"source"`
	Market              json.RawMessage `json:"market"`
	BroadcastAtMS       int64           `json:"broadcast_at_ms"`
	MappingWaitMS       int64           `json:"mapping_wait_ms"`
	MappingResolvedAtMS int64           `json:"mapping_resolved_at_ms"`
}

func (e Event) Key() string {
	return strings.ToLower(e.ConditionID) + ":" + e.EventType
}

func (e Event) SnapshotMissing() bool {
	return e.MarketID != "" && e.ConditionID != "" && (len(e.Market) == 0 || string(e.Market) == "null")
}

// MappingWasMissing identifies events that Master held back until the
// market_id -> condition_id mapping was repaired. A missing market snapshot on
// an ordinary realtime event is a different, lower-severity condition.
func (e Event) MappingWasMissing() bool { return e.Source == "delayed_replay" }

type Result struct {
	ObservedAt       time.Time `json:"observed_at"`
	CheckedAt        time.Time `json:"checked_at"`
	Attempt          int       `json:"attempt"`
	ProcessingKey    string    `json:"processing_key"`
	EventType        string    `json:"event_type"`
	ConditionID      string    `json:"condition_id"`
	MarketID         string    `json:"market_id"`
	TransactionHash  string    `json:"transaction_hash"`
	LogIndex         int       `json:"log_index"`
	BlockNumber      uint64    `json:"block_number"`
	Source           string    `json:"source"`
	Classification   string    `json:"classification"`
	ExactHTTPState   string    `json:"exact_lookup_state"`
	ReverseHTTPState string    `json:"condition_lookup_state"`
	ExactConditionID string    `json:"exact_condition_id,omitempty"`
	ReverseMarketID  string    `json:"reverse_market_id,omitempty"`
	ExactUpdatedAt   string    `json:"exact_updated_at,omitempty"`
	ReverseUpdatedAt string    `json:"reverse_updated_at,omitempty"`
	GammaLeadMS      int64     `json:"gamma_lead_ms,omitempty"`
	GammaActive      bool      `json:"gamma_active"`
	GammaClosed      bool      `json:"gamma_closed"`
	Detail           string    `json:"detail,omitempty"`
	CheckElapsedMS   int64     `json:"check_elapsed_ms"`
}

type GammaChecker interface {
	Market(context.Context, string) (uma.GammaMarketMapping, error)
	ByCondition(context.Context, string) ([]uma.GammaMarketMapping, error)
}

type Checker struct{ ProxyURL string }

func (c Checker) Market(ctx context.Context, marketID string) (uma.GammaMarketMapping, error) {
	return uma.FetchGammaMarket(ctx, c.ProxyURL, marketID)
}

func (c Checker) ByCondition(ctx context.Context, conditionID string) ([]uma.GammaMarketMapping, error) {
	return uma.FetchGammaMarketsByConditionID(ctx, c.ProxyURL, conditionID)
}

func Diagnose(ctx context.Context, checker GammaChecker, event Event, observedAt time.Time, attempt int) Result {
	started := time.Now()
	result := Result{ObservedAt: observedAt, CheckedAt: started, Attempt: attempt, ProcessingKey: event.Key(),
		EventType: event.EventType, ConditionID: event.ConditionID, MarketID: event.MarketID,
		TransactionHash: event.TransactionHash, LogIndex: event.LogIndex, BlockNumber: event.BlockNumber, Source: event.Source}

	exact, exactErr := checker.Market(ctx, event.MarketID)
	reverse, reverseErr := checker.ByCondition(ctx, event.ConditionID)
	result.ExactHTTPState, result.ReverseHTTPState = state(exactErr), state(reverseErr)
	result.ExactConditionID, result.ExactUpdatedAt = exact.ConditionID, exact.UpdatedAt
	result.GammaActive, result.GammaClosed = exact.Active, exact.Closed
	latestUpdate, _ := time.Parse(time.RFC3339Nano, exact.UpdatedAt)
	for _, market := range reverse {
		if market.ID == event.MarketID {
			result.ReverseMarketID = market.ID
			result.ReverseUpdatedAt = market.UpdatedAt
			if parsed, err := time.Parse(time.RFC3339Nano, market.UpdatedAt); err == nil && parsed.After(latestUpdate) {
				latestUpdate = parsed
			}
			break
		}
	}
	if result.ReverseMarketID == "" && len(reverse) > 0 {
		result.ReverseMarketID = reverse[0].ID
	}
	if !latestUpdate.IsZero() {
		result.GammaLeadMS = observedAt.Sub(latestUpdate).Milliseconds()
	}

	switch {
	case exactErr == nil && !exact.Active && !exact.Closed:
		result.Classification = "gamma_inactive_open_market"
		result.Detail = "Gamma exact lookup reports active=false and closed=false; this market is intentionally outside Master's active-only catalog"
	case exactErr == nil && strings.EqualFold(exact.ConditionID, event.ConditionID) && result.ReverseMarketID == event.MarketID && result.GammaLeadMS >= 0 && result.GammaLeadMS <= 45_000:
		result.Classification = "catalog_refresh_window_race"
		result.Detail = "Gamma mappings agree and were updated less than 45s before arrival; the market likely landed between Master catalog refreshes"
	case exactErr == nil && strings.EqualFold(exact.ConditionID, event.ConditionID) && result.ReverseMarketID == event.MarketID:
		result.Classification = "active_catalog_coverage_gap"
		result.Detail = "Gamma mappings agree but predate the normal refresh window; investigate catalog scan coverage or snapshot persistence"
	case exactErr == nil && exact.ConditionID != "" && !strings.EqualFold(exact.ConditionID, event.ConditionID):
		result.Classification = "mapping_conflict"
		result.Detail = fmt.Sprintf("Master condition_id differs from Gamma exact lookup (%s)", exact.ConditionID)
	case exactErr != nil && reverseErr == nil && result.ReverseMarketID == event.MarketID:
		result.Classification = "gamma_exact_lookup_inconsistent"
		result.Detail = "condition-id query found the market but exact market-id query failed"
	case exactErr == nil && reverseErr == nil && result.ReverseMarketID != "" && result.ReverseMarketID != event.MarketID:
		result.Classification = "condition_points_to_other_market"
		result.Detail = fmt.Sprintf("condition-id query returned market %s", result.ReverseMarketID)
	case exactErr != nil && reverseErr != nil:
		result.Classification = "gamma_unavailable_or_not_found"
		result.Detail = fmt.Sprintf("exact=%v; reverse=%v", exactErr, reverseErr)
	case exactErr == nil && reverseErr == nil && len(reverse) == 0:
		result.Classification = "condition_not_indexed"
		result.Detail = "exact market exists but condition-id query returned no market"
	default:
		result.Classification = "inconclusive"
		result.Detail = fmt.Sprintf("exact=%v; reverse=%v", exactErr, reverseErr)
	}
	result.CheckElapsedMS = time.Since(started).Milliseconds()
	return result
}

func state(err error) string {
	if err == nil {
		return "ok"
	}
	return err.Error()
}

type Recorder struct {
	mu   sync.Mutex
	file *os.File
}

func NewRecorder(path string) (*Recorder, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o640)
	if err != nil {
		return nil, err
	}
	return &Recorder{file: file}, nil
}

func (r *Recorder) Append(result Result) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	encoded, err := json.Marshal(result)
	if err != nil {
		return err
	}
	if _, err = r.file.Write(append(encoded, '\n')); err != nil {
		return err
	}
	return r.file.Sync()
}

func (r *Recorder) Close() error { return r.file.Close() }

type Stats struct {
	StartedAt       time.Time         `json:"started_at"`
	Connected       bool              `json:"connected"`
	LastConnectedAt time.Time         `json:"last_connected_at,omitempty"`
	LastEventAt     time.Time         `json:"last_event_at,omitempty"`
	EventsTotal     uint64            `json:"events_total"`
	SnapshotHits    uint64            `json:"mapping_ready_events"`
	SnapshotMisses  uint64            `json:"mapping_delayed_events"`
	ChecksTotal     uint64            `json:"checks_total"`
	ChecksDropped   uint64            `json:"checks_dropped"`
	ByCause         map[string]uint64 `json:"by_cause"`
}

type Monitor struct {
	mu    sync.RWMutex
	stats Stats
}

func NewMonitor() *Monitor {
	return &Monitor{stats: Stats{StartedAt: time.Now(), ByCause: make(map[string]uint64)}}
}

func (m *Monitor) Connected(value bool) {
	m.mu.Lock()
	m.stats.Connected = value
	if value {
		m.stats.LastConnectedAt = time.Now()
	}
	m.mu.Unlock()
}

func (m *Monitor) Event(miss bool) {
	m.mu.Lock()
	m.stats.EventsTotal++
	m.stats.LastEventAt = time.Now()
	if miss {
		m.stats.SnapshotMisses++
	} else {
		m.stats.SnapshotHits++
	}
	m.mu.Unlock()
}

func (m *Monitor) Result(cause string) {
	m.mu.Lock()
	m.stats.ChecksTotal++
	m.stats.ByCause[cause]++
	m.mu.Unlock()
}

func (m *Monitor) Dropped() {
	m.mu.Lock()
	m.stats.ChecksDropped++
	m.mu.Unlock()
}

func (m *Monitor) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(m.stats)
}
