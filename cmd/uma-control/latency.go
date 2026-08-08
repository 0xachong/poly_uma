package main

import (
	"crypto/subtle"
	"encoding/json"
	"io"
	"net/http"
	"sort"
	"sync"
	"time"
)

type latencyQuantiles struct {
	Count         int   `json:"count"`
	ClockAdjusted int   `json:"clock_adjusted"`
	P50           int64 `json:"p50_ms"`
	P90           int64 `json:"p90_ms"`
	P95           int64 `json:"p95_ms"`
	P99           int64 `json:"p99_ms"`
	Max           int64 `json:"max_ms"`
}

type latencyWindow struct {
	GeneratedAtMS      int64                                  `json:"generated_at_ms"`
	IntervalMS         int64                                  `json:"interval_ms"`
	CollectorID        string                                 `json:"collector_id,omitempty"`
	CollectorStatus    string                                 `json:"collector_status,omitempty"`
	Encoding           string                                 `json:"encoding,omitempty"`
	SchemaVersion      int                                    `json:"schema_version,omitempty"`
	SampleCount        int                                    `json:"sample_count"`
	FramesReceived     int64                                  `json:"frames_received"`
	FramesDecoded      int64                                  `json:"frames_decoded"`
	EventsReceived     int64                                  `json:"events_received"`
	RealtimeEvents     int64                                  `json:"realtime_events"`
	DecodeErrors       int64                                  `json:"decode_errors"`
	UnsupportedSchema  int64                                  `json:"unsupported_schema"`
	MissingTimestamp   int64                                  `json:"missing_timestamp"`
	NonRealtimeSkipped int64                                  `json:"non_realtime_skipped"`
	ReportsSucceeded   int64                                  `json:"reports_succeeded"`
	ReportsFailed      int64                                  `json:"reports_failed"`
	LastEventAtMS      int64                                  `json:"last_event_at_ms,omitempty"`
	LastReportAtMS     int64                                  `json:"last_report_at_ms,omitempty"`
	Overall            map[string]latencyQuantiles            `json:"overall"`
	Nodes              map[string]map[string]latencyQuantiles `json:"nodes"`
}

type latencyHistoryPoint struct {
	GeneratedAtMS int64                       `json:"generated_at_ms"`
	SampleCount   int                         `json:"sample_count"`
	Overall       map[string]latencyQuantiles `json:"overall"`
}

type latencyCollectorStatus struct {
	CollectorID     string `json:"collector_id"`
	CollectorStatus string `json:"collector_status"`
	Encoding        string `json:"encoding,omitempty"`
	SchemaVersion   int    `json:"schema_version,omitempty"`
	LastReportAtMS  int64  `json:"last_report_at_ms"`
	LastEventAtMS   int64  `json:"last_event_at_ms,omitempty"`
	ReportAgeMS     int64  `json:"report_age_ms"`
	Status          string `json:"status"`
	FramesReceived  int64  `json:"frames_received"`
	FramesDecoded   int64  `json:"frames_decoded"`
	DecodeErrors    int64  `json:"decode_errors"`
	ReportsFailed   int64  `json:"reports_failed"`
}

type latencySeries struct {
	latest  latencyWindow
	history []latencyHistoryPoint
}

type latencyStore struct {
	mu              sync.RWMutex
	limit           int
	latestCollector string
	collectors      map[string]*latencySeries
}

func newLatencyStore(limit int) *latencyStore {
	return &latencyStore{limit: limit, collectors: make(map[string]*latencySeries)}
}

func (s *latencyStore) add(window latencyWindow) {
	s.mu.Lock()
	defer s.mu.Unlock()
	collectorID := window.CollectorID
	if collectorID == "" {
		collectorID = "legacy"
		window.CollectorID = collectorID
	}
	series := s.collectors[collectorID]
	if series == nil {
		series = &latencySeries{}
		s.collectors[collectorID] = series
	}
	series.latest = window
	if window.SampleCount > 0 {
		series.history = append(series.history, latencyHistoryPoint{
			GeneratedAtMS: window.GeneratedAtMS,
			SampleCount:   window.SampleCount,
			Overall:       window.Overall,
		})
		if len(series.history) > s.limit {
			series.history = append([]latencyHistoryPoint(nil), series.history[len(series.history)-s.limit:]...)
		}
	}
	s.latestCollector = collectorID
}

func (s *latencyStore) snapshot(requested string, nowMS int64) (string, latencyWindow, []latencyHistoryPoint, []latencyCollectorStatus) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	selected := requested
	if selected == "" {
		if _, ok := s.collectors["worker-test-pb-v5"]; ok {
			selected = "worker-test-pb-v5"
		} else {
			selected = s.latestCollector
		}
	}
	var latest latencyWindow
	var history []latencyHistoryPoint
	if series := s.collectors[selected]; series != nil {
		latest = series.latest
		history = append([]latencyHistoryPoint(nil), series.history...)
	}
	statuses := make([]latencyCollectorStatus, 0, len(s.collectors))
	for id, series := range s.collectors {
		age := nowMS - series.latest.GeneratedAtMS
		status := "healthy"
		if age > 30000 {
			status = "offline"
		} else if age > 10000 {
			status = "stale"
		} else if series.latest.FramesReceived > 0 && series.latest.FramesDecoded == 0 {
			status = "decode_error"
		} else if series.latest.SampleCount == 0 {
			status = "idle"
		}
		statuses = append(statuses, latencyCollectorStatus{
			CollectorID: id, CollectorStatus: series.latest.CollectorStatus,
			Encoding: series.latest.Encoding, SchemaVersion: series.latest.SchemaVersion,
			LastReportAtMS: series.latest.GeneratedAtMS, LastEventAtMS: series.latest.LastEventAtMS,
			ReportAgeMS: age, Status: status, FramesReceived: series.latest.FramesReceived,
			FramesDecoded: series.latest.FramesDecoded, DecodeErrors: series.latest.DecodeErrors,
			ReportsFailed: series.latest.ReportsFailed,
		})
	}
	sort.Slice(statuses, func(i, j int) bool { return statuses[i].CollectorID < statuses[j].CollectorID })
	return selected, latest, history, statuses
}

func (c *controller) serveLatencyReport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	provided := r.Header.Get("Authorization")
	expected := "Bearer " + c.latencyToken
	if subtle.ConstantTimeCompare([]byte(provided), []byte(expected)) != 1 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	var window latencyWindow
	if err := json.NewDecoder(io.LimitReader(r.Body, 2<<20)).Decode(&window); err != nil ||
		window.GeneratedAtMS <= 0 || len(window.Nodes) > 20 {
		http.Error(w, "invalid latency report", http.StatusBadRequest)
		return
	}
	if window.IntervalMS <= 0 {
		window.IntervalMS = 3000
	}
	c.latency.add(window)
	w.WriteHeader(http.StatusNoContent)
}

func (c *controller) serveLatency(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	nowMS := time.Now().UnixMilli()
	selected, latest, history, collectors := c.latency.snapshot(r.URL.Query().Get("collector_id"), nowMS)
	writeJSON(w, http.StatusOK, map[string]any{
		"generated_at_ms":       nowMS,
		"selected_collector_id": selected,
		"collectors":            collectors,
		"latest":                latest,
		"history":               history,
	})
}
