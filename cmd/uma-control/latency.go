package main

import (
	"crypto/subtle"
	"encoding/json"
	"io"
	"net/http"
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
	GeneratedAtMS int64                                  `json:"generated_at_ms"`
	IntervalMS    int64                                  `json:"interval_ms"`
	Overall       map[string]latencyQuantiles            `json:"overall"`
	Nodes         map[string]map[string]latencyQuantiles `json:"nodes"`
}

type latencyHistoryPoint struct {
	GeneratedAtMS int64                       `json:"generated_at_ms"`
	Overall       map[string]latencyQuantiles `json:"overall"`
}

type latencyStore struct {
	mu      sync.RWMutex
	limit   int
	latest  latencyWindow
	history []latencyHistoryPoint
}

func newLatencyStore(limit int) *latencyStore {
	return &latencyStore{limit: limit}
}

func (s *latencyStore) add(window latencyWindow) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.latest = window
	s.history = append(s.history, latencyHistoryPoint{
		GeneratedAtMS: window.GeneratedAtMS,
		Overall:       window.Overall,
	})
	if len(s.history) > s.limit {
		s.history = append([]latencyHistoryPoint(nil), s.history[len(s.history)-s.limit:]...)
	}
}

func (s *latencyStore) snapshot() (latencyWindow, []latencyHistoryPoint) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	history := append([]latencyHistoryPoint(nil), s.history...)
	return s.latest, history
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
	latest, history := c.latency.snapshot()
	writeJSON(w, http.StatusOK, map[string]any{
		"generated_at_ms": time.Now().UnixMilli(),
		"latest":          latest,
		"history":         history,
	})
}
