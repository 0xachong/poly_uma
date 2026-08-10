package main

import (
	"crypto/subtle"
	"encoding/json"
	"io"
	"net/http"
	"sort"
	"strings"
)

const maxRebalanceRelease = 100

func (s *slaveServer) serveClients(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.adminAuthorized(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	clientsByID := make(map[uint64]downstreamClient)
	for _, hub := range s.hubs {
		for _, client := range hub.clients() {
			clientsByID[client.ID] = client
		}
	}
	clients := make([]downstreamClient, 0, len(clientsByID))
	for _, client := range clientsByID {
		clients = append(clients, client)
	}
	sort.Slice(clients, func(i, j int) bool {
		if clients[i].ConnectedAtMS == clients[j].ConnectedAtMS {
			return clients[i].ID > clients[j].ID
		}
		return clients[i].ConnectedAtMS > clients[j].ConnectedAtMS
	})
	writeJSONResponse(w, http.StatusOK, map[string]any{
		"status": "ok", "node_id": s.nodeID, "count": len(clients), "clients": clients,
	})
}

func (s *slaveServer) serveRebalance(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.adminAuthorized(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	var request struct {
		Count int `json:"count"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&request); err != nil || request.Count < 1 || request.Count > maxRebalanceRelease {
		http.Error(w, "count must be between 1 and 100", http.StatusBadRequest)
		return
	}
	released := 0
	// 优先释放主要交易流；其他低流量兼容流仅在数量不足时参与回流。
	for _, key := range []string{compactTradeV5PBKey, compactTradeV5JSONKey, compactTradeV4Key, compactEventsPath, proposedPath, disputedPath} {
		if released >= request.Count {
			break
		}
		released += s.hubs[key].releaseSubscribers(request.Count - released)
	}
	writeJSONResponse(w, http.StatusOK, map[string]any{
		"status": "ok", "node_id": s.nodeID, "requested": request.Count, "released": released,
	})
}

func (h *relayHub) releaseSubscribers(count int) int {
	if count <= 0 {
		return 0
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	released := 0
	for sub := range h.subscribers {
		delete(h.subscribers, sub)
		close(sub.send)
		released++
		if released >= count {
			break
		}
	}
	return released
}

func (s *slaveServer) adminAuthorized(r *http.Request) bool {
	provided := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	return s.adminToken != "" && subtle.ConstantTimeCompare([]byte(provided), []byte(s.adminToken)) == 1
}

func writeJSONResponse(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
