package main

import (
	"crypto/subtle"
	"encoding/json"
	"io"
	"net/http"
	"sort"
	"strings"
)

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
	if err := json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&request); err != nil ||
		request.Count < 1 || request.Count > 1000 {
		http.Error(w, "count must be between 1 and 1000", http.StatusBadRequest)
		return
	}

	released := 0
	for released < request.Count {
		var largest *relayHub
		largestCount := 0
		for _, hub := range s.hubs {
			if count := hub.subscriberCount(); count > largestCount {
				largest = hub
				largestCount = count
			}
		}
		if largest == nil {
			break
		}
		if largest.release(1) == 0 {
			break
		}
		released++
	}
	writeJSONResponse(w, http.StatusOK, map[string]any{
		"status":    "ok",
		"node_id":   s.nodeID,
		"requested": request.Count,
		"released":  released,
	})
}

func (s *slaveServer) serveClients(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.adminAuthorized(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	clients := make([]downstreamClient, 0)
	for _, hub := range s.hubs {
		clients = append(clients, hub.clients()...)
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

func (s *slaveServer) adminAuthorized(r *http.Request) bool {
	provided := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	return s.adminToken != "" &&
		subtle.ConstantTimeCompare([]byte(provided), []byte(s.adminToken)) == 1
}

func writeJSONResponse(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
