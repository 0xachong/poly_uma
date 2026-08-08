package main

import (
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"sort"
	"strings"
)

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

func (s *slaveServer) adminAuthorized(r *http.Request) bool {
	provided := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	return s.adminToken != "" && subtle.ConstantTimeCompare([]byte(provided), []byte(s.adminToken)) == 1
}

func writeJSONResponse(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
