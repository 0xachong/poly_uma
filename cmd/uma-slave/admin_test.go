package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func TestAdminClientsRequiresBearerAndReturnsCanonicalURI(t *testing.T) {
	target, _ := url.Parse("http://127.0.0.1")
	server := newSlaveServer(target, 8)
	server.adminToken = "secret"
	hub := server.hubs[compactTradeV5PBKey]
	sub := hub.subscribe(1, "/uma/v2/ws/events?batch=true&compression=none&encoding=protobuf&format=compact_trade&schema_version=5", "203.0.113.7", "1234", time.Now(), nil)
	defer hub.unsubscribe(sub)

	unauthorized := httptest.NewRecorder()
	server.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/slave/admin/clients", nil))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status=%d", unauthorized.Code)
	}

	request := httptest.NewRequest(http.MethodGet, "/slave/admin/clients", nil)
	request.Header.Set("Authorization", "Bearer secret")
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var body struct {
		Count   int                `json:"count"`
		Clients []downstreamClient `json:"clients"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Count != 1 || len(body.Clients) != 1 || body.Clients[0].IP != "203.0.113.7" || body.Clients[0].Stream == "" {
		t.Fatalf("body=%+v", body)
	}
}

func TestCanonicalClientURISortsQuery(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/uma/v2/ws/events?schema_version=5&batch=true&encoding=protobuf", nil)
	got := canonicalClientURI(request)
	want := "/uma/v2/ws/events?batch=true&encoding=protobuf&schema_version=5"
	if got != want {
		t.Fatalf("got=%q want=%q", got, want)
	}
}

func TestAdminRebalanceReleasesRequestedSubscribers(t *testing.T) {
	target, _ := url.Parse("http://127.0.0.1")
	server := newSlaveServer(target, 8)
	server.adminToken = "secret"
	hub := server.hubs[compactTradeV5PBKey]
	for id := uint64(1); id <= 3; id++ {
		hub.subscribe(id, "/uma/v2/ws/events", "203.0.113.7", "1234", time.Now(), nil)
	}

	request := httptest.NewRequest(http.MethodPost, "/slave/admin/rebalance", bytes.NewBufferString(`{"count":2}`))
	request.Header.Set("Authorization", "Bearer secret")
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var body struct {
		Released int `json:"released"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Released != 2 || hub.subscriberCount() != 1 {
		t.Fatalf("released=%d subscribers=%d", body.Released, hub.subscriberCount())
	}
}
