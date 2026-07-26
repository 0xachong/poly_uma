package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestProxyForwardsHTTPAndMarksResponse(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/uma/v1/proposed" || r.URL.RawQuery != "limit=1" {
			t.Fatalf("unexpected target: %s?%s", r.URL.Path, r.URL.RawQuery)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"count":1}`))
	}))
	defer upstream.Close()

	target, _ := url.Parse(upstream.URL)
	slave := httptest.NewServer(newSlaveServer(target, 8))
	defer slave.Close()

	response, err := http.Get(slave.URL + "/uma/v1/proposed?limit=1")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	if string(body) != `{"count":1}` {
		t.Fatalf("body = %s", body)
	}
	if response.Header.Get("X-UMA-Slave") != "true" {
		t.Fatalf("missing slave response header")
	}
}

func TestHTTPQueryCacheCoalescesAndCaches(t *testing.T) {
	var requests atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		_, _ = w.Write([]byte(`{"count":1}`))
	}))
	defer upstream.Close()

	target, _ := url.Parse(upstream.URL)
	cache := newHTTPQueryCache(target, 2, time.Minute, time.Minute)
	first := httptest.NewRecorder()
	cache.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/uma/v1/proposed?cursor=1", nil))
	second := httptest.NewRecorder()
	cache.ServeHTTP(second, httptest.NewRequest(http.MethodGet, "/uma/v1/proposed?cursor=1", nil))

	if requests.Load() != 1 {
		t.Fatalf("upstream requests=%d", requests.Load())
	}
	if second.Header().Get("X-UMA-Slave-Cache") != "HIT" {
		t.Fatalf("cache header=%q", second.Header().Get("X-UMA-Slave-Cache"))
	}
}

func TestSharedUpstreamBroadcastsToMultipleClients(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	var upstreamConnections atomic.Int64
	send := make(chan []byte, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != proposedPath {
			http.NotFound(w, r)
			return
		}
		connection, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		upstreamConnections.Add(1)
		defer connection.Close()
		payload := <-send
		_ = connection.WriteMessage(websocket.TextMessage, payload)
		<-r.Context().Done()
	}))
	defer upstream.Close()

	target, _ := url.Parse(upstream.URL)
	handler := newSlaveServer(target, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	handler.run(ctx)
	slave := httptest.NewServer(handler)
	defer slave.Close()

	waitFor(t, 2*time.Second, func() bool {
		return handler.hubs[proposedPath].upstreamUp.Load()
	})

	wsURL := "ws" + strings.TrimPrefix(slave.URL, "http") + proposedPath
	first := dialWS(t, wsURL)
	defer first.Close()
	second := dialWS(t, wsURL)
	defer second.Close()

	send <- []byte(`{"transaction_hash":"0xtest","broadcast_at_ms":100}`)
	firstMessage := readJSON(t, first)
	secondMessage := readJSON(t, second)

	if upstreamConnections.Load() != 1 {
		t.Fatalf("upstream connections = %d, want 1", upstreamConnections.Load())
	}
	for index, message := range []map[string]any{firstMessage, secondMessage} {
		if message["transaction_hash"] != "0xtest" {
			t.Fatalf("client %d payload = %#v", index, message)
		}
		if message["slave_received_at_ms"] == nil || message["slave_broadcast_at_ms"] == nil {
			t.Fatalf("client %d missing relay timestamps: %#v", index, message)
		}
	}
}

func TestRelayTimestampsPreserveMasterTimestamp(t *testing.T) {
	payload := addRelayTimestamps([]byte(`{"broadcast_at_ms":1234}`), 2000)
	var message map[string]any
	if err := json.Unmarshal(payload, &message); err != nil {
		t.Fatal(err)
	}
	if message["broadcast_at_ms"].(float64) != 1234 {
		t.Fatalf("master broadcast timestamp changed: %s", payload)
	}
	if message["slave_received_at_ms"].(float64) != 2000 {
		t.Fatalf("slave received timestamp = %s", payload)
	}
}

func TestReleaseClosesOnlyRequestedSubscribers(t *testing.T) {
	target, _ := url.Parse("http://127.0.0.1")
	hub := newRelayHub(target, proposedPath, 8)
	subscribers := []*subscriber{hub.subscribe(), hub.subscribe(), hub.subscribe()}

	if released := hub.release(2); released != 2 {
		t.Fatalf("released=%d, want 2", released)
	}
	if count := hub.subscriberCount(); count != 1 {
		t.Fatalf("subscriber count=%d, want 1", count)
	}
	closed := 0
	for _, subscriber := range subscribers {
		select {
		case _, ok := <-subscriber.send:
			if !ok {
				closed++
				if subscriber.closeCode != websocket.CloseServiceRestart {
					t.Fatalf("close code=%d", subscriber.closeCode)
				}
			}
		default:
		}
	}
	if closed != 2 {
		t.Fatalf("closed=%d, want 2", closed)
	}
}

func dialWS(t *testing.T, target string) *websocket.Conn {
	t.Helper()
	connection, _, err := websocket.DefaultDialer.Dial(target, nil)
	if err != nil {
		t.Fatal(err)
	}
	return connection
}

func readJSON(t *testing.T, connection *websocket.Conn) map[string]any {
	t.Helper()
	_ = connection.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, payload, err := connection.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	var message map[string]any
	if err := json.Unmarshal(payload, &message); err != nil {
		t.Fatal(err)
	}
	return message
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not reached")
}
