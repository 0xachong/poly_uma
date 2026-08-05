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

func TestSlaveLLMsDocumentsCombinedWebSocket(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = io.WriteString(w, "# upstream documentation\n")
	}))
	defer upstream.Close()

	target, _ := url.Parse(upstream.URL)
	slave := httptest.NewServer(newSlaveServer(target, 8))
	defer slave.Close()

	response, err := http.Get(slave.URL + "/llms.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, expected := range []string{"/uma/v1/ws/events", "/uma/v1/ws/proposed", "/uma/v1/ws/disputed"} {
		if !strings.Contains(text, expected) {
			t.Fatalf("llms.txt does not document %s:\n%s", expected, text)
		}
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

func TestCombinedEndpointMultiplexesBothStreams(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	sends := map[string]chan []byte{
		proposedPath: make(chan []byte, 1),
		disputedPath: make(chan []byte, 1),
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		send := sends[r.URL.Path]
		if send == nil {
			http.NotFound(w, r)
			return
		}
		connection, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer connection.Close()
		for {
			select {
			case payload := <-send:
				if err := connection.WriteMessage(websocket.TextMessage, payload); err != nil {
					return
				}
			case <-r.Context().Done():
				return
			}
		}
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
		return handler.hubs[proposedPath].upstreamUp.Load() &&
			handler.hubs[disputedPath].upstreamUp.Load()
	})

	wsURL := "ws" + strings.TrimPrefix(slave.URL, "http") + eventsPath
	connection := dialWS(t, wsURL)
	defer connection.Close()
	waitFor(t, 2*time.Second, func() bool {
		return handler.hubs[proposedPath].subscriberCount() == 1 &&
			handler.hubs[disputedPath].subscriberCount() == 1
	})

	sends[proposedPath] <- []byte(`{"event_type":"propose","transaction_hash":"0xpropose"}`)
	proposed := readJSON(t, connection)
	if proposed["event_type"] != "propose" || proposed["transaction_hash"] != "0xpropose" {
		t.Fatalf("proposed payload = %#v", proposed)
	}

	sends[disputedPath] <- []byte(`{"event_type":"dispute","transaction_hash":"0xdispute"}`)
	disputed := readJSON(t, connection)
	if disputed["event_type"] != "dispute" || disputed["transaction_hash"] != "0xdispute" {
		t.Fatalf("disputed payload = %#v", disputed)
	}

	proposedClients := handler.hubs[proposedPath].clients()
	disputedClients := handler.hubs[disputedPath].clients()
	if len(proposedClients) != 1 || len(disputedClients) != 1 {
		t.Fatalf("client snapshots proposed=%d disputed=%d", len(proposedClients), len(disputedClients))
	}
	if proposedClients[0].ID != disputedClients[0].ID ||
		proposedClients[0].Stream != eventsPath ||
		disputedClients[0].Stream != eventsPath {
		t.Fatalf("combined client metadata proposed=%#v disputed=%#v", proposedClients[0], disputedClients[0])
	}
}

func TestCompactBatchUsesOneSharedUpstreamAndPreservesQuery(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	var upstreamConnections atomic.Int64
	send := make(chan []byte, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != compactEventsPath || r.URL.Query().Get("batch") != "true" || r.URL.Query().Get("format") != "compact" {
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

	waitFor(t, 2*time.Second, func() bool { return handler.hubs[compactEventsPath].upstreamUp.Load() })
	wsURL := "ws" + strings.TrimPrefix(slave.URL, "http") + compactEventsPath + "?batch=true&format=compact"
	first := dialWS(t, wsURL)
	defer first.Close()
	second := dialWS(t, wsURL)
	defer second.Close()

	send <- []byte(`{"message_type":"uma_event_batch","schema_version":3,"payload_format":"compact","events":[{"t":"p","c":"0x1","tags":["sports"]}]}`)
	for _, connection := range []*websocket.Conn{first, second} {
		message := readJSON(t, connection)
		if message["payload_format"] != "compact" || message["slave_received_at_ms"] == nil {
			t.Fatalf("compact relay payload=%#v", message)
		}
	}
	if upstreamConnections.Load() != 1 {
		t.Fatalf("compact upstream connections=%d, want 1", upstreamConnections.Load())
	}
}

func TestNegotiatedLegacyRequestBypassesSingleEventHub(t *testing.T) {
	target, _ := url.Parse("http://127.0.0.1")
	handler := newSlaveServer(target, 8)
	request := httptest.NewRequest(http.MethodGet, proposedPath+"?batch=true", nil)
	if hub := handler.relayHub(request); hub != nil {
		t.Fatal("batch request must preserve its query through compatibility proxy")
	}
	request = httptest.NewRequest(http.MethodGet, proposedPath, nil)
	if hub := handler.relayHub(request); hub != handler.hubs[proposedPath] {
		t.Fatal("default legacy request did not select shared hub")
	}
	request = httptest.NewRequest(http.MethodGet, proposedPath+"?sports_types=moneyline", nil)
	if hub := handler.relayHub(request); hub != nil {
		t.Fatal("sports_types is supported only by the compact v2 stream")
	}
}

func TestCompactTradeSelectsDedicatedSharedHub(t *testing.T) {
	target, _ := url.Parse("http://127.0.0.1")
	handler := newSlaveServer(target, 8)
	request := httptest.NewRequest(http.MethodGet,
		compactEventsPath+"?batch=true&format=compact_trade&sports_types=moneyline,child_moneyline", nil)
	if hub := handler.relayHub(request); hub != handler.hubs[compactTradeKey] {
		t.Fatal("compact_trade request did not select its dedicated shared hub")
	}
	if got := handler.hubs[compactTradeKey].rawQuery; got != "batch=true&format=compact_trade" {
		t.Fatalf("compact_trade upstream query=%q", got)
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
	hub := newRelayHub(target, proposedPath, "", 8)
	subscribers := []*subscriber{
		hub.subscribe(1, proposedPath, "192.0.2.1", "1001", time.Now(), nil),
		hub.subscribe(2, proposedPath, "192.0.2.2", "1002", time.Now(), nil),
		hub.subscribe(3, proposedPath, "192.0.2.3", "1003", time.Now(), nil),
	}

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

func TestRequestClientAddressUsesTrustedProxyHeaders(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, proposedPath, nil)
	request.RemoteAddr = "172.19.1.10:43210"
	request.Header.Set("X-Forwarded-For", "198.51.100.7, 43.135.87.223")
	request.Header.Set("X-Forwarded-Client-Port", "51888")

	ip, port := requestClientAddress(request)
	if ip != "43.135.87.223" || port != "51888" {
		t.Fatalf("address=%s:%s, want 43.135.87.223:51888", ip, port)
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
