package api

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/polymas/poly_uma/internal/store"
)

func dialTypedTestWS(t *testing.T, mem *store.MemReplica, eventType, query string) *websocket.Conn {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/ws", makeWsTypeHandler(mem, eventType))
	server := httptest.NewServer(r)
	t.Cleanup(server.Close)
	url := "ws" + strings.TrimPrefix(server.URL, "http") + "/ws" + query
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	time.Sleep(10 * time.Millisecond)
	return conn
}

func dialTestWS(t *testing.T, mem *store.MemReplica, query string) *websocket.Conn {
	return dialTypedTestWS(t, mem, "propose", query)
}

func TestLegacyWSRemainsSingleEvent(t *testing.T) {
	mem := store.NewMemReplica()
	conn := dialTestWS(t, mem, "")
	mem.BroadcastNew("propose", store.EventRow{EventType: "propose", ConditionID: "0xA", TxHash: "0xtx", BlockNumber: 1})
	_, payload, err := conn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	var message map[string]any
	if err := json.Unmarshal(payload, &message); err != nil {
		t.Fatal(err)
	}
	if message["processing_key"] != "0xa:propose" || message["message_type"] != nil {
		t.Fatalf("legacy message changed: %s", payload)
	}
}

func TestBatchWSGroupsSameTransaction(t *testing.T) {
	t.Setenv("WS_BATCH_ENABLE", "1")
	mem := store.NewMemReplica()
	conn := dialTestWS(t, mem, "?batch=true")
	snapshot := &store.MarketSnapshot{MarketID: "1", ConditionID: "0xA", Question: "Question", Active: true}
	mem.BroadcastNew("propose", store.EventRow{EventType: "propose", ConditionID: "0xA", TxHash: "0xtx", BlockNumber: 1, LogIndex: 2, Market: snapshot})
	mem.BroadcastNew("propose", store.EventRow{EventType: "propose", ConditionID: "0xB", TxHash: "0xtx", BlockNumber: 1, LogIndex: 3, Market: snapshot})
	_, payload, err := conn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	var message struct {
		MessageType string           `json:"message_type"`
		Events      []map[string]any `json:"events"`
	}
	if err := json.Unmarshal(payload, &message); err != nil {
		t.Fatal(err)
	}
	if message.MessageType != "uma_event_batch" || len(message.Events) != 2 {
		t.Fatalf("batch message=%s", payload)
	}
	if message.Events[0]["market"] == nil {
		t.Fatalf("active catalog snapshot missing: %s", payload)
	}
}

func TestUnifiedBatchWSIncludesProposeAndDispute(t *testing.T) {
	t.Setenv("WS_BATCH_ENABLE", "1")
	mem := store.NewMemReplica()
	conn := dialTypedTestWS(t, mem, "events", "?batch=true")
	mem.BroadcastNew("propose", store.EventRow{EventType: "propose", ConditionID: "0xA", TxHash: "0xtx", BlockNumber: 1, LogIndex: 2})
	mem.BroadcastNew("dispute", store.EventRow{EventType: "dispute", ConditionID: "0xB", TxHash: "0xtx", BlockNumber: 1, LogIndex: 3})
	_, payload, err := conn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	var message struct {
		MessageType string           `json:"message_type"`
		EventType   string           `json:"event_type"`
		Events      []map[string]any `json:"events"`
	}
	if err := json.Unmarshal(payload, &message); err != nil {
		t.Fatal(err)
	}
	if message.MessageType != "uma_event_batch" || message.EventType != "" || len(message.Events) != 2 {
		t.Fatalf("unified batch=%s", payload)
	}
	if message.Events[0]["event_type"] != "propose" || message.Events[1]["event_type"] != "dispute" {
		t.Fatalf("unified event types=%s", payload)
	}
}

func TestEventDTOAddsProcessingKeyWithoutChangingLegacyFields(t *testing.T) {
	data := eventDTO(store.EventRow{EventType: "propose", ConditionID: "0xAbC", MarketID: "12"})
	if got := data["processing_key"]; got != "0xabc:propose" {
		t.Fatalf("processing_key=%v", got)
	}
	if got := data["condition_id"]; got != "0xAbC" {
		t.Fatalf("condition_id changed: %v", got)
	}
}
func TestLookupLRUEvictsAtCapacity(t *testing.T) {
	cache := newLookupLRU(2, time.Minute)
	cache.set("one", 1)
	cache.set("two", 2)
	cache.set("three", 3)

	if _, ok := cache.get("one"); ok {
		t.Fatal("oldest entry was not evicted")
	}
	if got, ok := cache.get("two"); !ok || got != 2 {
		t.Fatalf("second entry = %v, %t", got, ok)
	}
	if got, ok := cache.get("three"); !ok || got != 3 {
		t.Fatalf("third entry = %v, %t", got, ok)
	}
}

func TestLookupLRURemovesExpiredEntry(t *testing.T) {
	cache := newLookupLRU(2, time.Nanosecond)
	cache.set("expired", 1)
	time.Sleep(time.Millisecond)
	if _, ok := cache.get("expired"); ok {
		t.Fatal("expired entry was returned")
	}
	if len(cache.items) != 0 || cache.ll.Len() != 0 {
		t.Fatalf("expired entry retained: items=%d list=%d", len(cache.items), cache.ll.Len())
	}
}
