package api

import (
	"encoding/json"
	"fmt"
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

func TestCompactBatchWSIsOptInAndKeepsRoutingFields(t *testing.T) {
	t.Setenv("WS_BATCH_ENABLE", "1")
	mem := store.NewMemReplica()
	conn := dialTypedTestWS(t, mem, "events", "?batch=true&format=compact")
	snapshot := &store.MarketSnapshot{
		MarketID: "12", ConditionID: "0xA", Question: "Market question",
		Description: "must not cross the compact wire", PolymarketEventID: "34",
		PolymarketEventTitle: "Event title", SportsMarketType: "moneyline",
		Tags: []store.MarketTag{{ID: "1"}},
	}
	mem.BroadcastNew("propose", store.EventRow{EventType: "propose", ConditionID: "0xA", MarketID: "12", Price: "1", TxHash: "0xtx", BlockNumber: 9, LogIndex: 7, Timestamp: 8, Market: snapshot})
	_, payload, err := conn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	var message struct {
		SchemaVersion int              `json:"schema_version"`
		PayloadFormat string           `json:"payload_format"`
		Events        []map[string]any `json:"events"`
	}
	if err := json.Unmarshal(payload, &message); err != nil {
		t.Fatal(err)
	}
	if message.SchemaVersion != 3 || message.PayloadFormat != "compact" || len(message.Events) != 1 {
		t.Fatalf("compact envelope=%s", payload)
	}
	event := message.Events[0]
	if event["t"] != "p" || event["c"] != "0xA" || event["m"] != "12" || event["e"] != "34" || event["title"] != "Event title" {
		t.Fatalf("compact routing fields=%s", payload)
	}
	if _, exists := event["tags"]; exists {
		t.Fatalf("compact payload must route by tag_ids only: %s", payload)
	}
	if tagIDs, ok := event["tag_ids"].([]any); !ok || len(tagIDs) != 1 || tagIDs[0] != "1" {
		t.Fatalf("compact tag ids=%s", payload)
	}
	if event["market"] != nil || strings.Contains(string(payload), "must not cross") {
		t.Fatalf("full snapshot leaked into compact payload=%s", payload)
	}
}

func TestCompactSingleWSIsOptIn(t *testing.T) {
	mem := store.NewMemReplica()
	conn := dialTypedTestWS(t, mem, "events", "?format=compact")
	mem.BroadcastNew("dispute", store.EventRow{EventType: "dispute", ConditionID: "0xB", MarketID: "13", TxHash: "0xtx", BlockNumber: 9})
	_, payload, err := conn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	var message map[string]any
	if err := json.Unmarshal(payload, &message); err != nil {
		t.Fatal(err)
	}
	if message["t"] != "d" || message["event_type"] != nil || message["market"] != nil {
		t.Fatalf("compact single message=%s", payload)
	}
}

func TestCompactTradeV4IncludesAlignedTradeContext(t *testing.T) {
	t.Setenv("WS_BATCH_ENABLE", "1")
	mem := store.NewMemReplica()
	conn := dialTypedTestWS(t, mem, "events", "?batch=true&format=compact_trade")
	snapshot := &store.MarketSnapshot{
		MarketID: "12", ConditionID: "0xA", Question: "Winner", Slug: "winner",
		Tags: []store.MarketTag{{ID: "1"}}, SportsMarketType: "moneyline",
		TokenIDs: []string{"yes-token", "no-token"}, Outcomes: []string{"Yes", "No"}, OutcomePrices: []float64{0.91, 0.09},
		Active: true, AcceptingOrders: true, EnableOrderBook: true, UMAResolutionStatus: "proposed", TakerBaseFee: 7,
		CatalogSyncedAtUS: time.Now().Add(-time.Second).UnixMicro(),
	}
	mem.BroadcastNew("propose", store.EventRow{EventType: "propose", ConditionID: "0xA", MarketID: "12", Price: "1", TxHash: "0xtx", BlockNumber: 9, LogIndex: 7, Timestamp: 8, Source: "realtime", Market: snapshot})
	_, payload, err := conn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	var message struct {
		SchemaVersion int    `json:"schema_version"`
		PayloadFormat string `json:"payload_format"`
		Events        []struct {
			ProcessingKey string         `json:"processing_key"`
			Tokens        []wsTradeToken `json:"tokens"`
			Candidates    []wsTradeToken `json:"candidate_tokens"`
			Market        map[string]any `json:"market"`
		} `json:"events"`
	}
	if err := json.Unmarshal(payload, &message); err != nil {
		t.Fatal(err)
	}
	if message.SchemaVersion != 4 || message.PayloadFormat != "compact_trade" || len(message.Events) != 1 {
		t.Fatalf("v4 envelope=%s", payload)
	}
	event := message.Events[0]
	if event.ProcessingKey != "0xa:propose" || len(event.Tokens) != 2 || event.Tokens[0].TokenID != "yes-token" || event.Tokens[0].Outcome != "Yes" || event.Tokens[0].OutcomePrice != 0.91 {
		t.Fatalf("unaligned trade event=%s", payload)
	}
	if len(event.Candidates) != 1 || event.Candidates[0].TokenID != "yes-token" || event.Candidates[0].TakerBaseFee == nil || *event.Candidates[0].TakerBaseFee != 7 {
		t.Fatalf("candidate=%s", payload)
	}
	if event.Market["enable_order_book"] != true || event.Market["catalog_age_ms"] == nil {
		t.Fatalf("market context=%s", payload)
	}
}

func TestCompactTradeCandidateGuards(t *testing.T) {
	base := store.MarketSnapshot{
		MarketID: "1", ConditionID: "0x1", Slug: "winner", TokenIDs: []string{"a", "b"},
		Outcomes: []string{"Yes", "No"}, OutcomePrices: []float64{0.8, 0.2}, Active: true,
		AcceptingOrders: true, EnableOrderBook: true,
	}
	row := store.EventRow{EventType: "propose", ConditionID: "0x1"}
	assertNone := func(name string, snapshot store.MarketSnapshot) {
		t.Helper()
		t.Run(name, func(t *testing.T) {
			data, err := wsCompactTradeEventDTO(store.EventRow{EventType: row.EventType, ConditionID: row.ConditionID, Market: &snapshot})
			if err != nil {
				t.Fatal(err)
			}
			if got := data["candidate_tokens"].([]wsTradeToken); len(got) != 0 {
				t.Fatalf("candidates=%+v", got)
			}
		})
	}
	inactive := base
	inactive.Active = false
	assertNone("inactive", inactive)
	closed := base
	closed.Closed = true
	assertNone("closed", closed)
	notAccepting := base
	notAccepting.AcceptingOrders = false
	assertNone("not accepting", notAccepting)
	disputed := base
	disputed.UMAResolutionStatuses = []string{"disputed"}
	assertNone("disputed", disputed)
	extreme := base
	extreme.OutcomePrices = []float64{1, 0}
	assertNone("zero one prices", extreme)
	mismatch := base
	mismatch.Outcomes = []string{"Yes"}
	assertNone("array mismatch", mismatch)
	other := base
	other.Slug = "some-other"
	assertNone("other slug", other)
	sportsTotals := base
	sportsTotals.Tags = []store.MarketTag{{ID: "64"}}
	sportsTotals.SportsMarketType = "totals"
	assertNone("esports totals", sportsTotals)
}

func TestCompactTradeProcessingKeySeparatesDispute(t *testing.T) {
	snapshot := &store.MarketSnapshot{MarketID: "1", ConditionID: "0xABC"}
	propose, _ := wsCompactTradeEventDTO(store.EventRow{EventType: "propose", ConditionID: "0xABC", Market: snapshot})
	dispute, _ := wsCompactTradeEventDTO(store.EventRow{EventType: "dispute", ConditionID: "0xABC", Market: snapshot})
	if propose["processing_key"] != "0xabc:propose" || dispute["processing_key"] != "0xabc:dispute" {
		t.Fatalf("keys propose=%v dispute=%v", propose["processing_key"], dispute["processing_key"])
	}
}

func TestCompactTradeBurstDoesNotLoseEvents(t *testing.T) {
	t.Setenv("WS_BATCH_ENABLE", "1")
	mem := store.NewMemReplica()
	conn := dialTypedTestWS(t, mem, "events", "?batch=true&format=compact_trade")
	snapshot := &store.MarketSnapshot{
		MarketID: "1", ConditionID: "0x1", Slug: "winner", TokenIDs: []string{"a", "b"},
		Outcomes: []string{"Yes", "No"}, OutcomePrices: []float64{0.9, 0.1}, Active: true,
		AcceptingOrders: true, EnableOrderBook: true,
	}
	started := time.Now()
	for i := 0; i < 100; i++ {
		conditionID := fmt.Sprintf("0x%064x", i+1)
		copySnapshot := *snapshot
		copySnapshot.ConditionID = conditionID
		mem.BroadcastNew("propose", store.EventRow{
			EventType: "propose", ConditionID: conditionID, MarketID: "1", TxHash: "0xburst",
			BlockNumber: 10, LogIndex: i, Timestamp: 9, Source: "realtime", Market: &copySnapshot,
		})
	}
	got := 0
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	for got < 100 {
		_, payload, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("received=%d: %v", got, err)
		}
		var frame struct {
			SchemaVersion int               `json:"schema_version"`
			Events        []json.RawMessage `json:"events"`
		}
		if err := json.Unmarshal(payload, &frame); err != nil {
			t.Fatal(err)
		}
		if frame.SchemaVersion != 4 {
			t.Fatalf("schema=%d", frame.SchemaVersion)
		}
		got += len(frame.Events)
	}
	if got != 100 {
		t.Fatalf("received=%d, want 100", got)
	}
	t.Logf("100-event compact_trade burst delivered in %s", time.Since(started))
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
