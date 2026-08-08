package main

import (
	"testing"

	"github.com/gorilla/websocket"
	wirev5 "github.com/polymas/poly_uma/internal/wire/v5"
	"google.golang.org/protobuf/proto"
)

func TestLatencyReporterObservesCompactTradeJSONV4AndV5(t *testing.T) {
	for _, version := range []int{4, 5} {
		reporter := newLatencyReporter("http://report.invalid", "token", 0, "json-probe")
		payload := []byte(`{
			"schema_version":` + string(rune('0'+version)) + `,"payload_format":"compact_trade","batch_id":"batch-json","batch_part":2,"slave_node_id":"slave-03",
			"slave_received_at_us":2000300,"slave_broadcast_at_us":2000700,
			"events":[
				{"source":"realtime","chain_timestamp_ms":1000,"master_received_at_us":1500000,"master_broadcast_at_us":2000000},
				{"source":"backfill","chain_timestamp_ms":1000,"master_received_at_us":1500000,"master_broadcast_at_us":2000000}
			]
		}`)
		reporter.observe(websocket.TextMessage, payload, 2002000)

		samples := reporter.samples["slave-03"]
		want := map[string]int64{
			"chain_to_master": 500, "master_processing": 500, "master_to_slave": 0,
			"slave_processing": 0, "slave_to_client": 1, "end_to_end": 1001,
		}
		for stage, value := range want {
			if got := samples[stage]; len(got) != 1 || got[0] != value {
				t.Errorf("v%d %s samples=%v, want [%d]", version, stage, got, value)
			}
		}
		if reporter.framesDecoded.Load() != 1 || reporter.eventsReceived.Load() != 2 || reporter.realtimeEvents.Load() != 1 || reporter.nonRealtimeSkipped.Load() != 1 {
			t.Fatalf("v%d diagnostics decoded=%d events=%d realtime=%d skipped=%d", version, reporter.framesDecoded.Load(), reporter.eventsReceived.Load(), reporter.realtimeEvents.Load(), reporter.nonRealtimeSkipped.Load())
		}
		if reporter.windowBatchFrames != 1 || reporter.windowBatchEvents != 2 {
			t.Fatalf("v%d batch_frames=%d batch_events=%d", version, reporter.windowBatchFrames, reporter.windowBatchEvents)
		}
		if _, ok := reporter.windowBatchRefs["batch-json#2"]; !ok {
			t.Fatalf("v%d batch_refs=%v", version, reporter.windowBatchRefs)
		}
	}
}

func TestLatencyReporterObservesCompactTradeProtobufV5(t *testing.T) {
	reporter := newLatencyReporter("http://report.invalid", "token", 0, "pb-probe")
	payload, err := proto.Marshal(&wirev5.CompactTradeBatch{
		SchemaVersion: 5, PayloadFormat: "compact_trade",
		BatchId: "batch-pb", BatchPart: 3,
		SlaveReceivedAtUs: 2000300, SlaveBroadcastAtUs: 2000700,
		Events: []*wirev5.CompactTradeEvent{
			{Source: "realtime", ChainTimestampMs: 1000, MasterReceivedAtUs: 1500000, MasterBroadcastAtUs: 2000000},
			{Source: "delayed_replay", ChainTimestampMs: 1000, MasterReceivedAtUs: 1500000, MasterBroadcastAtUs: 2000000},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	reporter.observe(websocket.BinaryMessage, payload, 2002000)
	if got := reporter.samples["pb-probe"]["end_to_end"]; len(got) != 1 || got[0] != 1001 {
		t.Fatalf("protobuf end_to_end=%v, want [1001]", got)
	}
	if reporter.framesDecoded.Load() != 1 || reporter.realtimeEvents.Load() != 1 || reporter.nonRealtimeSkipped.Load() != 1 {
		t.Fatalf("protobuf diagnostics decoded=%d realtime=%d skipped=%d", reporter.framesDecoded.Load(), reporter.realtimeEvents.Load(), reporter.nonRealtimeSkipped.Load())
	}
	if reporter.windowBatchFrames != 1 || reporter.windowBatchEvents != 2 {
		t.Fatalf("protobuf batch_frames=%d batch_events=%d", reporter.windowBatchFrames, reporter.windowBatchEvents)
	}
	if _, ok := reporter.windowBatchRefs["batch-pb#3"]; !ok {
		t.Fatalf("protobuf batch_refs=%v", reporter.windowBatchRefs)
	}
}

func TestLatencyReporterUsesCollectorIDWhenJSONHasNoSlaveNodeID(t *testing.T) {
	reporter := newLatencyReporter("http://report.invalid", "token", 0, "json-probe")
	reporter.observe(websocket.TextMessage, []byte(`{
		"schema_version":4,"payload_format":"compact_trade",
		"slave_received_at_us":2000300,"slave_broadcast_at_us":2000700,
		"events":[{"source":"realtime","chain_timestamp_ms":1000,"master_received_at_us":1500000,"master_broadcast_at_us":2000000}]
	}`), 2002000)
	if got := reporter.samples["json-probe"]["end_to_end"]; len(got) != 1 || got[0] != 1001 {
		t.Fatalf("fallback node end_to_end=%v, want [1001]", got)
	}
	if reporter.missingTimestamp.Load() != 0 {
		t.Fatalf("missing_timestamp=%d, want 0", reporter.missingTimestamp.Load())
	}
}

func TestLatencyReporterRejectsMalformedFrameWithDiagnostic(t *testing.T) {
	reporter := newLatencyReporter("http://report.invalid", "token", 0, "probe")
	reporter.observe(websocket.BinaryMessage, []byte("not protobuf"), 1)
	if reporter.decodeErrors.Load() != 1 {
		t.Fatalf("decode_errors=%d, want 1", reporter.decodeErrors.Load())
	}
}
