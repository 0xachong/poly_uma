package main

import "testing"

func TestLatencyStoreSeparatesCollectorsAndKeepsOnlySampleHistory(t *testing.T) {
	store := newLatencyStore(2)
	store.add(latencyWindow{GeneratedAtMS: 1000, CollectorID: "json-v4", SampleCount: 1, Overall: map[string]latencyQuantiles{"end_to_end": {Count: 1, P50: 10}}})
	store.add(latencyWindow{GeneratedAtMS: 2000, CollectorID: "pb-v5", SampleCount: 0, FramesReceived: 1, FramesDecoded: 1})
	store.add(latencyWindow{GeneratedAtMS: 3000, CollectorID: "pb-v5", SampleCount: 1, Overall: map[string]latencyQuantiles{"end_to_end": {Count: 1, P50: 5}}})

	selected, latest, history, statuses := store.snapshot("json-v4", 4000)
	if selected != "json-v4" || latest.CollectorID != "json-v4" || len(history) != 1 {
		t.Fatalf("json snapshot selected=%q latest=%q history=%d", selected, latest.CollectorID, len(history))
	}
	selected, latest, history, statuses = store.snapshot("pb-v5", 4000)
	if selected != "pb-v5" || latest.GeneratedAtMS != 3000 || len(history) != 1 {
		t.Fatalf("pb snapshot selected=%q generated=%d history=%d", selected, latest.GeneratedAtMS, len(history))
	}
	if len(statuses) != 2 || statuses[0].CollectorID != "json-v4" || statuses[1].CollectorID != "pb-v5" {
		t.Fatalf("statuses=%+v", statuses)
	}
}

func TestLatencyStoreMarksStaleAndOffline(t *testing.T) {
	store := newLatencyStore(2)
	store.add(latencyWindow{GeneratedAtMS: 1000, CollectorID: "offline"})
	store.add(latencyWindow{GeneratedAtMS: 25000, CollectorID: "stale"})
	_, _, _, statuses := store.snapshot("", 40001)
	got := map[string]string{}
	for _, status := range statuses {
		got[status.CollectorID] = status.Status
	}
	if got["offline"] != "offline" || got["stale"] != "stale" {
		t.Fatalf("statuses=%v", got)
	}
}
