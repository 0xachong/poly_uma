package api

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/polymas/poly_uma/internal/store"
)

func TestBuildEnrichmentMetricsByTagAndEvent(t *testing.T) {
	dir := t.TempDir()
	db, err := store.Open(filepath.Join(dir, "events.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	marketDB, err := store.OpenMarket(filepath.Join(dir, "market.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer marketDB.Close()
	now := time.Now()
	for _, row := range []struct {
		event, tx, market, condition string
		index                        int
	}{
		{"propose", "0x1", "m1", "c1", 1}, {"propose", "0x2", "m1", "c1", 2}, {"dispute", "0x3", "m2", "c2", 3},
	} {
		if _, _, _, err := db.InsertEvent(row.event, row.tx, row.index, 100, now.Unix(), row.condition, row.market, "", ""); err != nil {
			t.Fatal(err)
		}
	}
	for _, snapshot := range []store.MarketSnapshot{
		{MarketID: "m1", ConditionID: "c1", Tags: []store.MarketTag{{ID: "1"}}},
		{MarketID: "m2", ConditionID: "c2", Tags: []store.MarketTag{{ID: "2"}}},
	} {
		encoded, _ := json.Marshal(snapshot)
		if err := marketDB.UpsertActiveMarketSnapshot(store.ActiveMarketSnapshotRecord{MarketID: snapshot.MarketID, ConditionID: snapshot.ConditionID, SnapshotJSON: string(encoded), Active: true}); err != nil {
			t.Fatal(err)
		}
	}
	if err := marketDB.AppendMarketEnrichmentIncident(store.MarketEnrichmentIncident{ObservedAtMS: now.UnixMilli(), Stage: "miss_observed", MarketID: "m1", EventType: "propose", TxHash: "0x2", LogIndex: 2, DetailJSON: `{"kind":"condition_mapping_miss"}`}); err != nil {
		t.Fatal(err)
	}
	result, err := buildEnrichmentMetrics(db, marketDB, 5, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if result.Overall.Total != 3 || result.Overall.Misses != 1 || result.Overall.MappingMiss != 1 {
		t.Fatalf("overall=%+v", result.Overall)
	}
	if len(result.ByEvent) != 2 {
		t.Fatalf("by_event=%+v", result.ByEvent)
	}
	var sports enrichmentDimension
	for _, item := range result.ByTag {
		if item.Key == "1" {
			sports = item
		}
	}
	if sports.Total != 2 || sports.Misses != 1 || sports.MissRate != 50 {
		t.Fatalf("sports=%+v", sports)
	}
}
