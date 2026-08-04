package store

import (
	"path/filepath"
	"testing"
)

func TestActiveMarketSnapshotLoadsOnlyActiveRows(t *testing.T) {
	db, err := OpenMarket(filepath.Join(t.TempDir(), "market.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, record := range []ActiveMarketSnapshotRecord{
		{MarketID: "active", ConditionID: "0xa", SnapshotJSON: `{"market_id":"active"}`, Active: true},
		{MarketID: "closed", ConditionID: "0xb", SnapshotJSON: `{"market_id":"closed"}`, Active: false, Closed: true},
	} {
		if err := db.UpsertActiveMarketSnapshot(record); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := db.LoadActiveMarketSnapshots()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].MarketID != "active" {
		t.Fatalf("active snapshots=%+v", rows)
	}
}
