package store

import (
	"path/filepath"
	"testing"
	"time"
)

func TestActiveMarketSnapshotLoadsOnlyActiveRows(t *testing.T) {
	db, err := OpenMarket(filepath.Join(t.TempDir(), "market.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, record := range []ActiveMarketSnapshotRecord{
		{MarketID: "active", ConditionID: "0xa", SnapshotJSON: `{"market_id":"active"}`, Active: true, CLOBLastSeenAt: time.Now().Unix()},
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

func TestCLOBRefreshAndCompletedBaselineKeepGraceAndPin(t *testing.T) {
	db, err := OpenMarket(filepath.Join(t.TempDir(), "market.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Now().Unix()
	for _, record := range []ActiveMarketSnapshotRecord{
		{MarketID: "hot", ConditionID: "0xhot", SnapshotJSON: `{}`, Active: true},
		{MarketID: "grace", ConditionID: "0xgrace", SnapshotJSON: `{}`, Active: true, CLOBLastSeenAt: now - 3600},
		{MarketID: "uma", ConditionID: "0xuma", SnapshotJSON: `{}`, Active: true, UMAPinnedUntil: now + 3600},
		{MarketID: "legacy", ConditionID: "0xlegacy", SnapshotJSON: `{}`, Active: true},
	} {
		if err := db.UpsertActiveMarketSnapshot(record); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.ApplyCLOBResidentSet([]string{"0xhot"}, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.PruneExpiredActiveMarketSnapshots(now-48*3600, now); err != nil {
		t.Fatal(err)
	}
	rows, err := db.LoadActiveMarketSnapshots()
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, row := range rows {
		got[row.ConditionID] = true
	}
	if !got["0xhot"] || !got["0xgrace"] || !got["0xuma"] || got["0xlegacy"] {
		t.Fatalf("resident set=%v", got)
	}
}

func TestDeactivateActiveSnapshotKeepsIdentity(t *testing.T) {
	db, err := OpenMarket(filepath.Join(t.TempDir(), "market.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, _, err := db.UpsertMarketCondition("market-1", "condition-1"); err != nil {
		t.Fatal(err)
	}
	if err := db.UpsertActiveMarketSnapshot(ActiveMarketSnapshotRecord{
		MarketID: "market-1", ConditionID: "condition-1", SnapshotJSON: `{}`, Active: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.DeactivateActiveMarketSnapshot("condition-1"); err != nil {
		t.Fatal(err)
	}
	rows, err := db.LoadActiveMarketSnapshots()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("active rows=%d", len(rows))
	}
	if got, err := db.GetMarketConditionID("market-1"); err != nil || got != "condition-1" {
		t.Fatalf("identity got=%q err=%v", got, err)
	}
}
