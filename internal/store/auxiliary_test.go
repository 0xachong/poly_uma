package store

import (
	"database/sql"
	"path/filepath"
	"testing"
)

func TestAuxiliarySQLiteSchemas(t *testing.T) {
	dir := t.TempDir()
	marketPath := filepath.Join(dir, "market.sqlite")
	market, err := OpenMarket(marketPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := market.Close(); err != nil {
		t.Fatal(err)
	}
	maintenancePath := filepath.Join(dir, "maintenance.sqlite")
	maintenance, err := OpenMaintenance(maintenancePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := maintenance.Close(); err != nil {
		t.Fatal(err)
	}

	assertTables(t, marketPath, "market_condition_map", "market_sync_state")
	assertTables(t, maintenancePath, "question_condition_map", "resolved_pending", "migration_state", "reconciliation_state")
}

func TestAuxiliaryMirrorWrites(t *testing.T) {
	dir := t.TempDir()
	market, err := OpenMarket(filepath.Join(dir, "market.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer market.Close()
	inserted, conflict, err := market.UpsertMarketCondition("market-1", "condition-1")
	if err != nil || !inserted || conflict {
		t.Fatalf("market insert inserted=%t conflict=%t err=%v", inserted, conflict, err)
	}
	_, conflict, err = market.UpsertMarketCondition("market-1", "condition-2")
	if err != nil || !conflict {
		t.Fatalf("market conflict=%t err=%v", conflict, err)
	}

	maintenance, err := OpenMaintenance(filepath.Join(dir, "maintenance.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer maintenance.Close()
	conflict, err = maintenance.UpsertQuestionMapping("question-1", "", "market-1", "tx-1")
	if err != nil || conflict {
		t.Fatalf("question insert conflict=%t err=%v", conflict, err)
	}
	if err := maintenance.FillConditionByMarketID("market-1", "condition-1"); err != nil {
		t.Fatal(err)
	}
	var conditionID string
	if err := maintenance.db.QueryRow(`SELECT condition_id FROM question_condition_map WHERE question_id='question-1'`).Scan(&conditionID); err != nil {
		t.Fatal(err)
	}
	if conditionID != "condition-1" {
		t.Fatalf("condition_id=%q", conditionID)
	}
}

func assertTables(t *testing.T, path string, names ...string) {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, name := range names {
		var count int
		if err := db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?`, name).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("table %s count=%d", name, count)
		}
	}
}
