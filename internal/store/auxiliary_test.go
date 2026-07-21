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
