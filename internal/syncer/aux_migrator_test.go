package syncer

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/polymas/poly_uma/internal/store"
)

func TestAuxiliaryMigratorCopiesAndCompletes(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "events.sqlite")
	source, err := store.Open(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := source.UpsertMarketCondition("market-1", "condition-1"); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := source.InsertEvent("init", "0xtx", 1, 10, 20, "condition-1", "market-1", "", "question-1"); err != nil {
		t.Fatal(err)
	}
	source.Close()

	reader, err := store.OpenLegacyReader(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	market, err := store.OpenMarket(filepath.Join(dir, "market.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer market.Close()
	maintenance, err := store.OpenMaintenance(filepath.Join(dir, "maintenance.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer maintenance.Close()

	(&AuxiliaryMigrator{Source: reader, Market: market, Maintenance: maintenance, BatchSize: 1, Yield: time.Millisecond}).Run(context.Background())
	if count, err := market.MappingCount(); err != nil || count != 1 {
		t.Fatalf("market count=%d err=%v", count, err)
	}
	if count, err := maintenance.QuestionMappingCount(); err != nil || count != 1 {
		t.Fatalf("question count=%d err=%v", count, err)
	}
	for _, task := range []string{"market_catalog_v1", "question_map_v1"} {
		state, err := maintenance.GetMigrationState(task)
		if err != nil || state.Status != "complete" {
			t.Fatalf("task=%s state=%+v err=%v", task, state, err)
		}
	}
}
