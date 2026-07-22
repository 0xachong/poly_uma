package syncer

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/polymas/poly_uma/internal/store"
	"github.com/polymas/poly_uma/internal/uma"
)

func TestConditionResolverUsesPersistentMapping(t *testing.T) {
	db, err := store.Open(t.TempDir() + "/resolver.sqlite")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, conflict, err := db.UpsertMarketCondition("123", "0xcondition"); err != nil || conflict {
		t.Fatalf("UpsertMarketCondition conflict=%v err=%v", conflict, err)
	}
	resolver := newConditionResolver(db, nil, nil, nil, "")
	got, err := resolver.ResolveRequired(context.Background(), "123")
	if err != nil {
		t.Fatal(err)
	}
	if got != "0xcondition" {
		t.Fatalf("ResolveRequired() = %q", got)
	}
}

func TestConditionResolverUsesMarketPrimary(t *testing.T) {
	dir := t.TempDir()
	db, err := store.Open(filepath.Join(dir, "events.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	market, err := store.OpenMarket(filepath.Join(dir, "market.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer market.Close()
	if _, conflict, err := market.UpsertMarketCondition("123", "from-primary"); err != nil || conflict {
		t.Fatalf("market insert conflict=%v err=%v", conflict, err)
	}
	resolver := newConditionResolver(db, market, nil, nil, "")
	got, err := resolver.ResolveRequired(context.Background(), "123")
	if err != nil {
		t.Fatal(err)
	}
	if got != "from-primary" {
		t.Fatalf("ResolveRequired() = %q", got)
	}
}

func TestConditionResolverUsesQuestionPrimary(t *testing.T) {
	dir := t.TempDir()
	db, err := store.Open(filepath.Join(dir, "events.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	maintenance, err := store.OpenMaintenance(filepath.Join(dir, "maintenance.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer maintenance.Close()
	if conflict, err := maintenance.UpsertQuestionMapping("question-1", "condition-1", "market-1", "tx-1"); err != nil || conflict {
		t.Fatalf("question insert conflict=%v err=%v", conflict, err)
	}
	resolver := newConditionResolver(db, nil, maintenance, nil, "")
	got, err := resolver.ResolveQuestion("question-1")
	if err != nil {
		t.Fatal(err)
	}
	if got != "condition-1" {
		t.Fatalf("ResolveQuestion() = %q", got)
	}
}

func TestMarketConditionMappingDetectsConcurrentConflict(t *testing.T) {
	db, err := store.Open(t.TempDir() + "/resolver.sqlite")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	start := make(chan struct{})
	results := make(chan bool, 2)
	var wg sync.WaitGroup
	for _, conditionID := range []string{"first", "second"} {
		wg.Add(1)
		go func(value string) {
			defer wg.Done()
			<-start
			_, conflict, upsertErr := db.UpsertMarketCondition("123", value)
			if upsertErr != nil {
				t.Errorf("UpsertMarketCondition(%q): %v", value, upsertErr)
			}
			results <- conflict
		}(conditionID)
	}
	close(start)
	wg.Wait()
	close(results)

	conflicts := 0
	for conflict := range results {
		if conflict {
			conflicts++
		}
	}
	if conflicts != 1 {
		t.Fatalf("conflicts=%d, want 1", conflicts)
	}
}

func TestMarketConditionMappingRejectsConflict(t *testing.T) {
	db, err := store.Open(t.TempDir() + "/resolver.sqlite")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, _, err := db.UpsertMarketCondition("123", "first"); err != nil {
		t.Fatal(err)
	}
	inserted, conflict, err := db.UpsertMarketCondition("123", "second")
	if err != nil {
		t.Fatal(err)
	}
	if inserted || !conflict {
		t.Fatalf("inserted=%v conflict=%v", inserted, conflict)
	}
}

func TestConditionResolverPreloadsActiveAndEvictsOldClosedMarket(t *testing.T) {
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
	if _, _, err := marketDB.UpsertMarketConditionStatus("123", "condition-1", true, false, 0); err != nil {
		t.Fatal(err)
	}

	resolver := newConditionResolver(db, marketDB, nil, nil, "")
	if got := resolver.cached("123"); got != "condition-1" {
		t.Fatalf("active preload=%q", got)
	}
	closedAt := time.Now().Add(-25 * time.Hour).UTC().Format(time.RFC3339Nano)
	if err := resolver.storeCatalogMapping(uma.GammaMarketMapping{
		ID: "123", ConditionID: "condition-1", Active: true, Closed: true, ClosedTime: closedAt,
	}); err != nil {
		t.Fatal(err)
	}
	if got := resolver.cached("123"); got != "" {
		t.Fatalf("old closed market remained cached: %q", got)
	}
	if got, err := marketDB.GetMarketConditionID("123"); err != nil || got != "condition-1" {
		t.Fatalf("durable closed mapping got=%q err=%v", got, err)
	}
}
