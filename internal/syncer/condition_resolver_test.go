package syncer

import (
	"context"
	"sync"
	"testing"

	"github.com/polymas/poly_uma/internal/store"
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
	resolver := newConditionResolver(db, nil, "")
	got, err := resolver.ResolveRequired(context.Background(), "123")
	if err != nil {
		t.Fatal(err)
	}
	if got != "0xcondition" {
		t.Fatalf("ResolveRequired() = %q", got)
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
