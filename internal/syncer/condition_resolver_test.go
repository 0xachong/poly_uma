package syncer

import (
	"testing"

	"github.com/polymas/poly_uma/internal/store"
)

func TestConditionResolverUsesPreloadedMapping(t *testing.T) {
	db, err := store.Open(t.TempDir() + "/resolver.sqlite")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, _, _, err = db.InsertEvent(
		"propose", "0xtx", 1, 100, store.RecentMemoryCutoffUnix()+1,
		"0xcondition", "123", "100", "",
	)
	if err != nil {
		t.Fatal(err)
	}

	resolver := newConditionResolver(db, nil, "")
	if got := resolver.Resolve("123"); got != "0xcondition" {
		t.Fatalf("Resolve() = %q, want %q", got, "0xcondition")
	}
	if len(resolver.jobs) != 0 {
		t.Fatalf("cache hit unexpectedly queued enrichment")
	}
}

func TestConditionResolverCoalescesCacheMiss(t *testing.T) {
	db, err := store.Open(t.TempDir() + "/resolver.sqlite")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	resolver := newConditionResolver(db, nil, "")
	for i := 0; i < 20; i++ {
		if got := resolver.Resolve("missing"); got != "" {
			t.Fatalf("Resolve() = %q, want empty cache miss", got)
		}
	}
	if len(resolver.jobs) != 1 {
		t.Fatalf("queued jobs = %d, want 1", len(resolver.jobs))
	}
}
