package syncer

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
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

func TestRealtimePrefetchIsBoundedDeduplicatedAndNonBlocking(t *testing.T) {
	resolver := &conditionResolver{
		prefetchQueue:  make(chan string, 8),
		prefetchQueued: make(map[string]struct{}),
	}
	started := time.Now()
	for i := 0; i < 1000; i++ {
		resolver.Prefetch(context.Background(), fmt.Sprintf("market-%d", i))
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("hot path blocked for %s", elapsed)
	}
	if got := len(resolver.prefetchQueue); got != 8 {
		t.Fatalf("queued=%d, want bounded capacity 8", got)
	}
	if got := resolver.prefetchDropped.Load(); got != 992 {
		t.Fatalf("dropped=%d, want 992", got)
	}

	resolver = &conditionResolver{
		prefetchQueue:  make(chan string, 8),
		prefetchQueued: make(map[string]struct{}),
	}
	for i := 0; i < 100; i++ {
		resolver.Prefetch(context.Background(), "same-market")
	}
	if got := len(resolver.prefetchQueue); got != 1 {
		t.Fatalf("duplicate market queued %d times", got)
	}
}

func TestSnapshotFromGammaIncludesRoutingMetadata(t *testing.T) {
	got := snapshotFromGamma(uma.GammaMarketMapping{
		ID: "12", ConditionID: "0xAbC", Question: "Will it win?", Active: true,
		SportsMarketType: "moneyline", TokenIDs: []string{"yes", "no"},
		Events: []uma.GammaEventInfo{{ID: "event-1", Title: "Match", Slug: "match",
			Tags: []uma.GammaTagMapping{{ID: "soccer", Label: "Soccer"}}}},
	})
	if got.MarketID != "12" || got.ConditionID != "0xAbC" || got.PolymarketEventTitle != "Match" {
		t.Fatalf("snapshot=%+v", got)
	}
	if len(got.Tags) != 1 || got.Tags[0].ID != "soccer" || got.SportsMarketType != "moneyline" {
		t.Fatalf("routing metadata=%+v", got)
	}
}

func TestSnapshotFromGammaFallsBackToMarketTitleWithoutEventRelation(t *testing.T) {
	got := snapshotFromGamma(uma.GammaMarketMapping{ID: "3335996", ConditionID: "0xcondition",
		Question: "Map 2 Rounds Handicap", Slug: "map-2-rounds", Tags: []uma.GammaTagMapping{{ID: "64", Label: "Esports"}}})
	if got.PolymarketEventTitle != "Map 2 Rounds Handicap" || got.PolymarketEventSlug != "map-2-rounds" || len(got.Tags) != 1 {
		t.Fatalf("fallback snapshot=%+v", got)
	}
}

func TestActiveCatalogUsesConditionIDPrimaryIndex(t *testing.T) {
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
	resolver := newConditionResolver(db, marketDB, nil, nil, "")
	resolver.SetActiveCatalogEnabled(true)
	resolver.clobReady.Store(true)
	resolver.clobHot["0xabc"] = struct{}{}
	if err := resolver.storeCatalogMapping(uma.GammaMarketMapping{
		ID: "market-1", ConditionID: "0xAbC", Question: "Question", Active: true, AcceptingOrders: true,
	}); err != nil {
		t.Fatal(err)
	}
	if got := resolver.ResolveSnapshotCached("0xabc"); got == nil || got.MarketID != "market-1" {
		t.Fatalf("condition primary lookup=%+v", got)
	}
	if got := resolver.ResolveSnapshotCached("market-1"); got != nil {
		t.Fatalf("market_id must not be a snapshot primary key: %+v", got)
	}
}

func TestCatalogResidencyUsesCLOBHotSetAndGraceSnapshots(t *testing.T) {
	resolver := &conditionResolver{clobHot: map[string]struct{}{"0xhot": {}}, snapshots: map[string]*store.MarketSnapshot{
		"0xgrace": {ConditionID: "0xgrace", MarketID: "grace"},
	}}
	if !resolver.isCLOBHot("0xHOT") || resolver.isCLOBHot("0xgrace") || !resolver.hasSnapshot("0xgrace") || resolver.hasSnapshot("0xcold") {
		t.Fatal("CLOB hot/grace residency classification failed")
	}
}

func TestGammaTradableDefinesFullActiveBaseline(t *testing.T) {
	base := uma.GammaMarketMapping{ID: "market", ConditionID: "0xcondition", Active: true, AcceptingOrders: true}
	if !gammaMarketTradable(base) {
		t.Fatal("active accepting market must enter full baseline")
	}
	for _, mutate := range []func(*uma.GammaMarketMapping){
		func(m *uma.GammaMarketMapping) { m.Active = false },
		func(m *uma.GammaMarketMapping) { m.Closed = true },
		func(m *uma.GammaMarketMapping) { m.Archived = true },
		func(m *uma.GammaMarketMapping) { m.AcceptingOrders = false },
	} {
		market := base
		mutate(&market)
		if gammaMarketTradable(market) {
			t.Fatalf("non-tradable market accepted: %+v", market)
		}
	}
}

func TestFullGammaActiveMarketDoesNotRequireSamplingMembership(t *testing.T) {
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
	resolver := newConditionResolver(db, marketDB, nil, nil, "")
	resolver.SetActiveCatalogEnabled(true)
	resolver.clobReady.Store(true)
	if _, err := resolver.storeCatalogMappingWithResult(uma.GammaMarketMapping{
		ID: "3241754", ConditionID: "0xfull", Question: "Full active", Active: true, AcceptingOrders: true,
	}); err != nil {
		t.Fatal(err)
	}
	if got := resolver.ResolveSnapshotCached("0xfull"); got == nil || got.MarketID != "3241754" {
		t.Fatalf("full active market missing without sampling membership: %+v", got)
	}
}

func TestForcedEnrichmentPinsColdMarketByCondition(t *testing.T) {
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
	resolver := newConditionResolver(db, marketDB, nil, nil, "")
	resolver.SetActiveCatalogEnabled(true)
	market := uma.GammaMarketMapping{
		ID: "old-market", ConditionID: "0xCold", Question: "Old question", Active: true,
		UpdatedAt: time.Now().Add(-30 * 24 * time.Hour).Format(time.RFC3339Nano),
	}
	if _, err := resolver.storeCatalogMappingWithResult(market); err != nil {
		t.Fatal(err)
	}
	if got := resolver.ResolveSnapshotCached("0xcold"); got != nil {
		t.Fatalf("cold market unexpectedly resident: %+v", got)
	}
	if _, err := resolver.storeCatalogMappingWithResultMode(market, true); err != nil {
		t.Fatal(err)
	}
	if got := resolver.ResolveSnapshotCached("0xcold"); got == nil || got.MarketID != "old-market" {
		t.Fatalf("forced condition enrichment=%+v", got)
	}
}

func TestForcedEnrichmentPinsInactiveIntermediateGammaState(t *testing.T) {
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
	resolver := newConditionResolver(db, marketDB, nil, nil, "")
	resolver.SetActiveCatalogEnabled(true)
	market := uma.GammaMarketMapping{ID: "market-intermediate", ConditionID: "0xintermediate", Question: "Published question", Active: false}
	if _, err := resolver.storeCatalogMappingWithResultMode(market, true); err != nil {
		t.Fatal(err)
	}
	if got := resolver.ResolveSnapshotCached("0xintermediate"); got == nil || got.Question != "Published question" {
		t.Fatalf("forced intermediate snapshot=%+v", got)
	}
}

func TestGammaSnapshotDiagnosticNamesMissingFields(t *testing.T) {
	detail := gammaSnapshotDiagnostic(uma.GammaMarketMapping{ID: "3335996", ConditionID: "0xcondition", UpdatedAt: "now"})
	for _, want := range []string{"market=3335996", "active=false", "question_empty=true", "events=0", "updated_at=now"} {
		if !strings.Contains(detail, want) {
			t.Fatalf("diagnostic %q missing %q", detail, want)
		}
	}
}

func TestInitPrefetchRequiresFullSnapshotNotOnlyMapping(t *testing.T) {
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
	if _, conflict, err := marketDB.UpsertMarketCondition("market-1", "0xcondition"); err != nil || conflict {
		t.Fatalf("mapping conflict=%t err=%v", conflict, err)
	}
	resolver := newConditionResolver(db, marketDB, nil, nil, "")
	resolver.SetActiveCatalogEnabled(true)
	if !resolver.prefetchNeeded("market-1") {
		t.Fatal("mapping-only init must still prefetch the full snapshot")
	}
	resolver.setSnapshot(&store.MarketSnapshot{MarketID: "market-1", ConditionID: "0xcondition"})
	if resolver.prefetchNeeded("market-1") {
		t.Fatal("complete condition-indexed snapshot should skip prefetch")
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
	if got, err := db.GetMarketConditionID("123"); err != nil || got != "" {
		t.Fatalf("catalog sync leaked into event database: got=%q err=%v", got, err)
	}
}
