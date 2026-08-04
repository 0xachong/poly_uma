package mappingdiag

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/polymas/poly_uma/internal/uma"
)

type fakeChecker struct {
	exact uma.GammaMarketMapping
	rev   []uma.GammaMarketMapping
	err1  error
	err2  error
}

func (f fakeChecker) Market(context.Context, string) (uma.GammaMarketMapping, error) {
	return f.exact, f.err1
}
func (f fakeChecker) ByCondition(context.Context, string) ([]uma.GammaMarketMapping, error) {
	return f.rev, f.err2
}

func TestDiagnoseCatalogLag(t *testing.T) {
	e := Event{EventType: "propose", MarketID: "10", ConditionID: "0xabc"}
	now := time.Now()
	f := fakeChecker{exact: uma.GammaMarketMapping{ID: "10", ConditionID: "0xAbC", Active: true, UpdatedAt: now.Add(-2 * time.Minute).Format(time.RFC3339Nano)}, rev: []uma.GammaMarketMapping{{ID: "10", UpdatedAt: now.Add(-20 * time.Second).Format(time.RFC3339Nano)}}}
	got := Diagnose(context.Background(), f, e, now, 1)
	if got.Classification != "catalog_refresh_window_race" {
		t.Fatalf("classification=%s", got.Classification)
	}
}

func TestDiagnoseCoverageGap(t *testing.T) {
	now := time.Now()
	e := Event{EventType: "propose", MarketID: "10", ConditionID: "0xabc"}
	f := fakeChecker{exact: uma.GammaMarketMapping{ID: "10", ConditionID: "0xabc", Active: true, UpdatedAt: now.Add(-2 * time.Minute).Format(time.RFC3339Nano)}, rev: []uma.GammaMarketMapping{{ID: "10", UpdatedAt: now.Add(-2 * time.Minute).Format(time.RFC3339Nano)}}}
	got := Diagnose(context.Background(), f, e, now, 1)
	if got.Classification != "active_catalog_coverage_gap" {
		t.Fatalf("classification=%s", got.Classification)
	}
}

func TestDiagnoseConflict(t *testing.T) {
	e := Event{EventType: "dispute", MarketID: "10", ConditionID: "0xabc"}
	f := fakeChecker{exact: uma.GammaMarketMapping{ID: "10", ConditionID: "0xdef", Active: true}}
	got := Diagnose(context.Background(), f, e, time.Now(), 1)
	if got.Classification != "mapping_conflict" {
		t.Fatalf("classification=%s", got.Classification)
	}
}

func TestDiagnoseInactiveOpenMarket(t *testing.T) {
	e := Event{EventType: "propose", MarketID: "10", ConditionID: "0xabc", Source: "delayed_replay"}
	f := fakeChecker{exact: uma.GammaMarketMapping{ID: "10", ConditionID: "0xabc", Active: false, Closed: false}}
	got := Diagnose(context.Background(), f, e, time.Now(), 1)
	if got.Classification != "gamma_inactive_open_market" {
		t.Fatalf("classification=%s", got.Classification)
	}
}

func TestDiagnoseGammaFailure(t *testing.T) {
	e := Event{EventType: "propose", MarketID: "10", ConditionID: "0xabc"}
	f := fakeChecker{err1: errors.New("timeout"), err2: errors.New("timeout")}
	got := Diagnose(context.Background(), f, e, time.Now(), 1)
	if got.Classification != "gamma_unavailable_or_not_found" {
		t.Fatalf("classification=%s", got.Classification)
	}
}

func TestSnapshotMissing(t *testing.T) {
	if !(Event{MarketID: "1", ConditionID: "x"}).SnapshotMissing() {
		t.Fatal("expected missing")
	}
	if (Event{MarketID: "1", ConditionID: "x", Market: []byte(`{}`)}).SnapshotMissing() {
		t.Fatal("expected hit")
	}
}
