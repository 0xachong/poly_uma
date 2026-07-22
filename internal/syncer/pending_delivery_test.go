package syncer

import (
	"context"
	"testing"
	"time"

	"github.com/polymas/poly_uma/internal/store"
)

func TestCompletePendingDeliveryUsesCachedMappingAndBroadcasts(t *testing.T) {
	db, err := store.Open(t.TempDir() + "/events.sqlite")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, conflict, err := db.UpsertMarketCondition("market-1", "condition-1"); err != nil || conflict {
		t.Fatalf("seed mapping conflict=%t err=%v", conflict, err)
	}
	inserted, _, _, err := db.InsertEvent("propose", "0xtx", 3, 100, 200, "", "market-1", "1", "")
	if err != nil || !inserted {
		t.Fatalf("insert event inserted=%t err=%v", inserted, err)
	}
	if err := db.EnqueueMarketDelivery("0xtx", 3, 123456); err != nil {
		t.Fatal(err)
	}
	pending, err := db.ListPendingMarketDeliveries(10)
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending=%+v err=%v", pending, err)
	}

	mem := store.NewMemReplica()
	ch, cancel := mem.Subscribe("propose")
	defer cancel()
	resolver := newConditionResolver(db, nil, nil, mem, "")
	resolver.completePendingDelivery(context.Background(), pending[0])

	select {
	case got := <-ch:
		if got.ConditionID != "condition-1" || got.Source != "delayed_replay" {
			t.Fatalf("unexpected replay: %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for delayed replay")
	}
	rows, err := db.QueryByType("propose", 0, 0, 10, 0)
	if err != nil || len(rows) != 1 || rows[0].ConditionID != "condition-1" {
		t.Fatalf("stored rows=%+v err=%v", rows, err)
	}
	pending, err = db.ListPendingMarketDeliveries(10)
	if err != nil || len(pending) != 0 {
		t.Fatalf("pending after replay=%+v err=%v", pending, err)
	}
}
