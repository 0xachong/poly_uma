package store

import "testing"

func TestPendingMarketDeliveryLifecycle(t *testing.T) {
	db, err := Open(t.TempDir() + "/events.sqlite")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	inserted, id, cursor, err := db.InsertEvent(
		"propose", "0xtx", 7, 100, 200, "", "market-1", "1", "",
	)
	if err != nil || !inserted {
		t.Fatalf("InsertEvent inserted=%t err=%v", inserted, err)
	}
	if err := db.EnqueueMarketDelivery("0xtx", 7, 123456); err != nil {
		t.Fatal(err)
	}
	// Enqueue is idempotent so reconnect/backfill overlap cannot duplicate work.
	if err := db.EnqueueMarketDelivery("0xtx", 7, 999999); err != nil {
		t.Fatal(err)
	}

	pending, err := db.ListPendingMarketDeliveries(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 {
		t.Fatalf("pending count=%d, want 1", len(pending))
	}
	got := pending[0]
	if got.ID != id || got.CursorID != cursor || got.MarketID != "market-1" || got.ConditionID != "" {
		t.Fatalf("unexpected pending row: %+v", got)
	}
	if got.UpstreamReceivedAtMS != 123456 {
		t.Fatalf("upstream_received_at_ms=%d, want first value 123456", got.UpstreamReceivedAtMS)
	}

	if err := db.UpdateConditionIDByMarketID("market-1", "condition-1"); err != nil {
		t.Fatal(err)
	}
	pending, err = db.ListPendingMarketDeliveries(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].ConditionID != "condition-1" {
		t.Fatalf("pending mapping was not refreshed: %+v", pending)
	}
	if err := db.DeletePendingMarketDelivery("0xtx", 7); err != nil {
		t.Fatal(err)
	}
	pending, err = db.ListPendingMarketDeliveries(10)
	if err != nil || len(pending) != 0 {
		t.Fatalf("pending after delete=%+v err=%v", pending, err)
	}
}
