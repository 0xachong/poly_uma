package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestEventShardReplicatorBackfillAndTransactionalOutbox(t *testing.T) {
	dir := t.TempDir()
	source, err := Open(filepath.Join(dir, "legacy.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	if ok, _, _, err := source.InsertEvent("init", "0xinit", 1, 10, 20, "c1", "m1", "", "q1"); err != nil || !ok {
		t.Fatalf("insert historical init ok=%t err=%v", ok, err)
	}
	if ok, _, _, err := source.InsertEvent("propose", "0xproposal", 2, 11, 21, "c2", "m2", "99", ""); err != nil || !ok {
		t.Fatalf("insert historical proposal ok=%t err=%v", ok, err)
	}
	replicator, err := OpenEventShardReplicator(source, filepath.Join(dir, "signal.sqlite"), filepath.Join(dir, "lifecycle.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer replicator.Close()
	if ok, _, _, err := source.InsertEvent("resolved", "0xresolved", 3, 12, 22, "c1", "", "100", "q1"); err != nil || !ok {
		t.Fatalf("insert live resolved ok=%t err=%v", ok, err)
	}
	var outbox int
	if err := source.db.QueryRow(`SELECT COUNT(*) FROM event_shard_outbox WHERE transaction_hash='0xresolved'`).Scan(&outbox); err != nil || outbox != 1 {
		t.Fatalf("transactional outbox=%d err=%v", outbox, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go replicator.Run(ctx)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var signal, lifecycle, pending int
		_ = replicator.signal.QueryRow(`SELECT COUNT(*) FROM uma_oo_events`).Scan(&signal)
		_ = replicator.lifecycle.QueryRow(`SELECT COUNT(*) FROM uma_oo_events`).Scan(&lifecycle)
		_ = source.db.QueryRow(`SELECT COUNT(*) FROM event_shard_outbox WHERE completed_at=0`).Scan(&pending)
		if signal == 1 && lifecycle == 2 && pending == 0 {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	var signal, lifecycle int
	_ = replicator.signal.QueryRow(`SELECT COUNT(*) FROM uma_oo_events`).Scan(&signal)
	_ = replicator.lifecycle.QueryRow(`SELECT COUNT(*) FROM uma_oo_events`).Scan(&lifecycle)
	if signal != 1 || lifecycle != 2 {
		t.Fatal("shadow shards did not converge")
	}
	if err := replicator.EnableShardPrimary(); err != nil {
		t.Fatal(err)
	}
	if ok, _, _, err := source.InsertEvent("propose", "0xprimary", 4, 13, 23, "c3", "m3", "98", ""); err != nil || !ok {
		t.Fatalf("insert shard-primary proposal ok=%t err=%v", ok, err)
	}
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var inShard, inLegacy int
		_ = replicator.signal.QueryRow(`SELECT COUNT(*) FROM uma_oo_events WHERE transaction_hash='0xprimary'`).Scan(&inShard)
		_ = source.db.QueryRow(`SELECT COUNT(*) FROM uma_oo_events WHERE transaction_hash='0xprimary'`).Scan(&inLegacy)
		if inShard == 1 && inLegacy == 1 {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("shard-primary reverse replication did not converge")
}

func TestShardPrimaryWithoutLegacyReplication(t *testing.T) {
	dir := t.TempDir()
	source, err := Open(filepath.Join(dir, "legacy.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	replicator, err := OpenEventShardReplicator(source, filepath.Join(dir, "signal.sqlite"), filepath.Join(dir, "lifecycle.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer replicator.Close()
	if err := replicator.EnableShardPrimaryWithLegacyReplication(false); err != nil {
		t.Fatal(err)
	}
	if ok, _, _, err := source.InsertEvent("propose", "0xcutover", 1, 20, time.Now().Unix(), "c", "m", "1", ""); err != nil || !ok {
		t.Fatalf("insert cutover event ok=%t err=%v", ok, err)
	}
	var inShard, inLegacy, pending int
	_ = replicator.signal.QueryRow(`SELECT COUNT(*) FROM uma_oo_events WHERE transaction_hash='0xcutover'`).Scan(&inShard)
	_ = source.db.QueryRow(`SELECT COUNT(*) FROM uma_oo_events WHERE transaction_hash='0xcutover'`).Scan(&inLegacy)
	_ = replicator.signal.QueryRow(`SELECT COUNT(*) FROM legacy_replication_outbox WHERE completed_at=0`).Scan(&pending)
	if inShard != 1 || inLegacy != 0 || pending != 0 {
		t.Fatalf("unexpected cutover state shard=%d legacy=%d pending=%d", inShard, inLegacy, pending)
	}
	rows, err := replicator.ScanEventsSince(RecentMemoryCutoffUnix())
	if err != nil || len(rows) != 1 || rows[0].TxHash != "0xcutover" {
		t.Fatalf("scan shards rows=%v err=%v", rows, err)
	}
}

func TestInsertSignalEventsCommitsBatchAndPreservesResults(t *testing.T) {
	dir := t.TempDir()
	source, err := Open(filepath.Join(dir, "legacy.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	replicator, err := OpenEventShardReplicator(source, filepath.Join(dir, "signal.sqlite"), filepath.Join(dir, "lifecycle.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer replicator.Close()
	if err := replicator.EnableShardPrimaryWithLegacyReplication(false); err != nil {
		t.Fatal(err)
	}

	// Deliberately submit reverse transaction order. The group commit sorts the
	// durable writes but returns results in caller order.
	events := []EventInsert{
		{EventType: "propose", TxHash: "0xb", LogIndex: 2, BlockNumber: 10, Timestamp: 100, ConditionID: "c2"},
		{EventType: "propose", TxHash: "0xa", LogIndex: 1, BlockNumber: 10, Timestamp: 100, ConditionID: "c1"},
		{EventType: "dispute", TxHash: "0xc", LogIndex: 3, BlockNumber: 10, Timestamp: 100, ConditionID: "c3"},
	}
	results := source.InsertSignalEvents(events)
	if len(results) != len(events) {
		t.Fatalf("results=%d, want %d", len(results), len(events))
	}
	for i, result := range results {
		if result.Err != nil || !result.Inserted || result.LastID == 0 {
			t.Fatalf("result[%d]=%+v", i, result)
		}
	}
	if results[1].CursorID != 100000 || results[0].CursorID != 100001 || results[2].CursorID != 100002 {
		t.Fatalf("unexpected caller-mapped cursors: %+v", results)
	}

	duplicate := source.InsertSignalEvents(events)
	for i, result := range duplicate {
		if result.Err != nil || result.Inserted {
			t.Fatalf("duplicate[%d]=%+v", i, result)
		}
	}
	var signalRows, legacyRows int
	_ = replicator.signal.QueryRow(`SELECT COUNT(*) FROM uma_oo_events`).Scan(&signalRows)
	_ = source.db.QueryRow(`SELECT COUNT(*) FROM uma_oo_events`).Scan(&legacyRows)
	if signalRows != 3 || legacyRows != 0 {
		t.Fatalf("signal rows=%d legacy rows=%d", signalRows, legacyRows)
	}
}
