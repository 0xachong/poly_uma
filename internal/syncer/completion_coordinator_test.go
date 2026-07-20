package syncer

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/polymas/poly_uma/internal/store"
)

func TestCompletionCoordinatorBroadcastsInReceiveOrder(t *testing.T) {
	db, err := store.Open(t.TempDir() + "/coordinator.sqlite")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mem := store.NewMemReplica()
	ch, cancel := mem.Subscribe("propose")
	defer cancel()

	results := make(chan processingResult, 2)
	var checkpoint atomic.Uint64
	done := make(chan struct{})
	go func() {
		runCompletionCoordinator(results, mem, db, &checkpoint)
		close(done)
	}()
	row1 := &store.EventRow{EventType: "propose", TxHash: "first"}
	row2 := &store.EventRow{EventType: "propose", TxHash: "second"}
	results <- processingResult{sequence: 2, blockNumber: 102, handled: true, broadcastRow: row2}
	results <- processingResult{sequence: 1, blockNumber: 101, handled: true, broadcastRow: row1}
	close(results)
	<-done

	for index, want := range []string{"first", "second"} {
		select {
		case got := <-ch:
			if got.TxHash != want {
				t.Fatalf("broadcast %d = %q, want %q", index, got.TxHash, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for broadcast %d", index)
		}
	}
	if got := checkpoint.Load(); got != 102 {
		t.Fatalf("checkpoint = %d, want 102", got)
	}
}

func TestCompletionCoordinatorDoesNotCheckpointPastFailure(t *testing.T) {
	db, err := store.Open(t.TempDir() + "/coordinator.sqlite")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	results := make(chan processingResult, 3)
	var checkpoint atomic.Uint64
	results <- processingResult{sequence: 1, blockNumber: 101, handled: true}
	results <- processingResult{sequence: 2, blockNumber: 102, handled: false}
	results <- processingResult{sequence: 3, blockNumber: 103, handled: true}
	close(results)
	runCompletionCoordinator(results, nil, db, &checkpoint)
	if got := checkpoint.Load(); got != 101 {
		t.Fatalf("checkpoint = %d, want 101", got)
	}
}
