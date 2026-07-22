package syncer

import (
	"sync/atomic"
	"testing"

	"github.com/polymas/poly_uma/internal/store"
)

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
	runCompletionCoordinator(results, &checkpoint)
	if got := checkpoint.Load(); got != 101 {
		t.Fatalf("checkpoint = %d, want 101", got)
	}
}
