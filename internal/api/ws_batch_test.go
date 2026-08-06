package api

import (
	"testing"
	"time"

	"github.com/polymas/poly_uma/internal/store"
)

func TestWSBatchAccumulatorGroupsAndSortsTransaction(t *testing.T) {
	b := newWSBatchAccumulator(2*time.Millisecond, 5*time.Millisecond)
	t0 := time.Unix(0, 0)
	b.Add(store.EventRow{TxHash: "0xtx", BlockNumber: 10, LogIndex: 9}, t0)
	b.Add(store.EventRow{TxHash: "0xtx", BlockNumber: 10, LogIndex: 3}, t0.Add(time.Millisecond))
	if got := b.Due(t0.Add(2 * time.Millisecond)); got != nil {
		t.Fatalf("early flush=%v", got)
	}
	due := b.Due(t0.Add(3 * time.Millisecond))
	if len(due) != 1 {
		t.Fatalf("due batches=%v", due)
	}
	got := due[0]
	if len(got) != 2 || got[0].LogIndex != 3 || got[1].LogIndex != 9 {
		t.Fatalf("batch=%+v", got)
	}
}

func TestWSBatchAccumulatorRetainsInterleavedTransactions(t *testing.T) {
	b := newWSBatchAccumulator(2*time.Millisecond, 5*time.Millisecond)
	t0 := time.Unix(0, 0)
	b.Add(store.EventRow{TxHash: "0x1", BlockNumber: 10}, t0)
	if got := b.Add(store.EventRow{TxHash: "0x2", BlockNumber: 10}, t0); got != nil {
		t.Fatalf("transaction change flushed batch=%+v", got)
	}
	b.Add(store.EventRow{TxHash: "0x1", BlockNumber: 10, LogIndex: 2}, t0.Add(time.Millisecond))
	due := b.Due(t0.Add(3 * time.Millisecond))
	if len(due) != 2 || len(due[0])+len(due[1]) != 3 {
		t.Fatalf("interleaved batches=%+v", due)
	}
}
