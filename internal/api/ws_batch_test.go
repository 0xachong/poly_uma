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
	got := b.Due(t0.Add(3 * time.Millisecond))
	if len(got) != 2 || got[0].LogIndex != 3 || got[1].LogIndex != 9 {
		t.Fatalf("batch=%+v", got)
	}
}

func TestWSBatchAccumulatorFlushesOnTransactionChange(t *testing.T) {
	b := newWSBatchAccumulator(2*time.Millisecond, 5*time.Millisecond)
	t0 := time.Unix(0, 0)
	b.Add(store.EventRow{TxHash: "0x1", BlockNumber: 10}, t0)
	got := b.Add(store.EventRow{TxHash: "0x2", BlockNumber: 10}, t0)
	if len(got) != 1 || got[0].TxHash != "0x1" {
		t.Fatalf("batch=%+v", got)
	}
}
