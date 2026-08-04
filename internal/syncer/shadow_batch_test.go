package syncer

import (
	"testing"
	"time"
)

func TestShadowBatcherGroupsSameTransactionUntilIdle(t *testing.T) {
	b := newShadowBatcher(2*time.Millisecond, 5*time.Millisecond)
	t0 := time.Unix(0, 0)
	b.observeAt(10, "0xtx", t0)
	b.observeAt(10, "0xtx", t0.Add(time.Millisecond))
	if got := b.sweepAt(t0.Add(2*time.Millisecond), false); len(got) != 0 {
		t.Fatalf("flushed before idle window: %v", got)
	}
	got := b.sweepAt(t0.Add(3*time.Millisecond), false)
	if len(got) != 1 || got[0].count != 2 {
		t.Fatalf("flush=%+v", got)
	}
}

func TestShadowBatcherHonorsMaximumWait(t *testing.T) {
	b := newShadowBatcher(2*time.Millisecond, 5*time.Millisecond)
	t0 := time.Unix(0, 0)
	b.observeAt(10, "0xtx", t0)
	b.observeAt(10, "0xtx", t0.Add(4*time.Millisecond))
	got := b.sweepAt(t0.Add(5*time.Millisecond), false)
	if len(got) != 1 || got[0].count != 2 {
		t.Fatalf("max-wait flush=%+v", got)
	}
}
