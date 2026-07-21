package syncer

import (
	"testing"

	"github.com/polymas/poly_uma/internal/uma"
)

func queuedEvent(sequence uint64) *uma.SubscribedEvent {
	return &uma.SubscribedEvent{Sequence: sequence}
}

func TestPriorityQueueTakesHighBeforeQueuedNormal(t *testing.T) {
	q := newPriorityEventQueue(4, 20)
	q.push(queuedEvent(1), false)
	q.push(queuedEvent(2), false)
	q.push(queuedEvent(3), true)

	for index, want := range []uint64{3, 1, 2} {
		got, ok := q.pop()
		if !ok || got.Sequence != want {
			t.Fatalf("pop %d = (%v, %t), want sequence %d", index, got, ok, want)
		}
	}
}

func TestPriorityQueuePreventsNormalStarvation(t *testing.T) {
	q := newPriorityEventQueue(8, 2)
	q.push(queuedEvent(1), false)
	q.push(queuedEvent(2), true)
	q.push(queuedEvent(3), true)
	q.push(queuedEvent(4), true)

	for index, want := range []uint64{2, 3, 1, 4} {
		got, ok := q.pop()
		if !ok || got.Sequence != want {
			t.Fatalf("pop %d = (%v, %t), want sequence %d", index, got, ok, want)
		}
	}
}

func TestPriorityQueueDrainsAfterClose(t *testing.T) {
	q := newPriorityEventQueue(2, 20)
	q.push(queuedEvent(1), false)
	q.close()
	if got, ok := q.pop(); !ok || got.Sequence != 1 {
		t.Fatalf("pop after close = (%v, %t)", got, ok)
	}
	if got, ok := q.pop(); ok || got != nil {
		t.Fatalf("empty pop after close = (%v, %t)", got, ok)
	}
}
