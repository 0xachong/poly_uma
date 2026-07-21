package syncer

import (
	"sync"

	"github.com/polymas/poly_uma/internal/uma"
)

const defaultHighBurst = 20

// priorityEventQueue is a bounded two-lane queue. High-priority events jump
// ahead of queued lifecycle events, while highBurst guarantees that a
// sustained high-priority stream cannot starve the normal lane forever.
type priorityEventQueue struct {
	mu              sync.Mutex
	notEmpty        *sync.Cond
	notFull         *sync.Cond
	high            []*uma.SubscribedEvent
	normal          []*uma.SubscribedEvent
	capacity        int
	highBurst       int
	consecutiveHigh int
	closed          bool
}

func newPriorityEventQueue(capacity, highBurst int) *priorityEventQueue {
	if capacity <= 0 {
		capacity = 1
	}
	if highBurst <= 0 {
		highBurst = defaultHighBurst
	}
	q := &priorityEventQueue{capacity: capacity, highBurst: highBurst}
	q.notEmpty = sync.NewCond(&q.mu)
	q.notFull = sync.NewCond(&q.mu)
	return q
}

func (q *priorityEventQueue) push(event *uma.SubscribedEvent, high bool) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	for !q.closed && len(q.high)+len(q.normal) >= q.capacity {
		q.notFull.Wait()
	}
	if q.closed {
		return false
	}
	if high {
		q.high = append(q.high, event)
	} else {
		q.normal = append(q.normal, event)
	}
	q.notEmpty.Signal()
	return true
}

func (q *priorityEventQueue) pop() (*uma.SubscribedEvent, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for !q.closed && len(q.high)+len(q.normal) == 0 {
		q.notEmpty.Wait()
	}
	if len(q.high)+len(q.normal) == 0 {
		return nil, false
	}
	var event *uma.SubscribedEvent
	if len(q.high) > 0 && (len(q.normal) == 0 || q.consecutiveHigh < q.highBurst) {
		event = q.high[0]
		q.high[0] = nil
		q.high = q.high[1:]
		q.consecutiveHigh++
	} else {
		event = q.normal[0]
		q.normal[0] = nil
		q.normal = q.normal[1:]
		q.consecutiveHigh = 0
	}
	q.notFull.Signal()
	return event, true
}

func (q *priorityEventQueue) depths() (high, normal int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.high), len(q.normal)
}

func (q *priorityEventQueue) close() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	q.notEmpty.Broadcast()
	q.notFull.Broadcast()
}

func isHighPriorityEvent(event *uma.SubscribedEvent) bool {
	if event == nil || event.Event == nil {
		return false
	}
	typeName := kindToType(event.Event.Kind)
	return typeName == "propose" || typeName == "dispute"
}
