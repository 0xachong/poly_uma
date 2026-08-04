package syncer

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/polymas/poly_uma/internal/uma"
)

type shadowBatchKey struct {
	block uint64
	tx    string
}

type shadowBatch struct {
	first time.Time
	last  time.Time
	count int
}

// shadowBatcher simulates transaction-scoped micro-batching. It never changes
// delivery: observations are diagnostic only until batch subscribers opt in.
type shadowBatcher struct {
	idle time.Duration
	max  time.Duration
	in   chan *uma.SubscribedEvent

	mu      sync.Mutex
	pending map[shadowBatchKey]shadowBatch
	seen    atomic.Uint64
	flushes atomic.Uint64
	dropped atomic.Uint64
}

func newShadowBatcher(idle, max time.Duration) *shadowBatcher {
	if idle <= 0 {
		idle = 2 * time.Millisecond
	}
	if max < idle {
		max = 5 * time.Millisecond
	}
	return &shadowBatcher{
		idle: idle, max: max, in: make(chan *uma.SubscribedEvent, 4096),
		pending: make(map[shadowBatchKey]shadowBatch),
	}
}

func (b *shadowBatcher) Observe(event *uma.SubscribedEvent) {
	if event == nil || !isHighPriorityEvent(event) {
		return
	}
	select {
	case b.in <- event:
	default:
		b.dropped.Add(1)
	}
}

func (b *shadowBatcher) Run(ctx context.Context) {
	sweep := time.NewTicker(time.Millisecond)
	report := time.NewTicker(time.Minute)
	defer sweep.Stop()
	defer report.Stop()
	for {
		select {
		case <-ctx.Done():
			b.sweepAt(time.Now().Add(b.max), true)
			return
		case event := <-b.in:
			at := event.ReceivedAt
			if at.IsZero() {
				at = time.Now()
			}
			b.observeAt(event.Raw.BlockNumber, event.Raw.TxHash.Hex(), at)
		case now := <-sweep.C:
			b.sweepAt(now, false)
		case <-report.C:
			log.Printf("[INFO] shadow batch stats: seen=%d flushes=%d dropped=%d idle=%s max=%s",
				b.seen.Load(), b.flushes.Load(), b.dropped.Load(), b.idle, b.max)
		}
	}
}

func (b *shadowBatcher) observeAt(block uint64, tx string, at time.Time) {
	if block == 0 || tx == "" {
		return
	}
	b.mu.Lock()
	key := shadowBatchKey{block: block, tx: tx}
	batch, ok := b.pending[key]
	if !ok {
		batch.first = at
	}
	batch.last = at
	batch.count++
	b.pending[key] = batch
	b.mu.Unlock()
	b.seen.Add(1)
}

func (b *shadowBatcher) sweepAt(now time.Time, force bool) []shadowBatch {
	b.mu.Lock()
	var flushed []shadowBatch
	for key, batch := range b.pending {
		if !force && now.Sub(batch.last) < b.idle && now.Sub(batch.first) < b.max {
			continue
		}
		delete(b.pending, key)
		flushed = append(flushed, batch)
		b.flushes.Add(1)
		if batch.count > 1 {
			log.Printf("[INFO] shadow batch: block=%d tx=%s events=%d arrival_span_us=%d simulated_wait_us=%d",
				key.block, key.tx, batch.count, batch.last.Sub(batch.first).Microseconds(), now.Sub(batch.first).Microseconds())
		}
	}
	b.mu.Unlock()
	return flushed
}
