package syncer

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/polymas/poly_uma/internal/store"
	"github.com/polymas/poly_uma/internal/uma"
)

type upstreamHeadTracker struct {
	db      *store.SQLite
	mu      sync.Mutex
	heads   map[uint64]time.Time
	pending map[uint64]time.Time
}

func newUpstreamHeadTracker(db *store.SQLite) *upstreamHeadTracker {
	return &upstreamHeadTracker{db: db, heads: make(map[uint64]time.Time), pending: make(map[uint64]time.Time)}
}

func (t *upstreamHeadTracker) observeHead(observation uma.HeadObservation) {
	t.db.ObserveHead(observation.Number, observation.Timestamp, observation.ReceivedAt)
	t.mu.Lock()
	t.heads[observation.Number] = observation.ReceivedAt
	if logReceivedAt, ok := t.pending[observation.Number]; ok {
		t.db.ObserveLogAfterHead(logReceivedAt.Sub(observation.ReceivedAt), true)
		delete(t.pending, observation.Number)
	}
	t.evict(observation.Number)
	t.mu.Unlock()
}

func (t *upstreamHeadTracker) observeLog(block uint64, receivedAt time.Time) {
	if block == 0 || receivedAt.IsZero() {
		return
	}
	t.mu.Lock()
	if headReceivedAt, ok := t.heads[block]; ok {
		t.db.ObserveLogAfterHead(receivedAt.Sub(headReceivedAt), false)
	} else {
		t.pending[block] = receivedAt
	}
	t.evict(block)
	t.mu.Unlock()
}

func (t *upstreamHeadTracker) evict(latest uint64) {
	if latest <= 512 {
		return
	}
	cutoff := latest - 512
	for block := range t.heads {
		if block < cutoff {
			delete(t.heads, block)
		}
	}
	for block := range t.pending {
		if block < cutoff {
			delete(t.pending, block)
		}
	}
}

func runUpstreamHeadObserver(ctx context.Context, client *uma.Client, tracker *upstreamHeadTracker) {
	backoff := time.Second
	for ctx.Err() == nil {
		heads, cleanup, err := client.SubscribeNewHeads(ctx)
		if err != nil {
			log.Printf("[WARN] newHeads 诊断订阅失败: %v", err)
			if !sleepContext(ctx, backoff) {
				return
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}
		log.Printf("[INFO] newHeads 诊断订阅已建立")
		backoff = time.Second
		for observation := range heads {
			tracker.observeHead(observation)
		}
		cleanup()
		if ctx.Err() == nil && !sleepContext(ctx, backoff) {
			return
		}
	}
}
