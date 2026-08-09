package syncer

import (
	"context"
	"log"
	"sync/atomic"
	"time"

	"github.com/polymas/poly_uma/internal/store"
)

const defaultSignalBatchSize = 64

type signalWriteRequest struct {
	event  store.EventInsert
	result chan store.EventInsertResult
}

// signalBatchWriter is a group-commit boundary, not another processing lane.
// Existing signal workers still prepare rows concurrently; this writer only
// serializes their short SQLite commit and returns the per-row result.
type signalBatchWriter struct {
	db       *store.SQLite
	idle     time.Duration
	maxWait  time.Duration
	maxBatch int
	requests chan signalWriteRequest
	done     chan struct{}

	batches atomic.Uint64
	events  atomic.Uint64
	maxSize atomic.Uint64
}

func newSignalBatchWriter(db *store.SQLite, idle, maxWait time.Duration, capacity int) *signalBatchWriter {
	if idle <= 0 {
		idle = 2 * time.Millisecond
	}
	if maxWait < idle {
		maxWait = 5 * time.Millisecond
	}
	if capacity <= 0 {
		capacity = 256
	}
	return &signalBatchWriter{
		db: db, idle: idle, maxWait: maxWait, maxBatch: defaultSignalBatchSize,
		requests: make(chan signalWriteRequest, capacity), done: make(chan struct{}),
	}
}

func (w *signalBatchWriter) submit(ctx context.Context, event store.EventInsert) store.EventInsertResult {
	result := make(chan store.EventInsertResult, 1)
	select {
	case w.requests <- signalWriteRequest{event: event, result: result}:
	case <-ctx.Done():
		return store.EventInsertResult{Err: ctx.Err()}
	}
	// Once accepted, always wait for the short commit result. Returning on
	// cancellation here could revert memory while the transaction still commits.
	return <-result
}

func (w *signalBatchWriter) run() {
	defer close(w.done)
	for first := range w.requests {
		batch := []signalWriteRequest{first}
		idleTimer := time.NewTimer(w.idle)
		maxTimer := time.NewTimer(w.maxWait)
	collect:
		for len(batch) < w.maxBatch {
			select {
			case request, ok := <-w.requests:
				if !ok {
					break collect
				}
				batch = append(batch, request)
				if !idleTimer.Stop() {
					select {
					case <-idleTimer.C:
					default:
					}
				}
				idleTimer.Reset(w.idle)
			case <-idleTimer.C:
				break collect
			case <-maxTimer.C:
				break collect
			}
		}
		if !idleTimer.Stop() {
			select {
			case <-idleTimer.C:
			default:
			}
		}
		if !maxTimer.Stop() {
			select {
			case <-maxTimer.C:
			default:
			}
		}

		events := make([]store.EventInsert, len(batch))
		for i := range batch {
			events[i] = batch[i].event
		}
		startedAt := time.Now()
		results := w.db.InsertSignalEvents(events)
		commitTime := time.Since(startedAt)
		w.db.ObserveSignalBatch(len(batch), commitTime)
		w.batches.Add(1)
		w.events.Add(uint64(len(batch)))
		for previous := w.maxSize.Load(); uint64(len(batch)) > previous; previous = w.maxSize.Load() {
			if w.maxSize.CompareAndSwap(previous, uint64(len(batch))) {
				break
			}
		}
		if len(batch) > 1 || commitTime >= 100*time.Millisecond {
			log.Printf("[INFO] signal group commit: size=%d sqlite_ms=%d", len(batch), commitTime.Milliseconds())
		}
		for i := range batch {
			batch[i].result <- results[i]
		}
	}
}

func (w *signalBatchWriter) close() {
	close(w.requests)
	<-w.done
}
