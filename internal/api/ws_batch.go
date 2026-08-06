package api

import (
	"sort"
	"strconv"
	"time"

	"github.com/polymas/poly_uma/internal/store"
)

type wsPendingBatch struct {
	first time.Time
	last  time.Time
	rows  []store.EventRow
}

// wsBatchAccumulator keeps multiple transaction batches open concurrently.
// Realtime processing can interleave transactions while SQLite serializes
// their writes; flushing on every key change turns those bursts into one-event
// websocket frames and amplifies downstream head-of-line blocking.
type wsBatchAccumulator struct {
	idle, max time.Duration
	pending   map[string]*wsPendingBatch
}

func newWSBatchAccumulator(idle, max time.Duration) *wsBatchAccumulator {
	if idle <= 0 {
		idle = 25 * time.Millisecond
	}
	if max < idle {
		max = 250 * time.Millisecond
	}
	return &wsBatchAccumulator{idle: idle, max: max, pending: make(map[string]*wsPendingBatch)}
}

func wsBatchKey(row store.EventRow) string {
	return strconv.FormatUint(row.BlockNumber, 10) + ":" + row.TxHash
}

// Add retains interleaved transaction batches. It only returns a batch when a
// single transaction reaches the hard frame limit.
func (b *wsBatchAccumulator) Add(row store.EventRow, now time.Time) []store.EventRow {
	key := wsBatchKey(row)
	pending := b.pending[key]
	if pending == nil {
		pending = &wsPendingBatch{first: now}
		b.pending[key] = pending
	}
	pending.last = now
	pending.rows = append(pending.rows, row)
	if len(pending.rows) >= 128 {
		return b.flush(key)
	}
	return nil
}

// Due returns every transaction whose idle or maximum wait has elapsed.
func (b *wsBatchAccumulator) Due(now time.Time) [][]store.EventRow {
	keys := make([]string, 0, len(b.pending))
	for key, pending := range b.pending {
		if now.Sub(pending.last) >= b.idle || now.Sub(pending.first) >= b.max {
			keys = append(keys, key)
		}
	}
	if len(keys) == 0 {
		return nil
	}
	sort.Strings(keys)
	rows := make([][]store.EventRow, 0, len(keys))
	for _, key := range keys {
		if batch := b.flush(key); len(batch) > 0 {
			rows = append(rows, batch)
		}
	}
	return rows
}

func (b *wsBatchAccumulator) FlushAll() [][]store.EventRow {
	keys := make([]string, 0, len(b.pending))
	for key := range b.pending {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	rows := make([][]store.EventRow, 0, len(keys))
	for _, key := range keys {
		if batch := b.flush(key); len(batch) > 0 {
			rows = append(rows, batch)
		}
	}
	return rows
}

func (b *wsBatchAccumulator) flush(key string) []store.EventRow {
	pending := b.pending[key]
	if pending == nil || len(pending.rows) == 0 {
		return nil
	}
	delete(b.pending, key)
	rows := pending.rows
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].LogIndex < rows[j].LogIndex })
	return rows
}
