package api

import (
	"sort"
	"strconv"
	"time"

	"github.com/polymas/poly_uma/internal/store"
)

type wsBatchAccumulator struct {
	idle, max time.Duration
	key       string
	first     time.Time
	last      time.Time
	rows      []store.EventRow
}

func newWSBatchAccumulator(idle, max time.Duration) *wsBatchAccumulator {
	if idle <= 0 {
		idle = 2 * time.Millisecond
	}
	if max < idle {
		max = 5 * time.Millisecond
	}
	return &wsBatchAccumulator{idle: idle, max: max}
}

func wsBatchKey(row store.EventRow) string {
	return strconv.FormatUint(row.BlockNumber, 10) + ":" + row.TxHash
}

// Add returns a previous transaction batch when the routing key changes.
func (b *wsBatchAccumulator) Add(row store.EventRow, now time.Time) []store.EventRow {
	key := wsBatchKey(row)
	var flushed []store.EventRow
	if len(b.rows) > 0 && key != b.key {
		flushed = b.flush()
	}
	if len(b.rows) == 0 {
		b.key, b.first = key, now
	}
	b.last = now
	b.rows = append(b.rows, row)
	if len(b.rows) >= 128 {
		if len(flushed) == 0 {
			return b.flush()
		}
	}
	return flushed
}

func (b *wsBatchAccumulator) Due(now time.Time) []store.EventRow {
	if len(b.rows) == 0 || (now.Sub(b.last) < b.idle && now.Sub(b.first) < b.max) {
		return nil
	}
	return b.flush()
}

func (b *wsBatchAccumulator) flush() []store.EventRow {
	if len(b.rows) == 0 {
		return nil
	}
	rows := b.rows
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].LogIndex < rows[j].LogIndex })
	b.rows, b.key = nil, ""
	b.first, b.last = time.Time{}, time.Time{}
	return rows
}
