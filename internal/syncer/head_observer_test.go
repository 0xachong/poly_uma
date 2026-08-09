package syncer

import (
	"testing"
	"time"

	"github.com/polymas/poly_uma/internal/store"
	"github.com/polymas/poly_uma/internal/uma"
)

func TestUpstreamHeadTrackerSplitsHeadAndLogDelivery(t *testing.T) {
	db := &store.SQLite{}
	tracker := newUpstreamHeadTracker(db)
	headAt := time.Unix(100, 200*int64(time.Millisecond))
	tracker.observeHead(uma.HeadObservation{Number: 10, Timestamp: 99, ReceivedAt: headAt})
	tracker.observeLog(10, headAt.Add(75*time.Millisecond))
	stats := db.PipelineStats()
	if stats.LastHeadDelayMillis != 1200 || stats.LastLogAfterHeadMillis != 75 {
		t.Fatalf("split latency=%+v", stats)
	}
}

func TestUpstreamHeadTrackerCorrelatesLogArrivingFirst(t *testing.T) {
	db := &store.SQLite{}
	tracker := newUpstreamHeadTracker(db)
	logAt := time.Unix(100, 0)
	tracker.observeLog(10, logAt)
	tracker.observeHead(uma.HeadObservation{Number: 10, Timestamp: 99, ReceivedAt: logAt.Add(20 * time.Millisecond)})
	stats := db.PipelineStats()
	if stats.LastLogAfterHeadMillis != 0 || stats.LogObservedBeforeHead != 1 {
		t.Fatalf("log-first classification=%+v", stats)
	}
}
