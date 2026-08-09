package store

import (
	"testing"
	"time"
)

func TestObserveHeadClassifiesDelayAndCatchup(t *testing.T) {
	db := &SQLite{}
	base := time.Unix(100, 200*int64(time.Millisecond))
	db.ObserveHead(10, 100, base)
	db.ObserveHead(11, 99, base.Add(200*time.Millisecond))
	stats := db.PipelineStats()
	if stats.LastHeadNumber != 11 || stats.LastHeadDelayMillis != 1400 {
		t.Fatalf("head stats=%+v", stats)
	}
	if stats.LastHeadArrivalGapMillis != 200 || stats.LastHeadBlockGap != 1 {
		t.Fatalf("arrival classification=%+v", stats)
	}
	if stats.HeadDelayOverOneSecond != 1 || stats.HeadCatchupBursts != 1 {
		t.Fatalf("catchup classification=%+v", stats)
	}
}

func TestObserveHeadClampsClockSkew(t *testing.T) {
	db := &SQLite{}
	db.ObserveHead(10, 101, time.Unix(100, 0))
	stats := db.PipelineStats()
	if stats.LastHeadDelayMillis != 0 || stats.HeadClockAdjusted != 1 {
		t.Fatalf("clock adjustment=%+v", stats)
	}
}
