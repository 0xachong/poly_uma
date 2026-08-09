package syncer

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/polymas/poly_uma/internal/store"
)

func TestSignalBatchWriterGroupsConcurrentPreparedRows(t *testing.T) {
	dir := t.TempDir()
	db, err := store.Open(filepath.Join(dir, "legacy.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	replicator, err := store.OpenEventShardReplicator(db, filepath.Join(dir, "signal.sqlite"), filepath.Join(dir, "lifecycle.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer replicator.Close()
	if err := replicator.EnableShardPrimaryWithLegacyReplication(false); err != nil {
		t.Fatal(err)
	}

	writer := newSignalBatchWriter(db, 5*time.Millisecond, 10*time.Millisecond, 32)
	go writer.run()
	const count = 8
	start := make(chan struct{})
	results := make([]store.EventInsertResult, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			results[index] = writer.submit(context.Background(), store.EventInsert{
				EventType: "propose", TxHash: "0xbatch" + string(rune('a'+index)), LogIndex: index,
				BlockNumber: 10, Timestamp: time.Now().Unix(), ConditionID: "condition",
			})
		}(i)
	}
	close(start)
	wg.Wait()
	writer.close()
	for i, result := range results {
		if result.Err != nil || !result.Inserted {
			t.Fatalf("result[%d]=%+v", i, result)
		}
	}
	if writer.maxSize.Load() <= 1 || writer.batches.Load() >= count || writer.events.Load() != count {
		t.Fatalf("group commit stats batches=%d events=%d max_size=%d", writer.batches.Load(), writer.events.Load(), writer.maxSize.Load())
	}
}
