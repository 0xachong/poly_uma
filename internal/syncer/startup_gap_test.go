package syncer

import (
	"context"
	"testing"

	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/polymas/poly_uma/internal/store"
)

type emptyHistoryClient struct {
	latest uint64
	ranges [][2]uint64
}

func (c *emptyHistoryClient) LatestBlock(context.Context) (uint64, error) {
	return c.latest, nil
}

func (c *emptyHistoryClient) FetchLogs(_ context.Context, from, to uint64) ([]ethtypes.Log, error) {
	c.ranges = append(c.ranges, [2]uint64{from, to})
	return nil, nil
}

func TestStartupGapBackfillAdvancesAcrossEmptyBlocks(t *testing.T) {
	db, err := store.Open(t.TempDir() + "/startup-gap.sqlite")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.SetCheckpoint(100); err != nil {
		t.Fatal(err)
	}

	first := &emptyHistoryClient{latest: 105}
	if err := backfillWithClient(context.Background(), Config{}, "", first, db, newTsCache(), nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	second := &emptyHistoryClient{latest: 108}
	if err := backfillWithClient(context.Background(), Config{}, "", second, db, newTsCache(), nil, nil, nil); err != nil {
		t.Fatal(err)
	}

	if len(first.ranges) != 1 || first.ranges[0] != [2]uint64{101, 105} {
		t.Fatalf("first ranges=%v", first.ranges)
	}
	if len(second.ranges) != 1 || second.ranges[0] != [2]uint64{106, 108} {
		t.Fatalf("second ranges=%v", second.ranges)
	}
	checkpoint, err := db.GetCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint != 108 {
		t.Fatalf("checkpoint=%d, want 108", checkpoint)
	}
}

func TestBackfillUsesMemoryBoundedBlockRanges(t *testing.T) {
	db, err := store.Open(t.TempDir() + "/bounded-backfill.sqlite")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.SetCheckpoint(100); err != nil {
		t.Fatal(err)
	}

	client := &emptyHistoryClient{latest: 145}
	if err := backfillWithClient(context.Background(), Config{}, "", client, db, newTsCache(), nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	want := [][2]uint64{{101, 120}, {121, 140}, {141, 145}}
	if len(client.ranges) != len(want) {
		t.Fatalf("ranges=%v, want %v", client.ranges, want)
	}
	for i := range want {
		if client.ranges[i] != want[i] {
			t.Fatalf("ranges[%d]=%v, want %v", i, client.ranges[i], want[i])
		}
	}
}

func TestCheckpointNeverRegresses(t *testing.T) {
	db, err := store.Open(t.TempDir() + "/monotonic-checkpoint.sqlite")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.SetCheckpoint(200); err != nil {
		t.Fatal(err)
	}
	if err := db.SetCheckpoint(150); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := db.GetCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint != 200 {
		t.Fatalf("checkpoint=%d, want 200", checkpoint)
	}
}
