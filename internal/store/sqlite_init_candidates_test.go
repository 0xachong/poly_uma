package store

import (
	"testing"
	"time"
)

func TestListUMAInitCandidatesExcludesAlreadyProposedAndOld(t *testing.T) {
	db, err := Open(t.TempDir() + "/events.sqlite")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Now().Unix()
	rows := []struct {
		kind, tx, market string
		ts               int64
	}{
		{"init", "0xinit-candidate", "candidate", now - 60},
		{"init", "0xinit-done", "done", now - 60},
		{"propose", "0xpropose-done", "done", now - 30},
		{"init", "0xinit-old", "old", now - 9*24*3600},
	}
	for i, row := range rows {
		if inserted, _, _, err := db.InsertEvent(row.kind, row.tx, i, 100, row.ts, "0xcondition", row.market, "", ""); err != nil || !inserted {
			t.Fatalf("insert %+v inserted=%t err=%v", row, inserted, err)
		}
	}
	got, err := db.ListUMAInitCandidates(now-7*24*3600, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != "candidate" {
		t.Fatalf("candidates=%v", got)
	}
}
