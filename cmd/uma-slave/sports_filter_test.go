package main

import (
	"encoding/json"
	"net/url"
	"testing"
	"time"
)

func TestSportsTypeFilterKeepsOtherMarketsAndWinnerMarkets(t *testing.T) {
	filter, err := parseSportsTypeFilter("moneyline,child_moneyline", true)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte(`{"message_type":"uma_event_batch","events":[` +
		`{"c":"non-sports","tag_ids":["2"]},` +
		`{"c":"sports-moneyline","tag_ids":["1"],"s":"moneyline"},` +
		`{"c":"esports-child","tag_ids":["64"],"s":"child_moneyline"},` +
		`{"c":"sports-handicap","tag_ids":["1"],"s":"map_handicap"},` +
		`{"c":"esports-total","tag_ids":["64"],"s":"totals"},` +
		`{"c":"sports-unknown","tag_ids":["1"]}` +
		`]}`)
	filtered, err := filterCompactSportsBatch(payload, filter)
	if err != nil {
		t.Fatal(err)
	}
	var envelope struct {
		Events []struct {
			ConditionID string `json:"c"`
		} `json:"events"`
	}
	if err := json.Unmarshal(filtered, &envelope); err != nil {
		t.Fatal(err)
	}
	got := make([]string, 0, len(envelope.Events))
	for _, event := range envelope.Events {
		got = append(got, event.ConditionID)
	}
	want := []string{"non-sports", "sports-moneyline", "esports-child"}
	if len(got) != len(want) {
		t.Fatalf("conditions=%v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("conditions=%v, want %v", got, want)
		}
	}
}

func TestRelayBroadcastAppliesSportsFilterPerSubscriber(t *testing.T) {
	filter, err := parseSportsTypeFilter("moneyline,child_moneyline", true)
	if err != nil {
		t.Fatal(err)
	}
	target, _ := url.Parse("http://127.0.0.1")
	hub := newRelayHub(target, compactEventsPath, "batch=true&format=compact", 4)
	unfiltered := hub.subscribe(1, compactEventsPath, "192.0.2.1", "1", time.Now(), nil)
	filtered := hub.subscribe(2, compactEventsPath, "192.0.2.2", "2", time.Now(), filter)
	payload := []byte(`{"events":[` +
		`{"c":"other","tag_ids":["2"]},` +
		`{"c":"winner","tag_ids":["1"],"s":"moneyline"},` +
		`{"c":"handicap","tag_ids":["1"],"s":"map_handicap"}` +
		`]}`)
	hub.broadcast(payload)
	if got := <-unfiltered.send; string(got) != string(payload) {
		t.Fatalf("unfiltered payload changed: %s", got)
	}
	var envelope struct {
		Events []json.RawMessage `json:"events"`
	}
	if err := json.Unmarshal(<-filtered.send, &envelope); err != nil {
		t.Fatal(err)
	}
	if len(envelope.Events) != 2 {
		t.Fatalf("filtered events=%d, want 2", len(envelope.Events))
	}
}

func TestSportsTypeFilterDropsEmptyBatch(t *testing.T) {
	filter, err := parseSportsTypeFilter("moneyline,child_moneyline", true)
	if err != nil {
		t.Fatal(err)
	}
	filtered, err := filterCompactSportsBatch([]byte(`{"events":[{"tag_ids":["1"],"s":"totals"}]}`), filter)
	if err != nil {
		t.Fatal(err)
	}
	if filtered != nil {
		t.Fatalf("filtered=%s, want no frame", filtered)
	}
}

func TestSportsTypeFilterAbsentIsCompatible(t *testing.T) {
	payload := []byte(`{"events":[{"tag_ids":["1"],"s":"map_handicap"}]}`)
	filtered, err := filterCompactSportsBatch(payload, nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(filtered) != string(payload) {
		t.Fatalf("unfiltered payload changed: %s", filtered)
	}
}

func TestParseSportsTypeFilterCanonicalizesAndRejectsInvalid(t *testing.T) {
	filter, err := parseSportsTypeFilter(" child_moneyline,MoneyLine,moneyline ", true)
	if err != nil {
		t.Fatal(err)
	}
	if filter.key != "child_moneyline,moneyline" {
		t.Fatalf("key=%q", filter.key)
	}
	if _, err := parseSportsTypeFilter("moneyline/other", true); err == nil {
		t.Fatal("invalid sports type was accepted")
	}
	if filter, err := parseSportsTypeFilter("", false); err != nil || filter != nil {
		t.Fatalf("absent filter=%v err=%v", filter, err)
	}
}
