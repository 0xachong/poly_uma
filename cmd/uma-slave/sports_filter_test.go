package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func TestSportsFilterKeepsNonSportsAndAllowedMoneyline(t *testing.T) {
	filter, err := parseSportsTypeFilter("moneyline,child_moneyline", true)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte(`{"events":[
		{"c":"politics","tags":["politics"]},
		{"c":"sports-ok","tag_ids":["1"],"s":"moneyline"},
		{"c":"esports-ok","tags":["esports"],"s":"child_moneyline"},
		{"c":"sports-bad","tags":["sports"],"s":"handicap"},
		{"c":"esports-empty","tag_ids":["64"]}
	]}`)
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
	if len(envelope.Events) != 3 || envelope.Events[0].ConditionID != "politics" || envelope.Events[2].ConditionID != "esports-ok" {
		t.Fatalf("events=%+v", envelope.Events)
	}
}

func TestCompactTradeUsesDedicatedSharedHub(t *testing.T) {
	target, _ := url.Parse("http://127.0.0.1")
	handler := newSlaveServer(target, 8)
	request := httptest.NewRequest(http.MethodGet, compactEventsPath+"?batch=true&format=compact_trade&sports_types=moneyline,child_moneyline", nil)
	if hub := handler.relayHub(request); hub != handler.hubs[compactTradeKey] {
		t.Fatal("compact_trade request did not select its shared upstream hub")
	}
}
