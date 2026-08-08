package main

import (
	"testing"

	wirev5 "github.com/polymas/poly_uma/internal/wire/v5"
	"google.golang.org/protobuf/proto"
)

func TestProtobufRelayAddsTimestampsAndFiltersSports(t *testing.T) {
	tickSize := 0.001
	input, err := proto.Marshal(&wirev5.CompactTradeBatch{
		SchemaVersion: 5,
		Events: []*wirev5.CompactTradeEvent{
			{ConditionId: "moneyline", TagIds: []string{"1"}, SportsMarketType: "moneyline", Market: &wirev5.CompactTradeMarket{TickSize: &tickSize}},
			{ConditionId: "totals", TagIds: []string{"1"}, SportsMarketType: "totals"},
			{ConditionId: "politics", TagIds: []string{"2"}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	stamped := addProtobufRelayTimestamps(input, 1234)
	filter, err := parseSportsTypeFilter("moneyline,child_moneyline", true)
	if err != nil {
		t.Fatal(err)
	}
	filtered, err := filterProtobufSportsBatch(stamped, filter)
	if err != nil {
		t.Fatal(err)
	}
	var batch wirev5.CompactTradeBatch
	if err := proto.Unmarshal(filtered, &batch); err != nil {
		t.Fatal(err)
	}
	if batch.SlaveReceivedAtUs != 1234000 || batch.SlaveBroadcastAtUs == 0 {
		t.Fatalf("timestamps=%d/%d", batch.SlaveReceivedAtUs, batch.SlaveBroadcastAtUs)
	}
	if len(batch.Events) != 2 || batch.Events[0].ConditionId != "moneyline" || batch.Events[1].ConditionId != "politics" {
		t.Fatalf("events=%+v", batch.Events)
	}
	if batch.Events[0].Market == nil || batch.Events[0].Market.TickSize == nil || *batch.Events[0].Market.TickSize != tickSize {
		t.Fatalf("tick_size was not preserved: event=%+v", batch.Events[0])
	}
}
