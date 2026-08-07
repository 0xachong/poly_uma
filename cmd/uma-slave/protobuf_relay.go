package main

import (
	"strings"
	"time"

	wirev5 "github.com/polymas/poly_uma/internal/wire/v5"
	"google.golang.org/protobuf/proto"
)

func addProtobufRelayTimestamps(payload []byte, receivedAtMS int64) []byte {
	var batch wirev5.CompactTradeBatch
	if err := proto.Unmarshal(payload, &batch); err != nil {
		return payload
	}
	batch.SlaveReceivedAtUs = receivedAtMS * 1000
	batch.SlaveBroadcastAtUs = time.Now().UnixMicro()
	encoded, err := proto.Marshal(&batch)
	if err != nil {
		return payload
	}
	return encoded
}

func filterProtobufSportsBatch(payload []byte, filter *sportsTypeFilter) ([]byte, error) {
	if filter == nil {
		return payload, nil
	}
	var batch wirev5.CompactTradeBatch
	if err := proto.Unmarshal(payload, &batch); err != nil {
		return nil, err
	}
	kept := make([]*wirev5.CompactTradeEvent, 0, len(batch.Events))
	for _, event := range batch.Events {
		if isSportsOrEsports(event.TagIds) {
			if _, ok := filter.allowed[strings.ToLower(strings.TrimSpace(event.SportsMarketType))]; !ok {
				continue
			}
		}
		kept = append(kept, event)
	}
	if len(kept) == len(batch.Events) {
		return payload, nil
	}
	if len(kept) == 0 {
		return nil, nil
	}
	batch.Events = kept
	return proto.Marshal(&batch)
}

func compactProtobufEventCount(payload []byte) int {
	var batch wirev5.CompactTradeBatch
	if proto.Unmarshal(payload, &batch) != nil {
		return 0
	}
	return len(batch.Events)
}
