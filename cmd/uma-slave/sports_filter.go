package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

type sportsTypeFilter struct {
	key     string
	allowed map[string]struct{}
}

func parseSportsTypeFilter(raw string, present bool) (*sportsTypeFilter, error) {
	if !present {
		return nil, nil
	}
	allowed := make(map[string]struct{})
	for _, value := range strings.Split(raw, ",") {
		value = strings.ToLower(strings.TrimSpace(value))
		if value == "" {
			continue
		}
		for _, char := range value {
			if (char < 'a' || char > 'z') && (char < '0' || char > '9') && char != '_' && char != '-' {
				return nil, fmt.Errorf("sports_types contains invalid value %q", value)
			}
		}
		allowed[value] = struct{}{}
	}
	if len(allowed) == 0 {
		return nil, fmt.Errorf("sports_types must contain at least one value")
	}
	keys := make([]string, 0, len(allowed))
	for value := range allowed {
		keys = append(keys, value)
	}
	sort.Strings(keys)
	return &sportsTypeFilter{key: strings.Join(keys, ","), allowed: allowed}, nil
}

func filterCompactSportsBatch(payload []byte, filter *sportsTypeFilter) ([]byte, error) {
	if filter == nil {
		return payload, nil
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return nil, err
	}
	var events []json.RawMessage
	if err := json.Unmarshal(envelope["events"], &events); err != nil {
		return nil, err
	}
	kept := make([]json.RawMessage, 0, len(events))
	for _, raw := range events {
		var event struct {
			SportsType string   `json:"s"`
			TagIDs     []string `json:"tag_ids"`
		}
		if err := json.Unmarshal(raw, &event); err != nil {
			return nil, err
		}
		if isSportsOrEsports(event.TagIDs) {
			if _, ok := filter.allowed[strings.ToLower(strings.TrimSpace(event.SportsType))]; !ok {
				continue
			}
		}
		kept = append(kept, raw)
	}
	if len(kept) == len(events) {
		return payload, nil
	}
	if len(kept) == 0 {
		return nil, nil
	}
	encoded, err := json.Marshal(kept)
	if err != nil {
		return nil, err
	}
	envelope["events"] = encoded
	return json.Marshal(envelope)
}

func compactEventCount(payload []byte) int {
	if len(payload) == 0 {
		return 0
	}
	var envelope struct {
		Events []json.RawMessage `json:"events"`
	}
	if json.Unmarshal(payload, &envelope) != nil {
		return 0
	}
	return len(envelope.Events)
}

func isSportsOrEsports(tagIDs []string) bool {
	for _, id := range tagIDs {
		if id == "1" || id == "64" {
			return true
		}
	}
	return false
}
