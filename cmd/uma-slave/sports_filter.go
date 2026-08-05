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
	values := strings.Split(raw, ",")
	allowed := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(value))
		if value == "" {
			continue
		}
		if len(value) > 64 {
			return nil, fmt.Errorf("sports_types value is too long")
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
		return nil, fmt.Errorf("decode compact envelope: %w", err)
	}
	var events []json.RawMessage
	if err := json.Unmarshal(envelope["events"], &events); err != nil {
		return nil, fmt.Errorf("decode compact events: %w", err)
	}
	kept := make([]json.RawMessage, 0, len(events))
	changed := false
	for _, raw := range events {
		var event struct {
			SportsType string   `json:"s"`
			TagIDs     []string `json:"tag_ids"`
		}
		if err := json.Unmarshal(raw, &event); err != nil {
			return nil, fmt.Errorf("decode compact event: %w", err)
		}
		if isSportsOrEsports(event.TagIDs) {
			if _, ok := filter.allowed[strings.ToLower(strings.TrimSpace(event.SportsType))]; !ok {
				changed = true
				continue
			}
		}
		kept = append(kept, raw)
	}
	if !changed {
		return payload, nil
	}
	if len(kept) == 0 {
		return nil, nil
	}
	encodedEvents, err := json.Marshal(kept)
	if err != nil {
		return nil, fmt.Errorf("encode compact events: %w", err)
	}
	envelope["events"] = encodedEvents
	encoded, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("encode compact envelope: %w", err)
	}
	return encoded, nil
}

func isSportsOrEsports(tagIDs []string) bool {
	for _, id := range tagIDs {
		if strings.TrimSpace(id) == "1" || strings.TrimSpace(id) == "64" {
			return true
		}
	}
	return false
}
