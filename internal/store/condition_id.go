package store

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

// ConditionID is the compact in-memory representation of a Polymarket
// condition identifier. Persistence and wire contracts continue to use the
// canonical lower-case 0x-prefixed string.
type ConditionID [32]byte

func ParseConditionID(value string) (ConditionID, error) {
	var id ConditionID
	value = strings.TrimSpace(value)
	if len(value) == 66 && (value[:2] == "0x" || value[:2] == "0X") {
		value = value[2:]
	}
	if len(value) != hex.EncodedLen(len(id)) {
		return id, fmt.Errorf("condition_id must contain 32 bytes")
	}
	if _, err := hex.Decode(id[:], []byte(value)); err != nil {
		return ConditionID{}, fmt.Errorf("decode condition_id: %w", err)
	}
	return id, nil
}

// MustParseConditionID is intended for trusted constants and test fixtures.
func MustParseConditionID(value string) ConditionID {
	id, err := ParseConditionID(value)
	if err != nil {
		panic(err)
	}
	return id
}

func (id ConditionID) IsZero() bool { return id == ConditionID{} }

func (id ConditionID) String() string {
	if id.IsZero() {
		return ""
	}
	encoded := make([]byte, 2+hex.EncodedLen(len(id)))
	encoded[0], encoded[1] = '0', 'x'
	hex.Encode(encoded[2:], id[:])
	return string(encoded)
}

func (id ConditionID) MarshalJSON() ([]byte, error) { return json.Marshal(id.String()) }

func (id *ConditionID) UnmarshalJSON(data []byte) error {
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	if value == "" {
		*id = ConditionID{}
		return nil
	}
	parsed, err := ParseConditionID(value)
	if err != nil {
		return err
	}
	*id = parsed
	return nil
}
