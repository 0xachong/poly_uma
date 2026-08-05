package store

import (
	"encoding/json"
	"testing"
	"unsafe"
)

func TestConditionIDRoundTripCanonicalizesBoundaryString(t *testing.T) {
	input := "0xABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789"
	id, err := ParseConditionID(input)
	if err != nil {
		t.Fatal(err)
	}
	want := "0xabcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	if got := id.String(); got != want {
		t.Fatalf("String()=%q want=%q", got, want)
	}
}

func TestConditionIDJSONContractRemainsHexString(t *testing.T) {
	id := MustParseConditionID("0x" + "0101010101010101010101010101010101010101010101010101010101010101")
	encoded, err := json.Marshal(struct {
		ConditionID ConditionID `json:"condition_id"`
	}{ConditionID: id})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"condition_id":"0x0101010101010101010101010101010101010101010101010101010101010101"}`
	if string(encoded) != want {
		t.Fatalf("json=%s want=%s", encoded, want)
	}
}

func TestMarketSnapshotConditionIDPersistenceContract(t *testing.T) {
	const persisted = `{"market_id":"42","condition_id":"0xabcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789","question":"Q","active":true,"closed":false,"accepting_orders":true,"enable_order_book":true,"catalog_synced_at_us":7}`
	var snapshot MarketSnapshot
	if err := json.Unmarshal([]byte(persisted), &snapshot); err != nil {
		t.Fatal(err)
	}
	if got := snapshot.ConditionID.String(); got != "0xabcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789" {
		t.Fatalf("condition_id=%q", got)
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]any
	if err := json.Unmarshal(encoded, &wire); err != nil {
		t.Fatal(err)
	}
	if wire["condition_id"] != snapshot.ConditionID.String() {
		t.Fatalf("wire condition_id=%v", wire["condition_id"])
	}
}

func TestConditionIDUsesExactly32Bytes(t *testing.T) {
	if got := unsafe.Sizeof(ConditionID{}); got != 32 {
		t.Fatalf("ConditionID size=%d", got)
	}
}

func TestConditionIDRejectsMalformedValues(t *testing.T) {
	for _, value := range []string{"", "0xabc", "condition", "0xzzzzzz0123456789abcdef0123456789abcdef0123456789abcdef0123456789"} {
		if _, err := ParseConditionID(value); err == nil {
			t.Fatalf("ParseConditionID(%q) succeeded", value)
		}
	}
}
