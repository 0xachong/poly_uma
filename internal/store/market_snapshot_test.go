package store

import "testing"

func TestProcessingKey(t *testing.T) {
	tests := []struct {
		row  EventRow
		want string
	}{
		{EventRow{ConditionID: " 0xAbC ", EventType: "Propose"}, "0xabc:propose"},
		{EventRow{ConditionID: "0xAbC", EventType: "dispute"}, "0xabc:dispute"},
		{EventRow{ConditionID: "0xAbC", EventType: "resolved"}, ""},
		{EventRow{EventType: "propose"}, ""},
	}
	for _, tt := range tests {
		if got := tt.row.ProcessingKey(); got != tt.want {
			t.Fatalf("ProcessingKey()=%q want=%q row=%+v", got, tt.want, tt.row)
		}
	}
}
