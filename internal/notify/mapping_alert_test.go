package notify

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestMappingAlerterSendsFirstEnrichmentFailure(t *testing.T) {
	var requests atomic.Int64
	received := make(chan struct{}, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusOK)
		received <- struct{}{}
	}))
	defer server.Close()

	alerter := NewMappingAlerter(server.URL)
	defer alerter.Close()
	alert := MappingAlert{Kind: "condition_mapping_miss", Severity: "high", ConditionID: "0xabc", EventType: "propose"}
	alerter.Send(alert)
	// An identical failure within the cooldown must not flood Feishu.
	alerter.Send(alert)

	select {
	case <-received:
	case <-time.After(2 * time.Second):
		t.Fatal("first enrichment failure did not produce an alert")
	}
	time.Sleep(50 * time.Millisecond)
	if got := requests.Load(); got != 1 {
		t.Fatalf("requests=%d want=1", got)
	}
}
