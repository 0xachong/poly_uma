package notify

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestTickSizeAlerterSendsAsynchronously(t *testing.T) {
	received := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	alerter := NewTickSizeAlerter(server.URL)
	defer alerter.Close()
	alerter.Send(TickSizeAlert{MarketID: "12", ConditionID: "0xabc", Previous: 0.001, Current: 0.01})
	select {
	case body := <-received:
		if !strings.Contains(body, "0.01") || !strings.Contains(body, "market_id") {
			t.Fatalf("unexpected body: %s", body)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for asynchronous alert")
	}
}
