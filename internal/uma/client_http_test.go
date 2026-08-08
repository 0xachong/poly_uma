package uma

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGammaJSONPreservesHTTPStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer server.Close()
	var payload map[string]any
	err := gammaJSON(context.Background(), "", server.URL, &payload)
	status, ok := GammaHTTPStatus(err)
	if !ok || status != http.StatusForbidden || err.Error() != "gamma HTTP 403" {
		t.Fatalf("status=%d ok=%t err=%v", status, ok, err)
	}
}
