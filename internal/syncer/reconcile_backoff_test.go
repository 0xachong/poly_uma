package syncer

import (
	"encoding/base64"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/polymas/poly_uma/internal/uma"
)

func TestCursorLastNumericMarketID(t *testing.T) {
	payload := append(make([]byte, 32), []byte(`{"v":1,"k":"markets","keys":[{"t":"string","v":"3222332"}]}`)...)
	cursor := base64.RawURLEncoding.EncodeToString(payload)
	got, err := cursorLastNumericMarketID(cursor)
	if err != nil || got != 3222332 {
		t.Fatalf("got=%d err=%v", got, err)
	}
	if _, err := cursorLastNumericMarketID("not-a-cursor"); err == nil {
		t.Fatal("malformed cursor should fail")
	}
}

func TestGammaCursorRejected(t *testing.T) {
	if !gammaCursorRejected(&uma.GammaHTTPError{StatusCode: http.StatusForbidden}) {
		t.Fatal("403 should reject the persisted cursor")
	}
	if !gammaCursorRejected(&uma.GammaHTTPError{StatusCode: http.StatusBadRequest}) {
		t.Fatal("400 should reject the persisted cursor")
	}
	if gammaCursorRejected(&uma.GammaHTTPError{StatusCode: http.StatusTooManyRequests}) || gammaCursorRejected(errors.New("timeout")) {
		t.Fatal("transient failures must retain the cursor")
	}
}

func TestReconcileFailureBackoff(t *testing.T) {
	wants := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 32 * time.Second, time.Minute, time.Minute}
	for i, want := range wants {
		if got := reconcileFailureBackoff(i + 1); got != want {
			t.Fatalf("failure %d: got %s want %s", i+1, got, want)
		}
	}
}
