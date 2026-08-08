package syncer

import (
	"testing"

	"github.com/polymas/poly_uma/internal/store"
)

func TestShouldNotifyTickSizeTransition(t *testing.T) {
	tick001, tick01 := 0.001, 0.01
	market := func(tick *float64) *store.MarketSnapshot { return &store.MarketSnapshot{OrderPriceMinTickSize: tick} }
	for name, tc := range map[string]struct {
		previous *store.MarketSnapshot
		current  *store.MarketSnapshot
		want     bool
	}{
		"transition":       {market(&tick001), market(&tick01), true},
		"startup":          {nil, market(&tick01), false},
		"missing previous": {market(nil), market(&tick01), false},
		"unchanged":        {market(&tick01), market(&tick01), false},
		"other target":     {market(&tick01), market(&tick001), false},
	} {
		t.Run(name, func(t *testing.T) {
			if got := shouldNotifyTickSizeTransition(tc.previous, tc.current); got != tc.want {
				t.Fatalf("got %t, want %t", got, tc.want)
			}
		})
	}
}
