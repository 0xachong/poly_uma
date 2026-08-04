package uma

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestJSONFloat64SliceAcceptsGammaRepresentations(t *testing.T) {
	for _, input := range []string{
		`["0.48","0.52"]`,
		`[0.48,0.52]`,
		`"[\"0.48\",\"0.52\"]"`,
	} {
		var got jsonFloat64Slice
		if err := json.Unmarshal([]byte(input), &got); err != nil {
			t.Fatalf("input=%s err=%v", input, err)
		}
		if !reflect.DeepEqual([]float64(got), []float64{0.48, 0.52}) {
			t.Fatalf("input=%s got=%v", input, got)
		}
	}
}
