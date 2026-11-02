//go:build integration

package replay

import (
	"context"
	"fmt"
	"testing"
	"time"

	"sdo.dev/operator/sdk/program"
)

func TestFiftyCaseTiming(t *testing.T) {
	p := fixtureProgram{mode: "field"}
	registry, _ := program.NewRegistry([]program.Diagnoser{p}, []program.Mitigator{p})
	cases := make([]Case, 50)
	for i := range cases {
		cases[i] = fixture("field")
		cases[i].ID = fmt.Sprintf("synthetic-field-%02d", i)
	}
	start := time.Now()
	rows := Matrix(context.Background(), registry, nil, cases)
	elapsed := time.Since(start)
	if len(rows) != 50 {
		t.Fatalf("got %d checks", len(rows))
	}
	for _, r := range rows {
		if r.Status != "replayed" {
			t.Fatalf("%+v", r)
		}
	}
	t.Logf("50 synthetic in-memory cases in %s (%.3f ms/case after build)", elapsed, float64(elapsed)/float64(time.Millisecond)/50)
}

