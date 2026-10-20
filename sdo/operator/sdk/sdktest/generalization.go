package sdktest

import (
	"context"
	"testing"
	"time"

	"sdo.dev/operator/sdk/program"
)

func CheckGeneralization(t testing.TB, p program.Diagnoser, c CorpusCase, peer program.Bindings) {
	t.Helper()
	for _, tc := range []struct {
		name     string
		snapshot Snapshot
		bindings program.Bindings
		want     program.Result
	}{
		{"open", c.Open, c.Bindings, program.Match},
		{"fixed", c.Fixed, c.Bindings, program.NoMatch},
		{"healthy", c.Healthy, peer, program.NoMatch},
	} {
		verdict := program.RunDiagnose(context.Background(), p, &ReplayReadView{Snapshot: tc.snapshot, Calls: c.Recordings}, tc.bindings, 10*time.Second)
		if verdict.Result != tc.want {
			t.Errorf("%s: got %+v, want %s", tc.name, verdict, tc.want)
		}
	}
}

func CheckMitigatorGeneralization(t testing.TB, p program.Mitigator, c CorpusCase) {
	t.Helper()
	for _, tc := range []struct {
		name        string
		snapshot    Snapshot
		wantActions bool
	}{
		{"open", c.Open, true}, {"fixed", c.Fixed, false},
	} {
		if err := program.ValidateBindings(p.Spec(), c.Bindings); err != nil {
			t.Fatal(err)
		}
		plan, err := p.Mitigate(context.Background(), &ReplayReadView{Snapshot: tc.snapshot, Calls: c.Recordings}, c.Bindings)
		if err != nil {
			t.Errorf("%s: %v", tc.name, err)
			continue
		}
		if err := plan.Validate(); err != nil {
			t.Errorf("%s plan: %v", tc.name, err)
		}
		if (len(plan.Actions) > 0) != tc.wantActions {
			t.Errorf("%s: got %d actions", tc.name, len(plan.Actions))
		}
	}
}
