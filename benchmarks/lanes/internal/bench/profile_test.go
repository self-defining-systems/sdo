package bench

import "testing"

// A subject that carries no baseline memory (a plain agent with no operator state) can still hold a warm pool and be
// claimed: two experiments match when both have no baseline, and never when only one has one.
func TestSameOperatorProfileBaselines(t *testing.T) {
	exp := func(baseline *Input) *Experiment {
		s := Spec{App: "examples/gitlab", Fuzzops: "86e1c7c", Namespace: "fg1",
			Operator: Operator{Hooks: "plain", Settings: map[string]any{"model": "m"}}}
		return &Experiment{Spec: &s, Lock: Lock{Baseline: baseline}}
	}
	a, b := &Input{SHA256: "aa"}, &Input{SHA256: "bb"}
	cases := []struct {
		name string
		x, y *Input
		want bool
	}{
		{"both without a baseline", nil, nil, true},
		{"same baseline", a, &Input{SHA256: "aa"}, true},
		{"different baselines", a, b, false},
		{"only the pool has one", a, nil, false},
		{"only the claimant has one", nil, a, false},
	}
	for _, c := range cases {
		if got := sameOperatorProfile(exp(c.x), exp(c.y)); got != c.want {
			t.Errorf("%s: sameOperatorProfile = %v, want %v", c.name, got, c.want)
		}
	}
}
