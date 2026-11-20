//go:build integration

package bench

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The subject's `describe` verb answers what the platform needs to know about it; lanes reads that answer and not the
// operator settings (docs/contract.md, "describe").

func TestStartupBoundIsWhatTheSubjectDescribes(t *testing.T) {
	hooked := func(d *Described, set map[string]any) Spec {
		return Spec{Operator: Operator{Hooks: "h", Settings: set}, Described: d}
	}
	for name, tc := range map[string]struct {
		s    Spec
		want time.Duration
	}{
		"described value wins over the settings": {hooked(&Described{MaxStartup: "5m"}, map[string]any{"startup_bound": "2h"}), 5 * time.Minute},
		"described none":                         {hooked(&Described{}, map[string]any{"startup_bound": "2h"}), 0},
		"no hooks":                               {Spec{Described: &Described{MaxStartup: "5m"}}, 0},
	} {
		if got := tc.s.StartupBound(); got != tc.want {
			t.Errorf("%s: StartupBound = %s, want %s", name, got, tc.want)
		}
	}
	s := hooked(&Described{MaxStartup: "45m"}, nil)
	s.ReadyTimeout = "10m"
	if _, rt, _ := s.Durations(); rt != 55*time.Minute {
		t.Errorf("ready timeout %s, want the described start-up plus the margin (55m)", rt)
	}
}

func TestInputRulesComeFromTheSubjectsDescription(t *testing.T) {
	seeded := Spec{Operator: Operator{Hooks: "h", Seed: "/s"}}
	withRule := seeded
	withRule.Described = &Described{Inputs: map[string]InputRule{"seed": {Requires: []string{"baseline"}}}}
	if err := checkInputRules(withRule); err == nil || !strings.Contains(err.Error(), "operator.seed needs operator.baseline") {
		t.Errorf("a described rule must refuse a seed with no baseline, got %v", err)
	}
	withRule.Operator.Baseline = "/b"
	if err := checkInputRules(withRule); err != nil {
		t.Errorf("seed with its baseline: %v", err)
	}
	noRule := seeded
	noRule.Described = &Described{}
	if err := checkInputRules(noRule); err != nil {
		t.Errorf("lanes must not invent a rule the subject did not describe: %v", err)
	}
}

func TestDescribeAsksTheHooksAndFallsBackForAPreContractPin(t *testing.T) {
	f := newFixture(t)
	inv, err := LoadInventory(string(f.root))
	must(t, err)
	st := t.TempDir()
	t.Setenv("HOOK_STATE", st)
	s := Spec{Name: "e", Namespace: "app1", Operator: Operator{Hooks: "test", Revision: "4d2915b3f", Settings: map[string]any{"startup_bound": "1h"}}}

	// A pin that answers: its description is what is used and stored.
	must(t, os.WriteFile(filepath.Join(st, "describe"), []byte(`{"max_startup":"7m","needs_credentials":true,"inputs":{"seed":{"requires":["baseline"]}}}`), 0o644))
	var log bytes.Buffer
	d, err := NewRenderedChecks(&log).describe(context.Background(), inv, s)
	must(t, err)
	if d == nil || d.MaxStartup != "7m" || !d.NeedsCredentials || len(d.Inputs["seed"].Requires) != 1 {
		t.Fatalf("description = %+v", d)
	}

	// A pin without the verb: nil, so the platform uses the built-in defaults, and says so once.
	must(t, os.Remove(filepath.Join(st, "describe")))
	log.Reset()
	c := NewRenderedChecks(&log)
	for i := 0; i < 3; i++ {
		d, err = c.describe(context.Background(), inv, s)
		must(t, err)
		if d != nil {
			t.Fatalf("a pre-contract pin has no description, got %+v", d)
		}
	}
	if n := strings.Count(log.String(), "pre-contract pin"); n != 1 {
		t.Errorf("want the pre-contract note once, got %d in %q", n, log.String())
	}
	if got := s.StartupBound(); got != time.Hour {
		t.Errorf("fallback start-up bound %s, want the settings' 1h", got)
	}
}
