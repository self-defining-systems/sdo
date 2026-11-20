package bench

import (
	"testing"
	"time"
)

// A hooked subject's start-up period (its startup_bound, SDO's default 30m) reports busy until it ends, so a
// build check never waits less than the bound plus a margin, whatever ready_timeout says.
func TestReadyTimeoutCoversTheSubjectStartupBound(t *testing.T) {
	cases := []struct {
		name string
		op   Operator
		want time.Duration
	}{
		{"default bound", Operator{Hooks: "test"}, 40 * time.Minute},
		{"longer bound", Operator{Hooks: "test", Settings: map[string]any{"startup_bound": "45m"}}, 55 * time.Minute},
		{"short bound", Operator{Hooks: "test", Settings: map[string]any{"startup_bound": "1m"}}, 20 * time.Minute},
		{"start-up period off", Operator{Hooks: "test", Settings: map[string]any{"startup_bound": "0s"}}, 20 * time.Minute},
		{"no hooks", Operator{Builtin: "noop"}, 20 * time.Minute},
	}
	for _, c := range cases {
		s := Spec{ReadyFor: "2m", ReadyTimeout: "20m", Cap: "25m", Operator: c.op}
		if _, rt, _ := s.Durations(); rt != c.want {
			t.Errorf("%s: ready timeout %v, want %v", c.name, rt, c.want)
		}
	}
}
