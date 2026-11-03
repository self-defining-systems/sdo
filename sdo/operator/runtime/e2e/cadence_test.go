//go:build integration && controller_e2e

package e2e

import (
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/runtime/sim"
)

// The properties are sim's; what can go wrong here is the mapping from process evidence to the log. Each
// case feeds hand-built evidence with one defect and requires sim.Check to name it, so a check that cannot
// fail (a mapping that drops the defect) is caught.
func TestEvidenceBecomesALogThatTheSharedChecksJudge(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	at := func(seconds int) time.Time { return base.Add(time.Duration(seconds) * time.Second) }
	specs := []DetectorSpec{{ID: "a", Interval: 2 * time.Second, AddedAt: at(-3600)}, {ID: "b", Interval: 2 * time.Second, AddedAt: at(-3600)}}
	pass := func(pod string, seconds int, ids ...string) Iteration {
		return Iteration{Pod: pod, At: at(seconds), Detectors: ids}
	}
	steady := func(pod string, from, to int, skip func(second int, id string) bool) []Iteration {
		var out []Iteration
		for s := from; s <= to; s += 2 {
			var ids []string
			for _, id := range []string{"a", "b"} {
				if skip == nil || !skip(s, id) {
					ids = append(ids, id)
				}
			}
			out = append(out, pass(pod, s, ids...))
		}
		return out
	}
	input := func(passes []Iteration) logInput {
		return logInput{Passes: passes, Pods: []string{"p1"}, Running: map[string]bool{"p1": true}, Killed: map[string]bool{}, Specs: specs,
			Quiesced: at(60), End: at(120)}
	}
	violations := func(in logInput, hung map[string]bool) []string {
		cfg := simConfig
		cfg.Hung = hung
		var out []string
		for _, err := range sim.Check(simLog(in), cfg) {
			out = append(out, err.Error())
		}
		return out
	}
	expect := func(name string, got []string, want string) {
		t.Helper()
		if want == "" && len(got) != 0 || want != "" && (len(got) != 1 || !strings.Contains(got[0], want)) {
			t.Fatalf("%s: got %v, want %q", name, got, want)
		}
	}

	healthy := input(steady("p1", 0, 120, nil))
	expect("healthy run", violations(healthy, nil), "")

	stalled := input(steady("p1", 0, 120, func(s int, id string) bool { return id == "b" && s > 20 }))
	expect("a detector that stopped", violations(stalled, nil), `detector "b" was not evaluated`)
	expect("an exempt detector", violations(stalled, map[string]bool{"b": true}), "")

	late := input(steady("p1", 0, 120, func(s int, id string) bool { return id == "a" && s >= 40 && s < 80 }))
	expect("a long gap", violations(late, nil), `detector "a" was not evaluated`)
	late.Windows = []Window{{From: at(30), To: at(90)}}
	expect("a declared fault window excuses its gap", violations(late, nil), "")

	added := healthy
	added.Specs = append(append([]DetectorSpec(nil), specs...), DetectorSpec{ID: "new", Interval: 2 * time.Second, AddedAt: at(30)})
	expect("a detector that never ran after its activation commit", violations(added, nil), `detector "new" was not evaluated`)

	restarted := input(append(steady("p1", 0, 40, nil), steady("p2", 60, 120, nil)...))
	restarted.Pods, restarted.Running = []string{"p1", "p2"}, map[string]bool{"p2": true}
	restarted.Killed = map[string]bool{"p1": true}
	expect("a killed controller that came back", violations(restarted, nil), "")
	restarted.Killed = map[string]bool{}
	expect("a controller that exited on its own", violations(restarted, nil), "exited on its own")

	wedged := healthy
	wedged.Opened = []openedAt{{"inc-1", at(10)}}
	expect("an incident with no outcome", violations(wedged, nil), `incident "inc-1" opened`)
	wedged.Outcomes = map[string]string{"inc-1": "success"}
	expect("an incident with an outcome", violations(wedged, nil), "")
	blocked := wedged
	blocked.Opened = []openedAt{{"inc-1", at(10)}, {"inc-2", at(20)}}
	blocked.Outcomes = map[string]string{"inc-1": "failed"}
	expect("a failed incident blocks the next", violations(blocked, nil), "blocked behind failed incident")
}
