//go:build integration

package bench

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/self-defining-systems/sdo/benchmarks/lanes/internal/plan"
)

// submitStub submits experiments as bare records (no lanes) on top of the real PlanBackend, so a plan can be driven
// through its real export and verify of artifacts.
type submitStub struct {
	*PlanBackend
	l *planLab
}

func (s submitStub) Submit(_ context.Context, j plan.Job) error {
	if fileExists(s.Root.LockPath(j.Exp)) {
		return plan.ErrExists
	}
	s.l.experiment(j.Exp, nil)
	return nil
}

// An artifact edge (p1 -> p2) whose artifact fails verify: p2 never starts on it, p1's attempt fails with the reason,
// and on the second identical failure the item parks with that reason.
func TestBadArtifactParksTheItemAndNeverStartsTheDependent(t *testing.T) {
	l := newPlanLab(t)
	dir := t.TempDir()
	for name, body := range map[string]string{
		"p1.json": `{"name": "x", "app": "examples/a"}`,
		"p2.json": `{"name": "x", "app": "examples/a", "operator": {"seed": "${artifact.memory}"}}`,
		"plan.yaml": "plan: tp\npins: {workload: fuzzops@4652656, subject: sdo@9b2cadb7f}\narms:\n  - {name: p1, spec: p1.json}\n  - {name: p2, spec: p2.json}\n" +
			"items: [a]\nparallel: 2\nartifacts: {memory: {path: \"${run}/operator/learned-baseline\", verify: exists}}\nedges: [{from: p1, to: p2, artifact: memory}]\n",
	} {
		must(t, os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644))
	}
	clock := SystemClock{}
	if _, err := plan.Add(l.root.PlansDir(), filepath.Join(dir, "plan.yaml"), clock, nil); err != nil {
		t.Fatal(err)
	}
	e, err := plan.Open(plan.Dir(l.root.PlansDir(), "tp"), submitStub{l.b, l}, clock)
	must(t, err)
	tick := func() { must(t, e.Tick(context.Background())) }
	st := func() *plan.State {
		s, err := plan.LoadState(plan.Dir(l.root.PlansDir(), "tp"))
		must(t, err)
		return s
	}
	// Each attempt of p1 ends done, but the artifact it names is not there.
	for attempt := 0; attempt < 2; attempt++ {
		tick()
		exp := ""
		for _, n := range st().Nodes {
			if n.ID == "p1/a/r1" {
				exp = n.Exp
			}
		}
		if exp == "" {
			t.Fatalf("attempt %d: p1 was not released: %+v", attempt, st().Nodes)
		}
		l.experiment(exp, map[string]RunFile{"r1": {Status: StatusDone, Ended: time.Now()}})
		tick()
	}
	s := st()
	for _, n := range s.Nodes {
		if n.ID == "p2/a/r1" && n.Exp != "" {
			t.Fatalf("p2 started on a bad artifact: %+v", n)
		}
	}
	if s.Parked["a"] != "artifact:memory" {
		t.Fatalf("the item is not parked with the reason: %v", s.Parked)
	}
}
