package bench

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// churnOps reports every step it is asked about as running and records what the manager starts.
type churnOps struct {
	HostOps
	started *[]Step
}

func (c churnOps) Probe(_ context.Context, steps map[string]string, _ []string) (Probe, error) {
	p := Probe{Steps: map[string]string{}}
	for l := range steps {
		p.Steps[l] = StepRunning
	}
	return p, nil
}

func (c churnOps) Start(_ context.Context, s Step) error {
	*c.started = append(*c.started, s)
	return nil
}

func churnManager(t *testing.T, maxBuilds int, started *[]Step) *Manager {
	t.Helper()
	root := Root(t.TempDir())
	inv := "hosts:\n  h1: {local: true, staging: /tmp/s, max_builds: " + string(rune('0'+maxBuilds)) + ", max_load5: 100, kubeconfigs: /tmp/k, evidence: /tmp/e, clusters: [c1, c2, c3, c4]}\n" +
		"checkouts:\n  fuzzops: {abc: {h1: /tmp/fz}}\n"
	if err := os.WriteFile(filepath.Join(string(root), "hosts.yaml"), []byte(inv), 0o644); err != nil {
		t.Fatal(err)
	}
	return &Manager{Root: root, Ops: func(string, Host) HostOps { return churnOps{started: started} }}
}

func writeLane(t *testing.T, m *Manager, lf LaneFile) {
	t.Helper()
	if err := writeJSON(m.Root.LanePath(lf.Lane), lf); err != nil {
		t.Fatal(err)
	}
}

func releases(started []Step) int {
	n := 0
	for _, s := range started {
		if s.Kind == StepRelease {
			n++
		}
	}
	return n
}

// max_builds is the host's budget for cluster churn, not only for builds: a release (which resets the lane to its
// golden copy or deletes its cluster) waits while the host already runs max_builds churn steps of any kind.
func TestChurnBudgetCoversReleasesAndResets(t *testing.T) {
	var started []Step
	m := churnManager(t, 1, &started)
	prepared := &Prepared{Experiment: "x", Hooks: "hk", WorkloadRev: "abc"}
	writeLane(t, m, LaneFile{Lane: "c1", Host: "h1", State: LaneBuilding, Step: &StepRef{ID: "build-1", Kind: StepBuild}})
	writeLane(t, m, LaneFile{Lane: "c2", Host: "h1", State: LaneFree, Prepared: prepared})
	writeLane(t, m, LaneFile{Lane: "c3", Host: "h1", State: LaneFree, Prepared: prepared})
	if err := m.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := releases(started); n != 0 {
		t.Fatalf("a build already holds the host's only churn slot, yet %d release(s) started", n)
	}
	// the build ends: exactly one release may take the slot, the other waits for it
	writeLane(t, m, LaneFile{Lane: "c1", Host: "h1", State: LaneFree})
	if err := m.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := releases(started); n != 1 {
		t.Fatalf("with one free slot exactly one release starts, got %d", n)
	}
	if st := m.hosts["h1"]; st.Builds != 1 {
		t.Fatalf("status counts every churn step, got %d", st.Builds)
	}
}

func TestChurnBudgetOfTwoAllowsTwo(t *testing.T) {
	var started []Step
	m := churnManager(t, 2, &started)
	prepared := &Prepared{Experiment: "x", Hooks: "hk", WorkloadRev: "abc"}
	for _, l := range []string{"c1", "c2", "c3"} {
		writeLane(t, m, LaneFile{Lane: l, Host: "h1", State: LaneFree, Prepared: prepared})
	}
	if err := m.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := releases(started); n != 2 {
		t.Fatalf("max_builds 2: two releases at once, got %d", n)
	}
}
