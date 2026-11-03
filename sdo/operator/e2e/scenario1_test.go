//go:build integration && controller_e2e

package e2e

import (
	"testing"
	"time"
)

// Scenario 1: a first incident is repaired by a scripted agent, reflection
// saves memory, the memory host activates it with no controller restart, and
// the same fault repeated is cleared by the deterministic stage with no agent.
func TestFirstIncidentLearnsThenRepeatIsDeterministic(t *testing.T) {
	w := NewWorld(t)
	w.DeployToy()
	w.InstallController()
	w.WaitBaseline(5 * time.Minute)
	pod := w.ControllerPod().Name
	const problem = "toy_target_port_frontend"

	first := time.Now()
	w.Inject(problem)
	w.waitFor(3*time.Minute, "first repair", func() bool { return w.Repaired(problem) })
	t.Logf("first incident repaired in %s", time.Since(first).Round(time.Second))
	w.waitFor(3*time.Minute, "first outcome", func() bool { return len(w.Outcomes()) >= 1 })
	incidents := w.OpenedIncidents()
	t.Logf("incidents %v, accepted log %v", incidents, w.AcceptedLog())
	w.waitFor(3*time.Minute, "reflection activation", func() bool { return len(w.Activations()) >= 2 })
	t.Logf("activations %+v", w.Activations())

	ids := w.OpenedIncidents()
	if len(ids) != 1 {
		t.Fatalf("one fault must open one incident, got %v", ids)
	}
	if o, _ := w.Incident(ids[0]); o.Raw["resolution_path"] != "responder" || o.Raw["classification"] != "success" {
		t.Errorf("first incident: want a successful responder repair, got path=%v class=%v", o.Raw["resolution_path"], o.Raw["classification"])
	}
	var revision string
	for _, a := range w.Activations() {
		if a.Revision != "" {
			revision = a.Revision
			if a.Pod != pod {
				t.Errorf("activation %s happened in controller pod %s, not %s: a restart is not an activation", a.Revision, a.Pod, pod)
			}
		}
	}
	if revision == "" {
		t.Fatal("reflection saved no memory the host activated")
	}
	t.Logf("reflection activated revision %s with the controller still running", revision[:8])

	second := time.Now()
	w.Inject(problem)
	w.waitFor(3*time.Minute, "second repair", func() bool { return w.Repaired(problem) })
	t.Logf("repeat repaired in %s", time.Since(second).Round(time.Second))
	w.waitFor(2*time.Minute, "second outcome", func() bool { return len(w.Outcomes()) >= 2 })
	ids = w.OpenedIncidents()
	if len(ids) != 2 {
		t.Fatalf("two faults must open two incidents, got %v", ids)
	}
	if o, _ := w.Incident(ids[1]); o.Raw["resolution_path"] != "deterministic" {
		t.Errorf("repeat must be cleared by the deterministic stage, got path=%v", o.Raw["resolution_path"])
	}
	if jobs := w.ResponderJobs(ids[1]); len(jobs) != 0 {
		t.Errorf("repeat must not start an agent, got Jobs %v", jobs)
	}
	if now := w.ControllerPod().Name; now != pod {
		t.Errorf("controller restarted: %s -> %s", pod, now)
	}
	w.settle(2 * time.Minute)
	for _, v := range w.CheckNoWedge() {
		t.Errorf("wedge: %s", v)
	}
	for _, v := range w.CheckCadence(nil) {
		t.Errorf("cadence: %s", v)
	}
	coverage := w.CadenceCoverage()
	t.Logf("detector passes: %v", coverage)
	for _, d := range w.Detectors() {
		if coverage[d.ID] < 5 {
			t.Errorf("detector %s ran %d times: the cadence check saw too little to mean anything", d.ID, coverage[d.ID])
		}
	}
}
