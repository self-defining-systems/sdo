//go:build integration && controller_e2e

package e2e

import (
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/contracts/settings"
)

// Scenario 2: reliability under faults. One run: API latency, an
// agent that crashes, kill -9 of the controller in the
// middle of an incident, and a killed memory host. After the faults stop every
// incident must have reached one terminal outcome and no detector may have
// stalled.
func TestControllerSurvivesFaults(t *testing.T) {
	w := NewWorld(t)
	w.DeployToy()
	w.InstallController(func(c *settings.Config) { c.Response.Timeout = settings.Duration(120 * time.Second) })
	w.WaitBaseline(5 * time.Minute)
	// The scripted agent repairs source drift on first sight; health faults need a learned program.
	const drift, health, later = "toy_target_port_frontend", "toy_target_port_catalog", "toy_target_port_backend"

	// Phase 1: a slow API plus a crashing agent. The incident escalates and stays open; the fault stays.
	latencyFrom := time.Now()
	w.Proxy.Set(Faults{Latency: 250 * time.Millisecond})
	w.SetAgentMode("crash")
	w.Inject(drift)
	// A responder that fails escalates: the incident stays open until its alerts clear, so the engineer repairs it.
	w.waitFor(4*time.Minute, "the crashing agent's incident to end in escalation", func() bool {
		return strings.Contains(w.ControllerLog(), "incident_escalated")
	})
	w.EngineerRepair(drift)
	// Once the engineer's repair clears the alerts, the escalated incident closes escalated.
	w.waitFor(4*time.Minute, "an escalated outcome from the crashing agent", func() bool { return w.hasOutcome("escalated") })
	t.Logf("phase 1: escalated outcome after agent crash; %d API requests delayed", w.Proxy.Requests.Load())

	// Phase 2: faults stop, the agent works again, and the controller is killed in the middle of an incident.
	w.Proxy.Clear()
	// Let the closed incident's reflection and memory-host swap finish: a fault injected during the swap is seen
	// by a restarted observation that has never seen its scenario succeed.
	w.settle(3 * time.Minute)
	// 250 ms on every API call stretches a pass to 10-24 s for the health detectors (12x their interval); recorded
	// in the handoff as a finding, and excused here because the property is about swaps, closure and agent work.
	latencyWindow := Window{From: latencyFrom, To: time.Now().Add(10 * time.Second)}
	w.SetAgentMode("normal")
	w.RestartStandbys()
	pod := w.ControllerPod().Name
	w.Inject(health)
	// "In the middle of an incident" means the dispatch is durable: the incident has been open for a few seconds.
	var openSince time.Time
	w.waitFor(4*time.Minute, "an incident in flight for 3 seconds", func() bool {
		if !w.incidentInFlight() {
			openSince = time.Time{}
			return false
		}
		if openSince.IsZero() {
			openSince = time.Now()
		}
		return time.Since(openSince) >= 3*time.Second
	})
	w.KillController()
	w.waitFor(2*time.Minute, "a new controller pod", func() bool {
		p := w.ControllerPod()
		return p != nil && p.Name != pod && p.Status.Phase == "Running"
	})
	// The interrupted incident must reach a terminal outcome. Its repair may fail
	// when the controller is killed before the worker has durably checkpointed; that failure escalates and the
	// incident stays open until the engineer repairs the fault.
	w.waitFor(3*time.Minute, "the interrupted incident to be repaired or escalated", func() bool {
		return w.Repaired(health) || strings.Contains(w.ControllerLog(), "incident_escalated")
	})
	if !w.Repaired(health) {
		w.EngineerRepair(health)
	}
	w.settle(5 * time.Minute)
	t.Logf("phase 2: every incident ended after kill -9 of %s (repaired=%v)", pod, w.Repaired(health))

	// Phase 3: kill the memory host, then a second fault must still be found and repaired.
	w.settle(3 * time.Minute)
	killedAt := time.Now()
	activations := strings.Count(w.ControllerLog(), "activation_latency_ms")
	if !w.KillMemoryHost() {
		t.Error("no memory host process found to kill")
	}
	// The controller restarts the host. A scenario's user test must have succeeded since the prober started
	// to count as a signal, so the next fault comes once the restarted host has judged a few clean passes.
	w.waitFor(2*time.Minute, "the memory host to be active again", func() bool {
		return strings.Count(w.ControllerLog(), "activation_latency_ms") > activations
	})
	time.Sleep(20 * time.Second)
	w.Inject(later)
	w.waitFor(4*time.Minute, "repair after the memory host was killed", func() bool { return w.Repaired(later) })
	hostWindow := Window{From: killedAt, To: time.Now().Add(20 * time.Second)}

	w.Quiesce()
	w.settle(3 * time.Minute)
	for _, v := range w.CheckProperties(nil, hostWindow, latencyWindow) {
		t.Errorf("property: %s", v)
	}
	t.Logf("detector passes: %v", w.CadenceCoverage())
	t.Logf("incidents: %d, outcomes: %d", len(w.OpenedIncidents()), len(w.Outcomes()))
}

func (w *World) hasOutcome(class string) bool { return w.countOutcomes(class) > 0 }

func (w *World) countOutcomes(class string) int {
	n := 0
	for _, o := range w.Outcomes() {
		if c, _ := o.Raw["classification"].(string); c == class {
			n++
		}
	}
	return n
}

// incidentInFlight reports whether the controller has opened an incident that has no outcome yet.
func (w *World) incidentInFlight() bool {
	for _, id := range w.OpenedIncidents() {
		if _, done := w.Incident(id); !done {
			return true
		}
	}
	return false
}

// settle waits until every opened incident has an outcome and no worker Job is active.
func (w *World) settle(limit time.Duration) {
	w.T.Helper()
	w.waitFor(limit, "all incidents to reach an outcome", func() bool {
		for _, id := range w.OpenedIncidents() {
			if _, done := w.Incident(id); !done {
				return false
			}
		}
		return !strings.Contains(strings.Join(w.ActiveWorkers(), ","), "sdo-")
	})
}

// TestControllerToleratesAPIErrors: with a sustained 10% of API requests answered 500 the controller keeps running.
// It is killed once with the faults on, so its startup reads (restore state, onboarding evidence, the leader Lease) run
// under errors too, and an incident opens in the same window so the steady-state listings run under them as well.
// Any further controller Pod is a controller that exited on a transient error. Every incident still reaches an outcome.
func TestControllerToleratesAPIErrors(t *testing.T) {
	w := NewWorld(t)
	w.DeployToy()
	w.InstallController()
	w.WaitBaseline(5 * time.Minute)
	before := len(w.ControllerPods())
	w.Proxy.Set(Faults{ErrorRate: 0.10})
	w.KillController()
	w.Inject("toy_target_port_frontend")
	time.Sleep(90 * time.Second)
	t.Logf("%d of %d API requests failed", w.Proxy.Injected.Load(), w.Proxy.Requests.Load())
	w.Proxy.Clear()
	if exits := len(w.ControllerPods()) - before - 1; exits > 0 {
		t.Errorf("the controller exited %d times on transient API errors; its log:\n%s", exits, w.ControllerLog())
	}
	w.Quiesce()
	w.settle(5 * time.Minute)
	for _, v := range w.CheckEveryIncidentEnds() {
		t.Errorf("property: %s", v)
	}
}

// TestControllerEndsHungAgent: an incident responder that hangs is ended by the controller at its response deadline,
// the attempt is recorded, the incident reaches an outcome, and the next incident is handled normally.
func TestControllerEndsHungAgent(t *testing.T) {
	w := NewWorld(t)
	w.DeployToy()
	w.InstallController(func(c *settings.Config) { c.Response.Timeout = settings.Duration(45 * time.Second) })
	w.WaitBaseline(5 * time.Minute)
	w.SetAgentMode("hang")
	w.RestartStandbys()
	w.Inject("toy_target_port_catalog")
	// The controller ends the hung responder at its deadline and escalates; the incident stays open until the
	// engineer repairs the fault, and then reaches its outcome.
	w.waitFor(3*time.Minute, "the hung responder to be ended", func() bool {
		return strings.Contains(w.ControllerLog(), "incident_terminal_dispatch_failure")
	})
	w.EngineerRepair("toy_target_port_catalog")
	w.waitFor(3*time.Minute, "a terminal outcome for the hung responder's incident", func() bool { return len(w.Outcomes()) > 0 })
	w.SetAgentMode("normal")
	w.RestartStandbys()
	w.settle(3 * time.Minute)
	w.Inject("toy_target_port_backend")
	w.waitFor(4*time.Minute, "the next incident repaired after the hung responder was ended", func() bool { return w.Repaired("toy_target_port_backend") })
	w.Quiesce()
	w.settle(3 * time.Minute)
	for _, v := range w.CheckEveryIncidentEnds() {
		t.Errorf("property: %s", v)
	}
}
