package runtime

import (
	"context"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

// worldDetector reports whatever the simulated cluster currently shows, so an
// extra gate re-evaluation sees the same state a scheduled one would.
type worldDetector struct {
	spec  sdk.DetectorSpec
	world func() []sdk.Finding
	calls int
}

func (d *worldDetector) Spec() sdk.DetectorSpec { return d.spec }

func (d *worldDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	d.calls++
	findings := make([]sdk.Finding, 0)
	for _, finding := range d.world() {
		finding.DetectorID = d.spec.ID
		finding.Severity = sdk.SeverityCritical
		findings = append(findings, finding)
	}
	return findings, nil
}

var gateWatch = sdk.WatchKind{APIVersion: "v1", Kind: "Service"}

// gateHealthDetector mirrors the seed's health-objective detector: a 30 s
// interval, watch-driven, with two-evaluation firing and clearing.
func gateHealthDetector(world func() []sdk.Finding) *worldDetector {
	return &worldDetector{world: world, spec: sdk.DetectorSpec{
		ID: "health-objective", Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge,
		Interval: 30 * time.Second, Watches: []sdk.WatchKind{gateWatch},
		Persistence:       sdk.PersistencePolicy{Firing: 2, Clearing: 2},
		Batching:          sdk.BatchingPolicy{Severity: sdk.SeverityCritical},
		OriginatingCommit: "health-objective",
	}}
}

func gateConfig() ControllerConfig {
	config := testControllerConfig()
	config.ConfirmationInterval = time.Second
	config.GateConfirmation = GateConfirmationPolicy{Evaluations: 3, Window: 2 * time.Second, Interval: time.Second}
	return config
}

type gateHarness struct {
	t          *testing.T
	controller *Controller
	dispatcher *blockingDispatcher
	detector   *worldDetector
	request    IncidentRequest
	now        time.Time
}

// openGateIncident fires the detector on two watch events and dispatches the
// responder, which stays running until released.
func openGateIncident(t *testing.T, world func() []sdk.Finding) *gateHarness {
	t.Helper()
	return openGateIncidentWith(t, gateConfig(), world)
}

func openGateIncidentWith(t *testing.T, config ControllerConfig, world func() []sdk.Finding) *gateHarness {
	t.Helper()
	detector := gateHealthDetector(world)
	dispatcher := &blockingDispatcher{requests: make(chan IncidentRequest, 1), release: make(chan struct{})}
	controller, err := NewController(config, []sdk.Detector{detector},
		staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "demo"}}, dispatcher, time.Unix(0, 0))
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	h := &gateHarness{t: t, controller: controller, dispatcher: dispatcher, detector: detector, now: time.Unix(0, 0)}
	h.event(0)
	h.event(500 * time.Millisecond)
	executePendingEffect(t, controller)
	h.request = awaitRequest(t, dispatcher.requests)
	t.Cleanup(func() {
		select {
		case <-dispatcher.release:
		default:
			close(dispatcher.release)
		}
	})
	return h
}

// event steps the controller on a watch event after delay.
func (h *gateHarness) event(delay time.Duration) {
	h.t.Helper()
	h.now = h.now.Add(delay)
	event := gateWatch
	if err := h.controller.Step(context.Background(), h.now, &event); err != nil {
		h.t.Fatalf("event step at %s: %v", h.now, err)
	}
}

// advance runs the controller loop, waking at NextWake, until until.
func (h *gateHarness) advance(until time.Time, each func()) {
	h.t.Helper()
	for {
		next := h.controller.NextWake()
		if next.IsZero() || next.After(until) {
			h.now = until
			return
		}
		if next.Before(h.now) {
			next = h.now
		}
		h.now = next
		if err := h.controller.Step(context.Background(), h.now, nil); err != nil {
			h.t.Fatalf("step at %s: %v", h.now, err)
		}
		if each != nil {
			each()
		}
	}
}

func (h *gateHarness) view() *IncidentView {
	h.t.Helper()
	view := cloneIncidentView(h.controller.incidentView)
	if view == nil || view.IncidentID != h.request.IncidentID {
		h.t.Fatalf("the open incident must publish its view: %#v", view)
	}
	return view
}

func findingNames(findings []sdk.Finding) []string {
	names := make([]string, 0, len(findings))
	for _, finding := range findings {
		names = append(names, finding.PrimaryResource.Name)
	}
	return names
}

func TestGateConfirmsACorrectFixWithinSeconds(t *testing.T) {
	broken := true
	h := openGateIncident(t, func() []sdk.Finding {
		if broken {
			return []sdk.Finding{stateFinding("geo")}
		}
		return nil
	})
	if view := h.view(); len(view.BlockingFindings) != 1 {
		t.Fatalf("the fault blocks the gate: %#v", view)
	}

	fixedAt := h.now.Add(5 * time.Second)
	h.now = fixedAt
	broken = false
	h.event(100 * time.Millisecond) // the fix's own watch event
	if view := h.view(); len(view.BlockingFindings) != 1 {
		t.Fatalf("one clear evaluation must not open the gate: %#v", view)
	}

	var confirmedAt time.Time
	h.advance(fixedAt.Add(10*time.Second), func() {
		if confirmedAt.IsZero() && len(h.view().BlockingFindings) == 0 {
			confirmedAt = h.now
		}
	})
	if confirmedAt.IsZero() || confirmedAt.Sub(fixedAt) > 3*time.Second {
		t.Fatalf("a correct fix must pass the gate within 3 s, passed at %v after the fix", confirmedAt.Sub(fixedAt))
	}
	view := h.view()
	if got := findingNames(view.ClearingFindings); len(got) != 1 || got[0] != "geo" {
		t.Fatalf("the finding the gate confirmed clear waits for closure's own hysteresis: %#v", view.ClearingFindings)
	}
	if len(view.BlockingDetectors) != 0 {
		t.Fatalf("no detector blocks the gate any more: %#v", view.BlockingDetectors)
	}
	// Gate re-evaluations stop once confirmed; they never feed closure.
	for _, state := range h.controller.tracker.Snapshot() {
		if !state.Active || state.ClearCount != 1 {
			t.Fatalf("gate re-evaluations must not advance closure's clear count: %#v", state)
		}
	}
	if h.detector.calls > 8 {
		t.Fatalf("gate re-evaluations must stop once confirmed, got %d detector calls", h.detector.calls)
	}
}

func TestGateKeepsRefusingAWrongFix(t *testing.T) {
	h := openGateIncident(t, func() []sdk.Finding { return []sdk.Finding{stateFinding("geo")} })
	h.event(3 * time.Second) // the wrong fix changes something the detector watches
	h.advance(h.now.Add(40*time.Second), func() {
		if len(h.view().BlockingFindings) != 1 {
			t.Fatalf("a finding that still fires must block the gate at %s: %#v", h.now, h.view())
		}
	})
	if clearing := h.view().ClearingFindings; len(clearing) != 0 {
		t.Fatalf("a firing finding is not clearing: %#v", clearing)
	}
}

func TestGateKeepsRefusingAPartialCompositeFix(t *testing.T) {
	geoBroken, rateBroken := true, true
	h := openGateIncident(t, func() []sdk.Finding {
		findings := make([]sdk.Finding, 0)
		if geoBroken {
			findings = append(findings, stateFinding("geo"))
		}
		if rateBroken {
			findings = append(findings, stateFinding("rate"))
		}
		return findings
	})
	geoBroken = false
	h.event(2 * time.Second)
	h.advance(h.now.Add(10*time.Second), nil)
	view := h.view()
	if got := findingNames(view.BlockingFindings); len(got) != 1 || got[0] != "rate" {
		t.Fatalf("the unfixed fault must still block the gate: %#v", view.BlockingFindings)
	}
	if got := findingNames(view.ClearingFindings); len(got) != 1 || got[0] != "geo" {
		t.Fatalf("the fixed fault is confirmed clear on its own: %#v", view.ClearingFindings)
	}
	if len(view.BlockingDetectors) != 1 || view.BlockingDetectors[0] != "health-objective" {
		t.Fatalf("the detector with a firing finding still blocks: %#v", view.BlockingDetectors)
	}
}

func TestGateNeverConfirmsAFlappingFinding(t *testing.T) {
	// After the "fix" the finding flaps on a 3 s cycle: clear for 2 s, firing
	// for 1 s, each transition announced by a watch event.
	var h *gateHarness
	var fixedAt time.Time
	h = openGateIncident(t, func() []sdk.Finding {
		if fixedAt.IsZero() || int(h.now.Sub(fixedAt)/time.Second)%3 == 2 {
			return []sdk.Finding{stateFinding("geo")}
		}
		return nil
	})
	fixedAt = h.now.Add(time.Second)
	h.now = fixedAt
	h.event(0)
	for second := 1; second <= 60; second++ {
		h.advance(fixedAt.Add(time.Duration(second)*time.Second-time.Millisecond), func() {
			if len(h.view().BlockingFindings) != 1 {
				t.Fatalf("a flapping finding must never pass the gate (at %s): %#v", h.now.Sub(fixedAt), h.view())
			}
		})
		h.now = fixedAt.Add(time.Duration(second) * time.Second)
		if second%3 == 0 || second%3 == 2 {
			h.event(0)
		}
		if len(h.view().BlockingFindings) != 1 {
			t.Fatalf("a flapping finding must never pass the gate (at %s): %#v", h.now.Sub(fixedAt), h.view())
		}
	}
}

func TestGateNeedsTheWindowNotJustEvaluations(t *testing.T) {
	config := gateConfig()
	config.GateConfirmation = GateConfirmationPolicy{Evaluations: 2, Window: 5 * time.Second, Interval: time.Second}
	broken := true
	h := openGateIncidentWith(t, config, func() []sdk.Finding {
		if broken {
			return []sdk.Finding{stateFinding("geo")}
		}
		return nil
	})
	broken = false
	h.event(time.Second)
	fixedAt := h.now
	h.advance(fixedAt.Add(4500*time.Millisecond), nil)
	if len(h.view().BlockingFindings) != 1 {
		t.Fatalf("enough clear evaluations inside the window must not open the gate: %#v", h.view())
	}
	h.advance(fixedAt.Add(5500*time.Millisecond), nil)
	if len(h.view().BlockingFindings) != 0 {
		t.Fatalf("the gate opens once the clear window has elapsed: %#v", h.view())
	}
}

func TestGateRelapseBlocksAgain(t *testing.T) {
	broken := true
	h := openGateIncident(t, func() []sdk.Finding {
		if broken {
			return []sdk.Finding{stateFinding("geo")}
		}
		return nil
	})
	broken = false
	h.event(time.Second)
	h.advance(h.now.Add(4*time.Second), nil)
	if len(h.view().BlockingFindings) != 0 {
		t.Fatalf("setup: the fix must pass the gate: %#v", h.view())
	}
	broken = true
	h.event(time.Second)
	if len(h.view().BlockingFindings) != 1 || len(h.view().ClearingFindings) != 0 {
		t.Fatalf("a relapse must block the gate on its first firing evaluation: %#v", h.view())
	}
}

func TestGateConfirmationLeavesClosureHysteresisUnchanged(t *testing.T) {
	broken := true
	h := openGateIncident(t, func() []sdk.Finding {
		if broken {
			return []sdk.Finding{stateFinding("geo")}
		}
		return nil
	})
	broken = false
	fixedAt := h.now.Add(time.Second)
	h.now = fixedAt
	h.event(0)
	close(h.dispatcher.release)
	awaitDispatchCompletionQueued(t, h.controller)
	h.advance(fixedAt.Add(5*time.Second), nil)
	if len(h.view().BlockingFindings) != 0 {
		t.Fatalf("setup: the gate opens: %#v", h.view())
	}
	if !h.controller.IncidentOpen() {
		t.Fatal("closure must still wait for the detector's own second clear evaluation")
	}
	h.advance(fixedAt.Add(31*time.Second), nil)
	if h.controller.IncidentOpen() {
		t.Fatal("the scheduled evaluation 30 s after the fix confirms closure")
	}
}

func TestGateConfirmationDisabledFollowsClosure(t *testing.T) {
	config := gateConfig()
	config.GateConfirmation = GateConfirmationPolicy{}
	broken := true
	detector := gateHealthDetector(func() []sdk.Finding {
		if broken {
			return []sdk.Finding{stateFinding("geo")}
		}
		return nil
	})
	dispatcher := &blockingDispatcher{requests: make(chan IncidentRequest, 1), release: make(chan struct{})}
	defer close(dispatcher.release)
	controller, err := NewController(config, []sdk.Detector{detector},
		staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "demo"}}, dispatcher, time.Unix(0, 0))
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	event := gateWatch
	for _, at := range []time.Duration{0, 500 * time.Millisecond} {
		if err := controller.Step(context.Background(), time.Unix(0, 0).Add(at), &event); err != nil {
			t.Fatalf("step: %v", err)
		}
	}
	executePendingEffect(t, controller)
	awaitRequest(t, dispatcher.requests)
	broken = false
	if err := controller.Step(context.Background(), time.Unix(2, 0), &event); err != nil {
		t.Fatalf("step: %v", err)
	}
	if next := controller.NextWake(); next.Before(time.Unix(30, 0)) {
		t.Fatalf("without a gate policy nothing re-evaluates early: next wake %s", next)
	}
	if err := controller.Step(context.Background(), time.Unix(5, 0), &event); err != nil {
		t.Fatalf("step: %v", err)
	}
	if view := exportState(t, controller).IncidentView; len(view.BlockingFindings) != 0 {
		t.Fatalf("without a gate policy the gate follows closure's clear count: %#v", view)
	}
}

func TestGateConfirmationPolicyValidation(t *testing.T) {
	for _, policy := range []GateConfirmationPolicy{
		{Evaluations: 1, Window: time.Second, Interval: time.Second},
		{Evaluations: 3, Window: -time.Second, Interval: time.Second},
		{Evaluations: 3, Window: time.Second},
	} {
		config := gateConfig()
		config.GateConfirmation = policy
		if _, err := NewController(config, []sdk.Detector{gateHealthDetector(func() []sdk.Finding { return nil })},
			staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "demo"}}, &recordingDispatcher{},
			time.Unix(0, 0)); err == nil {
			t.Fatalf("policy %#v must be rejected", policy)
		}
	}
}
