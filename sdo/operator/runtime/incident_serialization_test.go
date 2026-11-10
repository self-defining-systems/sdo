package runtime

import (
	"context"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

// twoFaultHealth is one health detector over two independent components: the
// first fault opens the incident, the second arrives while its responder works.
type twoFaultHealth struct{ first, second atomic.Bool }

func (*twoFaultHealth) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: "health", Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge,
		Interval: time.Second, Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1},
		Batching: sdk.BatchingPolicy{Severity: sdk.SeverityWarning}, OriginatingCommit: "synthetic"}
}

func (d *twoFaultHealth) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	findings := []sdk.Finding{}
	for _, fault := range []struct {
		on   bool
		name string
	}{{d.first.Load(), "first"}, {d.second.Load(), "second"}} {
		if fault.on {
			findings = append(findings, sdk.Finding{DetectorID: "health", RuleID: "unhealthy", Status: sdk.FindingActive,
				Severity: sdk.SeverityCritical, Summary: fault.name + " unhealthy", Evidence: "synthetic",
				PrimaryResource: sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "demo", Name: fault.name}})
		}
	}
	return findings, nil
}

type recordingLifecycle struct {
	mu     sync.Mutex
	events []*contracts.LifecycleEvent
}

func (r *recordingLifecycle) RecordLifecycle(event *contracts.LifecycleEvent) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, event)
	return nil
}

func (r *recordingLifecycle) closed() *contracts.LifecycleEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, event := range r.events {
		if event.GetEvent() == PhaseClosed {
			return event
		}
	}
	return nil
}

type recordingFirings struct {
	mu      sync.Mutex
	records []*contracts.FiringRecord
}

func (r *recordingFirings) Record(record *contracts.FiringRecord) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = append(r.records, record)
	return nil
}

func (r *recordingFirings) relations(fingerprint string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var relations []string
	for _, record := range r.records {
		if record.GetFingerprint() == fingerprint {
			relations = append(relations, record.GetEvent()+":"+record.GetDispatchRelation())
		}
	}
	return relations
}

// The controller runs one incident at a time. A fault that starts while an
// incident's responder works does not get its own incident, so the open
// incident must say so: the responder's verification names the finding as new
// since dispatch, and the closure record lists it, so neither the responder
// nor analysis mistakes it for the incident's own fault.
func TestFindingAfterDispatchIsMarkedNewInTheOpenIncident(t *testing.T) {
	runOnOneProcessor(t)
	ctx := context.Background()
	opened := time.Unix(20_000, 0).UTC()
	cfg := testControllerConfig()
	cfg.FiringThreshold, cfg.ClearThreshold = 1, 1
	cfg.ClosureStabilityWindow = 5 * time.Second
	cfg.VerificationTimeout = 5 * time.Minute
	health := &twoFaultHealth{}
	health.first.Store(true)
	dispatcher := &blockingDispatcher{}
	controller, err := NewController(cfg, []sdk.Detector{health}, staticProvider{}, dispatcher, opened)
	if err != nil {
		t.Fatal(err)
	}
	var clockMu sync.Mutex
	now := opened
	controller.now = func() time.Time { clockMu.Lock(); defer clockMu.Unlock(); return now }
	dispatcher.clock = controller.clock
	controller.gitRead = fakeArchitecture("")
	broker := &riskBroker{}
	controller.broker = broker
	lifecycle := &recordingLifecycle{}
	controller.SetLifecycleSink(lifecycle)
	firings := &recordingFirings{}
	controller.SetFiringSink(firings)

	second := FindingFingerprint(sdk.Finding{DetectorID: "health", RuleID: "unhealthy", PrimaryResource: sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "demo", Name: "second"}})
	var dispatchedAt, secondAt time.Time
	var verification ToolResponse
	for step := 0; step < 600 && len(broker.closures()) == 0; step++ {
		clockMu.Lock()
		at := now
		clockMu.Unlock()
		switch {
		case dispatchedAt.IsZero() && dispatcher.hasStarted():
			dispatchedAt = at
		case !dispatchedAt.IsZero() && secondAt.IsZero() && at.Sub(dispatchedAt) >= 5*time.Second:
			health.second.Store(true)
			secondAt = at
		case !secondAt.IsZero() && at.Sub(secondAt) == 5*time.Second:
			controller.mu.Lock()
			id := controller.currentIncidentRequest.IncidentID
			controller.mu.Unlock()
			verification = controller.cachedResponderVerification(ctx, id, 0)
		case !secondAt.IsZero() && at.Sub(secondAt) >= 10*time.Second:
			health.first.Store(false)
			health.second.Store(false)
		}
		if err := controller.Step(ctx, at, nil); err != nil {
			t.Fatal(err)
		}
		if effect, ok := controller.PendingWorkspaceEffect(); ok {
			if err := controller.ExecuteWorkspaceEffect(ctx, effect); err != nil {
				t.Fatal(err)
			}
			completion := <-controller.workspaceResults
			controller.workspaceResults <- completion
		} else if effect, ok := controller.PendingDispatchEffect(); ok {
			if err := controller.ExecuteDispatchEffect(ctx, effect); err != nil {
				t.Fatal(err)
			}
			for !dispatcher.hasStarted() {
				runtime.Gosched()
			}
		} else if effect, ok := controller.PendingClosureEffect(); ok {
			if err := controller.ExecuteClosureEffect(ctx, effect); err != nil {
				t.Fatal(err)
			}
			awaitEffectGoroutines(controller)
		}
		clockMu.Lock()
		now = now.Add(time.Second)
		clockMu.Unlock()
	}
	if secondAt.IsZero() {
		t.Fatal("the responder was never dispatched")
	}

	var newKeys []string
	for _, finding := range verification.NewSinceDispatch {
		newKeys = append(newKeys, FindingFingerprint(finding))
	}
	if !slices.Equal(newKeys, []string{second}) {
		t.Fatalf("verification new_since_dispatch = %v, want only the second fault %s (findings %d)", newKeys, second, len(verification.Findings))
	}
	if !strings.Contains(verification.Summary, "new since dispatch") {
		t.Fatalf("verification summary does not tell the responder about the new finding: %q", verification.Summary)
	}

	closures := broker.closures()
	if len(closures) == 0 {
		t.Fatal("incident never closed")
	}
	if got := closures[0].Request.Findings; len(got) != 1 || got[0].PrimaryResource.Name != "first" {
		t.Fatalf("the dispatched request must keep only the incident's own finding, got %+v", got)
	}
	closed := lifecycle.closed()
	if closed == nil {
		t.Fatal("no incident_closed lifecycle event")
	}
	if want := []string{FindingStateKey("health", second)}; !slices.Equal(closed.GetAfterDispatchFindingKeys(), want) {
		t.Fatalf("incident_closed after_dispatch_finding_keys = %v, want %v", closed.GetAfterDispatchFindingKeys(), want)
	}
	if got := firings.relations(second); len(got) == 0 || got[0] != "activated:after_dispatch" {
		t.Fatalf("second fault firing relations = %v, want activated:after_dispatch first", got)
	}
}

// A finding that joins the open incident after its responder launched (a
// learned-playbook match or late drift) is still recorded as after_dispatch:
// joining the incident must not rewrite when it arrived.
func TestLateJoinedFindingKeepsItsAfterDispatchRelation(t *testing.T) {
	now := time.Unix(30_000, 0).UTC()
	cfg := testControllerConfig()
	cfg.FiringThreshold = 1
	controller, err := NewController(cfg, nil, staticProvider{}, &recordingDispatcher{}, now)
	if err != nil {
		t.Fatal(err)
	}
	firings := &recordingFirings{}
	controller.SetFiringSink(firings)
	own := sdk.Finding{DetectorID: "own", RuleID: "r", Status: sdk.FindingActive, Severity: sdk.SeverityCritical,
		PrimaryResource: sdk.ObjectRef{Kind: "Deployment", Namespace: "demo", Name: "first"}}
	late := sdk.Finding{DetectorID: "late", RuleID: "r", Status: sdk.FindingActive, Severity: sdk.SeverityCritical,
		PrimaryResource: sdk.ObjectRef{Kind: "Deployment", Namespace: "demo", Name: "second"}}
	controller.beginIncident(now, IncidentRequest{IncidentID: "synthetic", Application: "demo", Findings: []sdk.Finding{own}}, []sdk.Finding{own})
	controller.mu.Lock()
	controller.incidentDispatchedAt = now.Add(time.Second)
	controller.dispatchState = contracts.DispatchState_DISPATCH_STATE_RUNNING
	// The learned path joins the finding, then the evaluation records its activation.
	controller.currentIncidentRequest.Findings = append(controller.currentIncidentRequest.Findings, late)
	controller.noteBatchedLocked("synthetic", []sdk.Finding{late})
	controller.mu.Unlock()
	changes := controller.tracker.Observe(now.Add(2*time.Second), "late", []sdk.Finding{late})
	controller.recordFiring(sdk.DetectorSpec{ID: "late", Class: sdk.DetectorClassIncident}, now.Add(2*time.Second), []sdk.Finding{late}, changes)
	relations := firings.relations(FindingFingerprint(late))
	if len(relations) < 2 {
		t.Fatalf("late-joined finding records %v, want batched and activated", relations)
	}
	for _, relation := range relations {
		if !strings.HasSuffix(relation, ":after_dispatch") {
			t.Fatalf("late-joined finding records %v, want every record after_dispatch", firings.relations(FindingFingerprint(late)))
		}
	}
	controller.mu.Lock()
	defer controller.mu.Unlock()
	index := controller.timelineIndexLocked("late", FindingFingerprint(late))
	if index < 0 || controller.timeline[index].Relation != RelationAfterDispatch {
		t.Fatalf("late-joined timeline entry = %+v, want after_dispatch", controller.timeline)
	}
}
