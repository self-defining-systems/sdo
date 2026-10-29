package runtime

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// The scheduler produces the proof and the real closure/tool consumers must
// agree before and after it. Detector clearing overlaps the stability window.
func TestRecoveryProofSharesClearingWindowAndToolVerdict(t *testing.T) {
	c, _, step := earlyWaitController(t, stateFinding("health"), stateFinding("health"), sdk.Finding{})
	c.tracker.SetPolicy("health", 2, 10, 0)
	for i := 2; i < 17; i++ {
		step(i)
		got := c.cachedResponderVerification(context.Background(), c.currentIncidentRequest.IncidentID, 0)
		if got.Healthy {
			t.Fatalf("tool granted recovery before 15 seconds at %d", i)
		}
	}
	step(17)
	if c.IncidentOpen() {
		t.Fatal("detector clearing added a second stability window")
	}
	got := c.cachedResponderVerification(context.Background(), c.pendingClosure.Request.IncidentID, 0)
	if !got.Healthy || got.IncidentVerified == nil {
		t.Fatalf("closure proof not shared with tool: %+v", got)
	}
}

func TestRecoveryProofReleasesOutcomeWithoutNarrationGrace(t *testing.T) {
	c, _, step := earlyWaitController(t, stateFinding("health"), stateFinding("health"), sdk.Finding{})
	c.broker = &recordingIncidentBroker{}
	for i := 2; i <= 17; i++ {
		step(i)
	}
	if c.IncidentOpen() {
		t.Fatal("missing recovery proof")
	}
	step(18)
	if _, ok := c.PendingClosureEffect(); !ok {
		t.Fatal("outcome still waits for final narration")
	}
	if !c.pendingClosure.VerifiedAt.Equal(time.Unix(17, 0)) {
		t.Fatal("proof changed during handoff")
	}
}

// Real executor preparation must carry the previous acceptance baseline into
// the source revision it applies, without accepting unknown object instances.
func TestRecoveryProofSourceRepairRebindsAcceptance(t *testing.T) {
	for _, kind := range []string{"same-source", "new-source", "missing-record", "reused-uid"} {
		t.Run(kind, func(t *testing.T) {
			source := addedFieldsSource()
			live := addedFieldsLive()
			live.SetUID("original")
			if err := RecordAcceptance(live, source); err != nil {
				t.Fatal(err)
			}
			if kind == "new-source" {
				_ = unstructured.SetNestedField(source.Object, "new", "spec", "newField")
			}
			if kind == "missing-record" {
				a := live.GetAnnotations()
				delete(a, acceptedPathsAnnotation)
				live.SetAnnotations(a)
			}
			if kind == "reused-uid" {
				live.SetUID("replacement")
			}
			executor := PlanExecutor{Source: planFakeSource{source: source}}
			repaired, err := executor.prepareAction(context.Background(), IncidentRequest{Namespace: addedFieldsRef().Namespace, SourceCommit: "accepted"}, program.ReconcileFromSource{Ref: addedFieldsRef()}, live)
			if err != nil {
				t.Fatal(err)
			}
			_, err = addedSourceFields(repaired, source)
			wantKnown := kind == "same-source" || kind == "new-source"
			if (err == nil) != wantKnown {
				t.Fatalf("acceptance known=%v want %v (%v)", err == nil, wantKnown, err)
			}
		})
	}
}

type recoveryHandoffDispatcher struct{ requests chan IncidentRequest }

func (d recoveryHandoffDispatcher) PrepareDispatch(_ context.Context, r IncidentRequest) (IncidentRequest, error) {
	r.PreparedAgentTask = "synthetic-residual-task"
	return r, nil
}
func (d recoveryHandoffDispatcher) Dispatch(_ context.Context, r IncidentRequest) (IncidentResult, error) {
	d.requests <- r
	result := completedResult(r.IncidentID)
	result.ResponderSessionID = "residual-session"
	return result, nil
}

// Real executor -> real automatic stage -> controller closure -> real pipeline
// handoff. A program's NoMatch is not clearance of its unrelated health peer.
func TestRecoveryProofStageOnlyResidualReachesResponder(t *testing.T) {
	loop, state := pipelineTestLoop(t, true)
	stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, ObservationCeiling: time.Millisecond}
	if err := RunResponseStages(context.Background(), state, []ResponseStage{stage}); err != nil {
		t.Fatal(err)
	}
	if len(state.Findings) == 0 || len(state.PriorStages[0].Mitigations) == 0 {
		t.Fatal("fixture did not produce partial repair")
	}
	now := time.Now().UTC()
	produced, err := responsePipelineResult(state, now.Add(-time.Second), now)
	if err != nil {
		t.Fatal(err)
	}
	d := recoveryHandoffDispatcher{requests: make(chan IncidentRequest, 1)}
	cfg := testControllerConfig()
	cfg.Namespace = "app"
	c, err := NewController(cfg, loop.Detectors, loop.Provider, d, now)
	if err != nil {
		t.Fatal(err)
	}
	c.currentIncidentRequest = cloneIncidentRequest(&state.Request)
	c.currentIncidentRequest.PriorStages = cloneStageReports(state.PriorStages)
	c.incidentOpen = true
	c.dispatchState = contracts.DispatchState_DISPATCH_STATE_RUNNING
	c.incidentDispatchedAt = now.Add(-time.Second)
	c.handleDispatchCompletion(dispatchCompletion{incidentID: state.Request.IncidentID, result: produced, priorStages: state.PriorStages}, now)
	if err := c.Step(context.Background(), now.Add(time.Second), nil); err != nil {
		t.Fatal(err)
	}
	effect, ok := c.PendingDispatchEffect()
	if !ok {
		t.Fatal("active health after automatic repair did not hand off to responder")
	}
	if len(effect.Request.PriorStages) == 0 {
		t.Fatal("handoff lost stage receipts")
	}
	if _, _, err := c.runResponsePipeline(context.Background(), effect.Request); err != nil {
		t.Fatal(err)
	}
	got := awaitRequest(t, d.requests)
	if len(got.Findings) == 0 || len(got.PriorStages[0].Mitigations) == 0 {
		t.Fatal("responder lost residuals or repair receipts")
	}
	if _, ok := c.PendingIncidentClosure(); ok {
		t.Fatal("active health became closure")
	}
}
