package runtime

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

type summarizingDispatcher struct {
	requests chan IncidentRequest
	mu       sync.Mutex
}

func (d *summarizingDispatcher) Dispatch(_ context.Context, request IncidentRequest) (IncidentResult, error) {
	d.requests <- request
	result := completedResult(request.IncidentID)
	result.ConfirmedRootCauses = []ConfirmedRootCause{{
		Summary:   "fixed " + request.Findings[0].DetectorID,
		Resources: []sdk.ObjectRef{{Kind: "Deployment", Name: "x"}},
	}}
	return result, nil
}

func followUpHealthDetector(id string, samples ...bool) *sequenceDetector {
	findings := make([]sdk.Finding, 0, len(samples))
	for _, active := range samples {
		if !active {
			findings = append(findings, sdk.Finding{})
			continue
		}
		finding := stateFinding(id)
		finding.Severity = sdk.SeverityCritical
		findings = append(findings, finding)
	}
	detector := controllerDetector(id, time.Second, findings...)
	detector.spec.Class = sdk.DetectorClassHealth
	detector.spec.Owner = sdk.DetectorOwnerHealthJudge
	detector.spec.Persistence = sdk.PersistencePolicy{Firing: 2, Clearing: 2}
	detector.spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityCritical}
	detector.spec.OriginatingCommit = "health-objective"
	return detector
}

func newFollowUpController(t *testing.T, maxFollowUps int, healthB *sequenceDetector) (*Controller, *summarizingDispatcher) {
	t.Helper()
	healthA := followUpHealthDetector("healtha", true, true, false)
	dispatcher := &summarizingDispatcher{requests: make(chan IncidentRequest, 4)}
	config := testControllerConfig()
	config.VerificationTimeout = 10 * time.Second
	config.MaxFollowUps = maxFollowUps
	config.FollowUpCooldown = time.Second
	controller, err := NewController(
		config, []sdk.Detector{healthA, healthB}, staticProvider{snapshot: sdktest.Snapshot{}},
		dispatcher, time.Unix(0, 0),
	)
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	return controller, dispatcher
}

func stepAt(t *testing.T, controller *Controller, second int64) {
	t.Helper()
	if err := controller.Step(context.Background(), time.Unix(second, 0), nil); err != nil {
		t.Fatalf("step %d: %v", second, err)
	}
}

// completeFirstResponder runs the original incident (dispatched for healtha
// only) to completion; healthb activates after dispatch and stays active.
func completeFirstResponder(t *testing.T, controller *Controller, dispatcher *summarizingDispatcher) IncidentRequest {
	t.Helper()
	stepAt(t, controller, 0)
	stepAt(t, controller, 1)
	executePendingEffect(t, controller)
	first := awaitRequest(t, dispatcher.requests)
	awaitDispatchCompletionQueued(t, controller)
	stepAt(t, controller, 2)
	return first
}

func healthBPersistent() *sequenceDetector {
	return followUpHealthDetector("healthb", false, false, true)
}

func TestControllerDispatchesFollowUpForResidualHealthFinding(t *testing.T) {
	controller, dispatcher := newFollowUpController(t, 2, healthBPersistent())
	first := completeFirstResponder(t, controller, dispatcher)
	if len(first.Findings) != 1 || first.Findings[0].DetectorID != "healtha" || first.FollowUp != nil {
		t.Fatalf("unexpected original request: %#v", first)
	}
	stepAt(t, controller, 3)
	effect, ok := controller.PendingDispatchEffect()
	if !ok {
		t.Fatal("residual health finding did not produce a follow-up dispatch")
	}
	request := effect.Request
	if request.IncidentID == first.IncidentID || request.FollowUp == nil {
		t.Fatalf("follow-up must be a distinct incident with context: %#v", request)
	}
	followUp := request.FollowUp
	if followUp.ParentIncidentID != first.IncidentID || followUp.OriginalIncidentID != first.IncidentID ||
		followUp.Attempt != 1 || followUp.MaxFollowUps != 2 {
		t.Fatalf("unexpected follow-up context: %#v", followUp)
	}
	if len(request.Findings) != 1 || request.Findings[0].DetectorID != "healthb" {
		t.Fatalf("follow-up must carry only residual findings: %#v", request.Findings)
	}
	if !strings.Contains(followUp.PriorSummary, "fixed healtha") {
		t.Fatalf("follow-up omitted the prior responder summary: %q", followUp.PriorSummary)
	}
	if controller.DetectorReviewRequired() {
		t.Fatal("follow-up must not require detector review")
	}
	executePendingEffect(t, controller)
	if got := awaitRequest(t, dispatcher.requests); got.IncidentID != request.IncidentID {
		t.Fatalf("dispatched %q, want %q", got.IncidentID, request.IncidentID)
	}
}

func TestControllerFollowUpsAreBoundedAcrossRestarts(t *testing.T) {
	controller, dispatcher := newFollowUpController(t, 1, healthBPersistent())
	completeFirstResponder(t, controller, dispatcher)
	stepAt(t, controller, 3)
	executePendingEffect(t, controller)
	followUp := awaitRequest(t, dispatcher.requests)
	awaitDispatchCompletionQueued(t, controller)
	stepAt(t, controller, 4)
	stepAt(t, controller, 6)
	if _, ok := controller.PendingDispatchEffect(); ok {
		t.Fatal("follow-up budget of one was exceeded")
	}
	if controller.DetectorReviewRequired() {
		t.Fatal("detector review required before the verification window elapsed")
	}
	stepAt(t, controller, 20)
	if !controller.DetectorReviewRequired() {
		t.Fatal("exhausted follow-ups must fall back to detector review")
	}
	state := exportState(t, controller)
	if state.IncidentRequest == nil || state.IncidentRequest.IncidentId != followUp.IncidentID ||
		state.IncidentRequest.FollowUp == nil || state.IncidentRequest.FollowUp.Attempt != 1 {
		t.Fatalf("follow-up attempt was not persisted: %#v", state.IncidentRequest)
	}
	restarted, _ := newFollowUpController(t, 1, healthBPersistent())
	if err := restarted.RestoreState(state); err != nil {
		t.Fatalf("restore: %v", err)
	}
	stepAt(t, restarted, 30)
	if _, ok := restarted.PendingDispatchEffect(); ok {
		t.Fatal("a restart from exhausted state dispatched another follow-up")
	}
	if !restarted.DetectorReviewRequired() {
		t.Fatal("restart lost the detector review requirement")
	}
}

func TestControllerWithoutFollowUpsKeepsDetectorReview(t *testing.T) {
	controller, dispatcher := newFollowUpController(t, 0, healthBPersistent())
	completeFirstResponder(t, controller, dispatcher)
	stepAt(t, controller, 3)
	if _, ok := controller.PendingDispatchEffect(); ok {
		t.Fatal("follow-ups are opt-in and must be off by default")
	}
	stepAt(t, controller, 20)
	if !controller.DetectorReviewRequired() {
		t.Fatal("default behavior must still require detector review")
	}
}

func TestControllerClosesFollowUpIncidentOnceResidualClears(t *testing.T) {
	healthB := followUpHealthDetector("healthb", false, false, true, true, false)
	controller, dispatcher := newFollowUpController(t, 2, healthB)
	first := completeFirstResponder(t, controller, dispatcher)
	stepAt(t, controller, 3)
	executePendingEffect(t, controller)
	followUp := awaitRequest(t, dispatcher.requests)
	awaitDispatchCompletionQueued(t, controller)
	stepAt(t, controller, 4)
	stepAt(t, controller, 5)
	closure, ok := controller.PendingIncidentClosure()
	if !ok {
		t.Fatal("verified follow-up did not close the incident")
	}
	if closure.Request.IncidentID != followUp.IncidentID || closure.Request.FollowUp == nil ||
		closure.Request.FollowUp.OriginalIncidentID != first.IncidentID {
		t.Fatalf("closure does not identify the follow-up chain: %#v", closure.Request)
	}
}

func TestNewControllerRejectsNegativeFollowUpConfig(t *testing.T) {
	for _, mutate := range []func(*ControllerConfig){
		func(config *ControllerConfig) { config.MaxFollowUps = -1 },
		func(config *ControllerConfig) { config.FollowUpCooldown = -time.Second },
	} {
		config := testControllerConfig()
		mutate(&config)
		_, err := NewController(
			config, nil, staticProvider{snapshot: sdktest.Snapshot{}}, &recordingDispatcher{}, time.Unix(0, 0),
		)
		if err == nil {
			t.Fatalf("config %#v was accepted", config)
		}
	}
}
