package runtime

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// A harness redeploy leaves a comment-only ConfigMap difference that predates
// the fault. The responder may call it benign in prose, but only a structured
// label can clear it; the controller must not wait out VerificationTimeout.
func preexistingConfigMapDrift() (sdk.DriftEvent, program.Diff) {
	accepted, observed := "# license\necho geo", "echo geo"
	event := sdk.DriftEvent{
		Ref:            sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "demo", Name: "mongo-geo-script"},
		Classification: sdk.DriftUnexplained, Manager: "OpenAPI-Generator",
		Fields: []sdk.DriftField{{Path: "data.script", Source: digestJSON(accepted), New: digestJSON(observed)}},
	}
	a, _ := json.Marshal(map[string]any{"data": map[string]any{"script": accepted}})
	o, _ := json.Marshal(map[string]any{"data": map[string]any{"script": observed}})
	return event, program.Diff{Changed: true, Text: "accepted: " + string(a) + "\nobserved: " + string(o)}
}

type labelingDispatcher struct {
	requests chan IncidentRequest
	label    func(IncidentRequest) []sdk.DriftEvent
}

func (d *labelingDispatcher) Dispatch(_ context.Context, request IncidentRequest) (IncidentResult, error) {
	d.requests <- request
	result := completedResult(request.IncidentID)
	if d.label != nil {
		result.DriftEvents = d.label(request)
	}
	return result, nil
}

func benignLabel(event sdk.DriftEvent) sdk.DriftEvent {
	event.InvestigationStatus = "completed"
	event.InvestigationVerdict = "benign"
	event.InvestigationReason = "harness redeploy stripped license comments only"
	event.InvestigationEvidence = []sdk.DriftEvidence{{Kind: "observed_impact", Observation: "script behaviour is identical without the comments"}}
	return event
}

func newLabelLivenessController(t *testing.T, dispatcher Dispatcher) (*Controller, sdk.DriftEvent, time.Time) {
	t.Helper()
	start := time.Unix(100, 0).UTC()
	event, diff := preexistingConfigMapDrift()
	health := followUpHealthDetector("health", true, true, false, false, false, false, false, false)
	config := testControllerConfig()
	config.RepositoryWorktree = t.TempDir()
	config.VerificationTimeout = 10 * time.Minute
	config.ClearThreshold = 1
	// MaxFollowUps stays 0: label recovery must not depend on residual-health follow-ups.
	c, err := NewController(config, []sdk.Detector{health}, staticProvider{snapshot: sdktest.Snapshot{}}, dispatcher, start)
	if err != nil {
		t.Fatal(err)
	}
	c.ConfigureDrift(DriftControllerConfig{ProgramPersistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1}, BatchWindow: time.Nanosecond, View: func(sdk.DetectionContext) program.ReadView {
		return driftReadView{ReplayReadView: &sdktest.ReplayReadView{}, diff: diff}
	}})
	c.queueUnexplainedFinding(start, event)
	return c, event, start
}

func runToResponderCompletion(t *testing.T, c *Controller, dispatcher *labelingDispatcher, start time.Time) {
	t.Helper()
	for _, second := range []int{0, 1} {
		if err := c.Step(context.Background(), start.Add(time.Duration(second)*time.Second), nil); err != nil {
			t.Fatal(err)
		}
	}
	executePendingEffect(t, c)
	awaitRequest(t, dispatcher.requests)
	awaitDispatchCompletionQueued(t, c)
}

func TestUnlabeledPreexistingDriftGetsLabelFollowUpBeforeVerificationTimeout(t *testing.T) {
	dispatcher := &labelingDispatcher{requests: make(chan IncidentRequest, 4)}
	c, event, start := newLabelLivenessController(t, dispatcher)
	runToResponderCompletion(t, c, dispatcher, start)
	// Health clears at start+2s; the drift finding stays active without a label.
	for second := 2; second <= 5; second++ {
		if err := c.Step(context.Background(), start.Add(time.Duration(second)*time.Second), nil); err != nil {
			t.Fatal(err)
		}
	}
	effect, ok := c.PendingDispatchEffect()
	if !ok {
		t.Fatalf("unlabeled drift with quiet health did not produce a label follow-up: %+v", exportState(t, c))
	}
	followUp := effect.Request.FollowUp
	if followUp == nil || !followUp.DriftLabelsOnly || followUp.Attempt != 1 {
		t.Fatalf("follow-up must be a bounded labels-only request: %+v", followUp)
	}
	if len(effect.Request.Findings) != 1 || effect.Request.Findings[0].DetectorID != "drift.unexplained" ||
		len(effect.Request.DriftEvents) != 1 || driftEventKey(effect.Request.DriftEvents[0]) != driftEventKey(event) {
		t.Fatalf("follow-up must carry only the unlabeled drift: %+v", effect.Request)
	}
	if c.DetectorReviewRequired() {
		t.Fatal("review must not be required while a label follow-up is available")
	}

	// The follow-up returns the label; the incident then closes normally.
	dispatcher.label = func(r IncidentRequest) []sdk.DriftEvent { return []sdk.DriftEvent{benignLabel(r.DriftEvents[0])} }
	executePendingEffect(t, c)
	awaitRequest(t, dispatcher.requests)
	awaitDispatchCompletionQueued(t, c)
	for second := 6; second <= 8; second++ {
		if err := c.Step(context.Background(), start.Add(time.Duration(second)*time.Second), nil); err != nil {
			t.Fatal(err)
		}
	}
	if _, ok := c.PendingIncidentClosure(); !ok {
		t.Fatalf("labeled drift did not close the incident: %+v", exportState(t, c))
	}
	if c.DetectorReviewRequired() {
		t.Fatal("labeled drift closed through detector review")
	}
}

func TestLabeledDriftClosesWithoutFollowUp(t *testing.T) {
	dispatcher := &labelingDispatcher{requests: make(chan IncidentRequest, 4),
		label: func(r IncidentRequest) []sdk.DriftEvent { return []sdk.DriftEvent{benignLabel(r.DriftEvents[0])} }}
	c, _, start := newLabelLivenessController(t, dispatcher)
	runToResponderCompletion(t, c, dispatcher, start)
	for second := 2; second <= 5; second++ {
		if err := c.Step(context.Background(), start.Add(time.Duration(second)*time.Second), nil); err != nil {
			t.Fatal(err)
		}
	}
	if _, ok := c.PendingDispatchEffect(); ok {
		t.Fatal("a labeled drift needs no follow-up")
	}
	if _, ok := c.PendingIncidentClosure(); !ok {
		t.Fatalf("labeled drift did not close the incident: %+v", exportState(t, c))
	}
}

func TestStillUnlabeledDriftRequiresReviewNamingTheEventWithoutWaitingForTimeout(t *testing.T) {
	dispatcher := &labelingDispatcher{requests: make(chan IncidentRequest, 4)}
	c, _, start := newLabelLivenessController(t, dispatcher)
	runToResponderCompletion(t, c, dispatcher, start)
	for second := 2; second <= 5; second++ {
		if err := c.Step(context.Background(), start.Add(time.Duration(second)*time.Second), nil); err != nil {
			t.Fatal(err)
		}
	}
	executePendingEffect(t, c)
	awaitRequest(t, dispatcher.requests)
	awaitDispatchCompletionQueued(t, c)
	for second := 6; second <= 9; second++ {
		if err := c.Step(context.Background(), start.Add(time.Duration(second)*time.Second), nil); err != nil {
			t.Fatal(err)
		}
	}
	if _, ok := c.PendingDispatchEffect(); ok {
		t.Fatal("label follow-up must be bounded to one attempt")
	}
	required, reason := c.DetectorReviewStatus()
	if !required {
		t.Fatalf("an unanswerable label request must not wait for VerificationTimeout: %+v", exportState(t, c))
	}
	if !strings.Contains(reason, "ConfigMap/mongo-geo-script") || !strings.Contains(reason, "drift_events") {
		t.Fatalf("review reason must name the unlabeled drift event: %q", reason)
	}
}
