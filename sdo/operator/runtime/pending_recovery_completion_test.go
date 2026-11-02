package runtime

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func TestResponderPendingRecoveryProofRequiresCompletion(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	provider := staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}
	c, err := NewController(testControllerConfig(), []sdk.Detector{inventoryHealth()}, provider, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, now)
	if err != nil {
		t.Fatal(err)
	}
	request := goldenRequest(t)
	request.IncidentID, request.Namespace = "pending-proof", "app"
	request.ResponseDeadline = now.Add(time.Minute)
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "front"}
	request.Findings = []sdk.Finding{{DetectorID: "drift.unexplained", Status: sdk.FindingActive, PrimaryResource: ref, Metadata: map[string]any{"drift_event": sdk.DriftEvent{Ref: ref, Fields: []sdk.DriftField{{Path: "spec.ports[0].targetPort"}}}}}}
	c.ConfigureDrift(DriftControllerConfig{View: func(sdk.DetectionContext) program.ReadView {
		return sourceCompareView{ReplayReadView: &sdktest.ReplayReadView{}, diff: program.Diff{Changed: true, Text: "accepted source differs"}}
	}})
	c.currentIncidentRequest, c.incidentOpen = &request, true
	loop := NewResponderLoop(request, nil, provider, nil, PlanExecutor{}, 3)
	bindRecoveryFixture(c, loop, now)
	loop.WaitClosure = func(ctx context.Context, wait time.Duration) ToolResponse {
		return c.cachedResponderVerification(ctx, request.IncidentID, wait)
	}
	server := SessionToolServer{Directory: filepath.Join(t.TempDir(), "tools"), Handle: loop.Handle}
	if err := server.Prepare(); err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	status := func(response ToolResponse) string {
		raw, err := json.Marshal(response)
		if err != nil {
			t.Fatal(err)
		}
		var wire map[string]any
		if err := json.Unmarshal(raw, &wire); err != nil {
			t.Fatal(err)
		}
		value, _ := wire["verification_status"].(string)
		return value
	}
	for _, elapsed := range []time.Duration{0, 15 * time.Second} {
		if err := c.Step(context.Background(), now.Add(elapsed), nil); err != nil {
			t.Fatal(err)
		}
		response := exchangeSelfCheck(t, &server, `{"command":"verify"}`)
		if response.Healthy || response.IncidentVerified != nil || status(response) != "awaiting_responder_completion" || !strings.Contains(response.Summary, "return the structured incident result") {
			t.Fatalf("tool made the responder wait for its own completion: %+v", response)
		}
		waitCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		waited := loop.Handle(waitCtx, ToolRequest{Command: "verify", WaitSeconds: 60})
		cancel()
		if waited.Error != "" || status(waited) != "awaiting_responder_completion" {
			t.Fatalf("verify --wait hid the completion request: %+v", waited)
		}
		if !c.IncidentOpen() {
			t.Fatal("pending completion granted closure")
		}
	}
	finished := now.Add(16 * time.Second)
	result := completedResult(request.IncidentID)
	result.Timing.StartedAt, result.Timing.CompletedAt = now, finished
	c.handleDispatchCompletion(dispatchCompletion{incidentID: request.IncidentID, result: result}, finished)
	if err := c.Step(context.Background(), finished.Add(time.Second), nil); err != nil {
		t.Fatal(err)
	}
	final := c.cachedResponderVerification(context.Background(), request.IncidentID, 0)
	if !final.Healthy || status(final) != "" || c.IncidentOpen() {
		t.Fatalf("completion did not release the independent proof: %+v", final)
	}
}

func TestResponderPendingCompletionRetainsRealHealthFailures(t *testing.T) {
	for _, mode := range []string{"firing", "error"} {
		t.Run(mode, func(t *testing.T) {
			now := time.Unix(100, 0).UTC()
			health := controllerDetector("health", time.Second, stateFinding("health"))
			health.spec = inventoryHealth().Spec()
			health.spec.Class, health.spec.Owner = sdk.DetectorClassHealth, sdk.DetectorOwnerHealthJudge
			health.spec.OriginatingCommit = "health-objective"
			if mode == "error" {
				health.errors[0] = context.DeadlineExceeded
			}
			c, err := NewController(testControllerConfig(), []sdk.Detector{health}, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, now)
			if err != nil {
				t.Fatal(err)
			}
			request := goldenRequest(t)
			request.IncidentID = "pending-negative"
			request.Namespace = "app"
			request.ResponseDeadline = now.Add(time.Minute)
			request.Findings = []sdk.Finding{{DetectorID: "drift.unexplained", Status: sdk.FindingActive, PrimaryResource: sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "front"}}}
			c.currentIncidentRequest, c.incidentOpen = &request, true
			loop := NewResponderLoop(request, nil, c.provider, nil, PlanExecutor{}, 3)
			bindRecoveryFixture(c, loop, now)
			if err := c.Step(context.Background(), now, nil); err != nil {
				t.Fatal(err)
			}
			result := loop.Handle(context.Background(), ToolRequest{Command: "verify"})
			if result.Healthy || result.VerificationStatus != "" || !c.IncidentOpen() {
				t.Fatalf("%s was promoted to finish guidance: %+v", mode, result)
			}
		})
	}
}
