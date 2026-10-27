package runtime

import (
	"context"
	"path/filepath"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"testing"
	"time"
)

func TestResponderSelfCheckClosureParity(t *testing.T) {
	for _, scenario := range []string{"healthy", "health-firing", "drift-remaining", "prober-unavailable", "pending"} {
		t.Run(scenario, func(t *testing.T) {
			now := time.Now().UTC()
			c, err := NewController(testControllerConfig(), nil, staticProvider{snapshot: sdktest.Snapshot{}}, &verificationRetryDispatcher{}, now)
			if err != nil {
				t.Fatal(err)
			}
			request := IncidentRequest{IncidentID: "synthetic", Namespace: "demo"}
			c.currentIncidentRequest = &request
			c.incidentOpen = true
			c.responderDone = true
			c.responderCompletedAt = now.Add(-time.Second)
			c.healthDetectorIDs = []string{"health"}
			state := DetectorEvaluation{DetectorID: "health", Status: DetectorEvaluationClear, EvaluatedAt: now}
			finding := sdk.Finding{DetectorID: "health", RuleID: "synthetic", Fingerprint: "synthetic", Status: sdk.FindingActive, PrimaryResource: sdk.ObjectRef{Kind: "Service", Namespace: "demo", Name: "api"}}
			switch scenario {
			case "health-firing":
				state.Status = DetectorEvaluationFiring
				state.Fingerprints = []string{FindingFingerprint(finding)}
				c.tracker.SetPolicy("health", 1, 1, 0)
				c.tracker.Observe(now, "health", []sdk.Finding{finding})
			case "drift-remaining":
				finding.DetectorID = "drift.unexplained"
				finding.Metadata = map[string]any{"drift_event": sdk.DriftEvent{Ref: finding.PrimaryResource, Fields: []sdk.DriftField{{Path: "spec.ports[0].targetPort"}}}}
				request.Findings = []sdk.Finding{finding}
				c.ConfigureDrift(DriftControllerConfig{View: func(sdk.DetectionContext) program.ReadView {
					return driftReadView{ReplayReadView: &sdktest.ReplayReadView{}, diff: program.Diff{Changed: true, Text: "synthetic accepted/live target port mismatch"}}
				}})
			case "pending":
				state.Status = DetectorEvaluationPending
			case "prober-unavailable":
				state.Status = DetectorEvaluationError
				state.Error = "prober unavailable"
			}
			c.history = []DetectorEvaluation{state}
			loop := NewResponderLoop(request, nil, c.provider, nil, PlanExecutor{}, 3)
			loop.VerifyClosure = func(ctx context.Context) ToolResponse { return c.responderVerification(ctx, request.IncidentID) }
			server := SessionToolServer{Directory: filepath.Join(t.TempDir(), "tools"), Handle: loop.Handle}
			if err = server.Prepare(); err != nil {
				t.Fatal(err)
			}
			defer server.Close()
			actual := exchangeSelfCheck(t, &server, `{"command":"verify"}`)
			source := c.verifyClosureSource(context.Background(), now, sdktest.Snapshot{}, nil)
			_, health := c.finalVerificationStates()
			expected := c.continuationVerificationLocked(health, source)
			if actual.Healthy != expected.Healthy || len(actual.Findings) != len(expected.Findings) || actual.Error != expected.Error {
				t.Fatalf("tool=%+v controller=%+v", actual, expected)
			}
			if actual.Healthy != (scenario == "healthy" || scenario == "drift-remaining") {
				t.Fatalf("unexpected %s verdict: %+v", scenario, actual)
			}
		})
	}
}
