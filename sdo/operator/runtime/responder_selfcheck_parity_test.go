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
			now := time.Unix(100, 0).UTC()
			c, err := NewController(testControllerConfig(), nil, staticProvider{snapshot: sdktest.Snapshot{}}, &verificationRetryDispatcher{}, now.Add(-time.Second))
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
			c.detectorClearSince = map[string]time.Time{"health": now}
			loop := NewResponderLoop(request, nil, c.provider, nil, PlanExecutor{}, 3)
			loop.VerifyClosure = func(ctx context.Context) ToolResponse {
				return c.cachedResponderVerification(ctx, request.IncidentID, 0)
			}
			server := SessionToolServer{Directory: filepath.Join(t.TempDir(), "tools"), Handle: loop.Handle}
			if err = server.Prepare(); err != nil {
				t.Fatal(err)
			}
			defer server.Close()
			// The intermediate continuation report is not a recovery proof. Exercise
			// the actual closure producer and the production cached tool consumer:
			// fresh clear health alone must not bypass the shared stability window.
			for _, elapsed := range []time.Duration{0, 14 * time.Second, 15 * time.Second} {
				at := now.Add(elapsed)
				state.EvaluatedAt = at
				c.history = []DetectorEvaluation{state}
				c.maybeCloseIncident(context.Background(), at, sdktest.Snapshot{})
				actual := exchangeSelfCheck(t, &server, `{"command":"verify"}`)
				expected := c.cachedVerification
				if actual.Healthy != expected.Healthy || len(actual.Findings) != len(expected.Findings) || actual.Error != expected.Error || len(actual.Drift) != len(expected.Drift) {
					t.Fatalf("at %s tool=%+v controller=%+v", elapsed, actual, expected)
				}
				wantHealthy := elapsed == 15*time.Second && (scenario == "healthy" || scenario == "drift-remaining")
				if actual.Healthy != wantHealthy {
					t.Fatalf("unexpected %s verdict at %s: %+v", scenario, elapsed, actual)
				}
				if c.IncidentOpen() {
					// Result verification may read fresh source, but must consume
					// the same published proof while the incident is current.
					direct := c.responderVerification(context.Background(), request.IncidentID)
					if direct.Healthy != actual.Healthy || direct.Error != actual.Error || len(direct.Findings) != len(actual.Findings) {
						t.Fatalf("fresh result check bypassed proof at %s: %+v", elapsed, direct)
					}
				}
				closure, closed := c.PendingIncidentClosure()
				if closed != wantHealthy || closed && len(closure.FinalDetectorStates) == 0 {
					t.Fatalf("tool and frozen closure disagree at %s: tool=%+v closure=%+v", elapsed, actual, closure)
				}
				if scenario == "drift-remaining" && len(actual.Drift) != 1 {
					t.Fatalf("informational source difference lost: %+v", actual)
				}
			}
		})
	}
}
