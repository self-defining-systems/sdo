package runtime

import (
	"context"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

// TestSuccessClosureWaitsForTheStabilityWindow pins the success-commit gate on
// the normal (responder-done) closure path. Fast health proof (for example
// three quick dials after a pod turns Ready) reports health clear as soon as
// it holds, but a success outcome teaches memory, so it commits only after
// health has stayed clear for the closure stability window. A relapse inside
// the window must never commit success.
func TestSuccessClosureWaitsForTheStabilityWindow(t *testing.T) {
	const clearAt = 2 // responder done; health clears at t=2 s
	for _, relapse := range []bool{false, true} {
		name := "stable"
		if relapse {
			name = "relapse-at-plus-5s"
		}
		t.Run(name, func(t *testing.T) {
			samples := []sdk.Finding{stateFinding("health"), stateFinding("health")}
			for i := clearAt; i < clearAt+5; i++ {
				samples = append(samples, sdk.Finding{})
			}
			if relapse {
				samples = append(samples, stateFinding("health"))
			} else {
				samples = append(samples, sdk.Finding{})
			}
			health := controllerDetector("health", time.Second, samples...)
			health.spec.Class, health.spec.Owner = sdk.DetectorClassHealth, sdk.DetectorOwnerHealthJudge
			health.spec.Persistence = sdk.PersistencePolicy{Firing: 2, Clearing: 1}
			health.spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityWarning}
			health.spec.OriginatingCommit = "health-objective"
			dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
			config := testControllerConfig()
			config.VerificationTimeout = time.Minute
			c, err := NewController(config, []sdk.Detector{health}, staticProvider{snapshot: sdktest.Snapshot{}}, dispatcher, time.Unix(0, 0))
			if err != nil {
				t.Fatal(err)
			}
			closed := make(chan IncidentClosure, 1)
			c.OnIncidentClosed = func(closure IncidentClosure) { closed <- closure }
			step := func(at int) {
				t.Helper()
				if err := c.Step(context.Background(), time.Unix(int64(at), 0), nil); err != nil {
					t.Fatal(err)
				}
			}
			step(0)
			step(1)
			executePendingEffect(t, c)
			awaitRequest(t, dispatcher.requests)
			awaitDispatchCompletionQueued(t, c)

			window := int(c.config.ClosureStabilityWindow / time.Second)
			if window != 15 {
				t.Fatalf("default closure stability window = %ds, want 15s", window)
			}
			for at := clearAt; at < clearAt+window+30; at++ {
				step(at)
				select {
				case closure := <-closed:
					if relapse {
						if closure.HealthClearedAt != nil || len(closure.FinalDetectorStates) != 0 {
							t.Fatalf("a relapse inside the window committed success at %ds: %+v", at, closure)
						}
						return
					}
					if at < clearAt+window {
						t.Fatalf("success committed at %ds, before %ds of continued health", at, clearAt+window)
					}
					if closure.HealthClearedAt == nil || !closure.HealthClearedAt.Equal(time.Unix(clearAt, 0).UTC()) {
						t.Fatalf("health cleared at %v, want the early proof at %ds", closure.HealthClearedAt, clearAt)
					}
					if closure.ClosureStabilityWindow != c.config.ClosureStabilityWindow {
						t.Fatalf("closure window = %v, want %v", closure.ClosureStabilityWindow, c.config.ClosureStabilityWindow)
					}
					return
				default:
				}
			}
			if !relapse {
				t.Fatal("stable health never committed success")
			}
		})
	}
}
