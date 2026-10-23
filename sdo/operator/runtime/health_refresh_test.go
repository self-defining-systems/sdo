package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
)

func TestHealthRefreshFailureBlocksEvaluationRepairAndClosure(t *testing.T) {
	now := time.Now().UTC()
	detector := controllerDetector("health", time.Second, sdk.Finding{RuleID: "obsolete-jaeger", Status: sdk.FindingActive, Severity: sdk.SeverityCritical})
	dispatcher := &recordingDispatcher{}
	c, err := NewController(ControllerConfig{Application: "app", Namespace: "app", FiringThreshold: 1, ClearThreshold: 1, ResponseTimeout: time.Minute}, []sdk.Detector{detector}, staticProvider{}, dispatcher, now)
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.New("health unverified: judge failed")
	c.CheckHealth = func(context.Context) error { return failure }
	if err := c.Step(context.Background(), now, nil); !errors.Is(err, failure) {
		t.Fatalf("stale detector evaluated: %v", err)
	}
	if len(c.history) != 0 {
		t.Fatal("stale findings entered controller history")
	}
	if _, _, err := c.actionContext(context.Background()); !errors.Is(err, failure) {
		t.Fatalf("stale policy authorized effect: %v", err)
	}
}

func TestHealthRefreshBlocksToolsAndPendingEffects(t *testing.T) {
	ctx := context.Background()
	failure := memory.ErrHealthRefreshRequired
	loop := NewResponderLoop(IncidentRequest{IncidentID: "stale"}, nil, staticProvider{}, nil, PlanExecutor{}, 1)
	loop.CheckHealth = func(context.Context) error { return failure }
	for _, command := range []string{"status", "apply"} {
		response := loop.Handle(ctx, ToolRequest{Command: command, PlanIDs: []string{"old-plan"}})
		if response.Error != failure.Error() || response.Healthy || response.LoopsUsed != 0 {
			t.Fatalf("stale tool authorized: %+v", response)
		}
	}
	// There is no backing client: reaching it would panic and expose a write.
	client := healthGuardedPlanClient{Check: loop.CheckHealth}
	if err := client.Patch(ctx, sdk.ObjectRef{}, types.MergePatchType, nil); !errors.Is(err, failure) {
		t.Fatalf("write allowed: %v", err)
	}
	c, err := NewController(testControllerConfig(), nil, staticProvider{}, &recordingDispatcher{}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	c.CheckHealth = loop.CheckHealth
	if err := c.ExecuteDispatchEffect(ctx, DispatchEffect{}); !errors.Is(err, failure) {
		t.Fatalf("dispatch allowed: %v", err)
	}
	if err := c.ExecuteClosureEffect(ctx, ClosureEffect{}); !errors.Is(err, failure) {
		t.Fatalf("closure allowed: %v", err)
	}
	if err := c.ExecuteClosureAcknowledgmentEffect(ctx, ClosureAcknowledgmentEffect{}); !errors.Is(err, failure) {
		t.Fatalf("ack allowed: %v", err)
	}
}

func TestHealthAdoptionDiscardsQueuedObsoleteFindings(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(0, 0).UTC()
	finding := stateFinding("health")
	finding.Severity = sdk.SeverityCritical
	obsolete := controllerDetector("health", time.Minute, finding)
	obsolete.spec.Class, obsolete.spec.Owner = sdk.DetectorClassHealth, sdk.DetectorOwnerHealthJudge
	obsolete.spec.OriginatingCommit = "old-judge"
	obsolete.spec.Persistence = sdk.PersistencePolicy{Firing: 1, Clearing: 1}
	obsolete.spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityCritical, Debounce: time.Minute}
	old, err := NewController(testControllerConfig(), []sdk.Detector{obsolete}, staticProvider{}, &recordingDispatcher{}, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := old.Step(ctx, now, nil); err != nil {
		t.Fatal(err)
	}
	state := exportState(t, old)
	if len(state.GetPendingBatch().GetFindings()) != 1 {
		t.Fatalf("fixture has no queued obsolete finding: %v", state)
	}
	refreshed := controllerDetector("health", time.Minute, sdk.Finding{})
	refreshed.spec = obsolete.spec
	adopted, err := NewController(testControllerConfig(), []sdk.Detector{refreshed}, staticProvider{}, &recordingDispatcher{}, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := adopted.RestoreState(state); err != nil {
		t.Fatal(err)
	}
	adopted.discardRestoredHealthObservations(now.Add(time.Second))
	if err := adopted.Step(ctx, now.Add(time.Second), nil); err != nil {
		t.Fatal(err)
	}
	if adopted.incidentOpen || len(adopted.batcher.Snapshot().Findings) > 0 || adopted.tracker.HasActiveDetector("health") {
		t.Fatal("adoption dispatched an obsolete alarm")
	}
	if refreshed.calls != 1 {
		t.Fatal("adopted health policy was not evaluated immediately")
	}
}
