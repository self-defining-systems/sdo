package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func TestPendingDetectorStatusPersistsThroughControllerState(t *testing.T) {
	evaluations := []DetectorEvaluation{{DetectorID: "service-reachability", EvaluatedAt: time.Unix(100, 0), Status: DetectorEvaluationPending, Reason: "no synthetic scenario has succeeded since observation began"}}
	store := lifecycleio.NewConfigMapStateStore(fake.NewSimpleClientset(), "control", "sdo-controller-state")
	request := nativeClosure(IncidentWorkspace{IncidentID: "pending-incident", Worktree: "/proposal", BaseCommit: "base"}).Request
	request.DetectorHistory = evaluations
	typedRequest, err := incidentRequestContract(request)
	if err != nil {
		t.Fatal(err)
	}
	state := &contracts.ControllerState{Version: RuntimeStateVersion, History: evaluationsContract(evaluations), IncidentRequest: typedRequest}
	if _, err := store.Save(context.Background(), state, ""); err != nil {
		t.Fatalf("persist unconfirmed detector: %v", err)
	}
	restored, _, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got := evaluationsState(restored.History)
	if len(got) != 1 || got[0].Status != DetectorEvaluationPending || restored.IncidentRequest.DetectorHistory[0].Status != "pending" || got[0].Reason != evaluations[0].Reason || restored.IncidentRequest.DetectorHistory[0].GetReason() != evaluations[0].Reason {
		t.Fatalf("pending changed on restart: %+v", got)
	}
}

// A detector may emit no finding while its probe window remains incomplete.
type unconfirmedDetector struct{ sdk.Detector }

func (unconfirmedDetector) Confirmed(sdk.DetectionContext) bool { return false }

func TestPendingDetectorNeverEstablishesHealth(t *testing.T) {
	ctx := context.Background()
	detector := unconfirmedDetector{inventoryHealth()}
	provider := ReadSnapshotProvider{Base: staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}, View: func(snapshot sdk.DetectionContext) program.ReadView { return program.ReadFrom(snapshot) }}
	t.Run("detect", func(t *testing.T) {
		loop := NewResponderLoop(IncidentRequest{}, nil, provider, []sdk.Detector{detector}, PlanExecutor{}, 3)
		response := loop.Handle(ctx, ToolRequest{Command: "detect"})
		if len(response.DetectorStates) != 1 || response.DetectorStates[0].Status != DetectorEvaluationPending || response.Healthy || len(response.Findings) != 0 {
			t.Fatalf("unconfirmed detect: %+v", response)
		}
	})
	t.Run("verify", func(t *testing.T) {
		loop := NewResponderLoop(IncidentRequest{}, nil, provider, []sdk.Detector{detector}, PlanExecutor{}, 3)
		loop.ReadView = func(snapshot sdk.DetectionContext) program.ReadView { return program.ReadFrom(snapshot) }
		response := loop.Handle(ctx, ToolRequest{Command: "verify"})
		if response.Healthy || len(response.DetectorStates) != 1 || response.DetectorStates[0].Status != DetectorEvaluationPending {
			t.Fatalf("unconfirmed verification: %+v", response)
		}
	})
	for _, timeout := range []time.Duration{0, 5 * time.Millisecond} {
		t.Run("plan-"+timeout.String(), func(t *testing.T) {
			healthy, err := (PlanHealthVerifier{Detectors: []sdk.Detector{detector}, Provider: provider, Timeout: timeout, PollInterval: time.Millisecond}).Verify(ctx)
			if err != nil || healthy {
				t.Fatalf("unconfirmed plan health: %v %v", healthy, err)
			}
		})
	}
	t.Run("mitigation", func(t *testing.T) {
		verifier := scopedMitigationVerifier(PlanHealthVerifier{Detectors: []sdk.Detector{detector}, Provider: provider}, program.Plan{}, nil)
		healthy, err := verifier.Verify(ctx)
		if err != nil || healthy || verifier.observation.Healthy {
			t.Fatalf("unconfirmed scoped health: %v %v %+v", healthy, err, verifier.observation)
		}
	})
	t.Run("evaluate-once", func(t *testing.T) {
		var stdout bytes.Buffer
		if err := RunWithOptions(ctx, []sdk.Detector{detector}, RuntimeOptions{Args: []string{"--evaluate-once", "--namespace", "app"}, Stdout: &stdout, Provider: provider}); err != nil {
			t.Fatal(err)
		}
		var output struct {
			Status string `json:"status"`
		}
		if err := json.Unmarshal(stdout.Bytes(), &output); err != nil {
			t.Fatal(err)
		}
		if output.Status != "pending" {
			t.Fatalf("unconfirmed read-only evaluation: %s", stdout.String())
		}
		if err := RunWithOptions(ctx, []sdk.Detector{detector}, RuntimeOptions{Args: []string{"--evaluate-once", "--namespace", "app", "--accept-seed"}, Stdout: &stdout, Provider: provider}); err == nil {
			t.Fatal("pending seed accepted")
		}
	})
}
