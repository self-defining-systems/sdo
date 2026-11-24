package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func TestPreApplyCaptureKeepsWriteBoundaryAcrossRestart(t *testing.T) {
	ctx := t.Context()
	root := t.TempDir()
	request := IncidentRequest{IncidentID: "synthetic", Namespace: "app"}
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "worker"}
	snapshot := func(replicas int32) sdktest.Snapshot {
		return sdktest.Snapshot{NamespaceName: "app", DeploymentList: []appsv1.Deployment{{
			ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: ref.Name},
			Spec:       appsv1.DeploymentSpec{Replicas: &replicas},
		}}}
	}
	if err := memory.CaptureIncidentSnapshot(root, request.IncidentID, "open@detect", snapshot(1), request.Namespace); err != nil {
		t.Fatal(err)
	}
	dir, err := memory.IncidentCorpusDir(root, request.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	// A process died after recording observations but before publishing the
	// snapshot. Its orphan recordings cannot describe the retry's snapshot.
	recordingsPath := filepath.Join(dir, "open@pre-apply-recordings.json")
	if err := os.WriteFile(recordingsPath, []byte(`[{"method":"old-attempt"}]`), 0600); err != nil {
		t.Fatal(err)
	}
	newLoop := func(replicas int32) (*ResponderLoop, *countingProvider) {
		provider := &countingProvider{SnapshotProvider: preApplyTestProvider{snapshot(replicas)}}
		loop := NewResponderLoop(request, nil, provider, nil, PlanExecutor{}, 3)
		loop.CorpusSessionRoot = root
		loop.ReadView = func(view sdk.DetectionContext) program.ReadView { return program.ReadFrom(view) }
		return loop, provider
	}
	loop, provider := newLoop(2)
	if err := loop.capturePreApply(ctx, ref); err != nil {
		t.Fatal(err)
	}
	if got := provider.snapshots.Load(); got != 1 {
		t.Errorf("snapshots before first write = %d, want 1", got)
	}
	// No controller write occurred before the restart. The committed
	// capture is immutable, even if the workload changes again.
	restarted, provider := newLoop(3)
	if err := restarted.capturePreApply(ctx, ref); err != nil {
		t.Fatal(err)
	}
	if got := provider.snapshots.Load(); got != 1 {
		t.Errorf("snapshots after restart = %d, want only the supplemental comparison read", got)
	}
	read := func(path string, target any) {
		t.Helper()
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(raw, target); err != nil {
			t.Fatal(err)
		}
	}
	var before sdktest.Snapshot
	read(filepath.Join(dir, "open@pre-apply.json"), &before)
	if deployments := before.Deployments(); len(deployments) != 1 || *deployments[0].Spec.Replicas != 2 {
		t.Fatalf("captured deployments = %v, want 2 replicas observed at apply", deployments)
	}
	var calls []sdktest.PrimitiveCall
	read(recordingsPath, &calls)
	haveDiff := false
	for _, call := range calls {
		if call.Method == "old-attempt" {
			t.Error("orphan observations survived the retry")
		}
		haveDiff = haveDiff || call.Method == "DiffAgainstSource"
	}
	if !haveDiff {
		t.Error("source comparison not durable with the before-image")
	}
}

type preApplyTestProvider struct{ view sdktest.Snapshot }

func (p preApplyTestProvider) Snapshot(context.Context) (sdk.DetectionContext, error) {
	return p.view, nil
}

// Kill the attempt at its journal boundary, after durable capture and before
// executor entry. The restarted loop must apply once and retain that capture.
func TestPreApplyCaptureSurvivesStoppingBeforeWriteIntent(t *testing.T) {
	ctx := t.Context()
	world, stage, state := keyedStageLoop(t, credits("fix", keyedSymptom, "api"))
	request := state.Request
	root := t.TempDir()
	first := stage.NewLoop(request)
	first.Now = func() time.Time { return time.Unix(1000, 0) }
	first.Executor.Now = first.Now
	first.CorpusSessionRoot = root
	bindings := program.Bindings{Objects: map[string]sdk.ObjectRef{ComponentRole: keyedDeployment("api")}}
	apply := func(loop *ResponderLoop, afterPlan func()) ToolResponse {
		t.Helper()
		plan := loop.Handle(ctx, ToolRequest{Command: "plan", ProgramID: "fix", Bindings: bindings, stage: true})
		if plan.Error != "" || plan.PlanID == "" {
			t.Fatalf("plan failed: %+v", plan)
		}
		if afterPlan != nil {
			afterPlan()
		}
		return loop.Handle(ctx, ToolRequest{Command: "apply", PlanIDs: []string{plan.PlanID}, stage: true})
	}
	stop := func() {
		first.saveToolState = func(context.Context, []byte) error { return errors.New("synthetic stop before write intent") }
	}
	if response := apply(first, stop); !strings.Contains(response.Error, "synthetic stop before write intent") || world.restarts.Load() != 0 {
		t.Fatalf("first attempt wrote or did not stop at the journal: writes=%d response=%+v", world.restarts.Load(), response)
	}
	dir, err := memory.IncidentCorpusDir(root, request.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "open@pre-apply.json")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	second := NewResponderLoop(request, first.Programs, world, first.Detectors, first.Executor, 5)
	second.CorpusSessionRoot, second.ReadView, second.Now = root, first.ReadView, first.Now
	apply(second, nil)
	if world.restarts.Load() != 1 {
		t.Fatalf("write count across restart = %d, want 1", world.restarts.Load())
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("restart replaced the first-write capture: %v", err)
	}
	var captured sdktest.Snapshot
	if err := json.Unmarshal(after, &captured); err != nil {
		t.Fatal(err)
	}
	for _, deployment := range captured.Deployments() {
		if deployment.Name == bindings.Objects[ComponentRole].Name {
			if deployment.Spec.Template.Annotations["kubectl.kubernetes.io/restartedAt"] != "" {
				t.Fatal("first-write capture contains the repair")
			}
			return
		}
	}
	t.Fatal("the plan's target is missing from the first-write capture")
}
