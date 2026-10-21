package runtime

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
)

type fastloopDispatch struct {
	requests chan IncidentRequest
	results  chan IncidentResult
}

func (d fastloopDispatch) Dispatch(ctx context.Context, request IncidentRequest) (IncidentResult, error) {
	select {
	case d.requests <- request:
	case <-ctx.Done():
		return IncidentResult{}, ctx.Err()
	}
	select {
	case result := <-d.results:
		return result, nil
	case <-ctx.Done():
		return IncidentResult{}, ctx.Err()
	}
}

func TestHermeticControllerClosureReachesNativeOutcomeBroker(t *testing.T) {
	ctx := context.Background()
	client := fastloopClient()
	accepted := nativeRepository(t)
	repository, err := broker.New(accepted, filepath.Join(t.TempDir(), "proposals"), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	native := NativeIncidentBroker{Repository: repository, LedgerRoot: filepath.Join(t.TempDir(), "ledger"), Deriver: ProtoOutcomeDeriver{}, ResponderBackend: "scripted", ResponderModel: "none",
		Reflector: ReflectionWorkerFunc(func(context.Context, IncidentClosure, IncidentWorkspace, *contracts.OutcomeRecord, string, string, ReflectionFeedback) (ReflectionDecision, error) {
			return ReflectionDecision{LearningDecision: "no_change", NoChangeReason: "fixture program already covers this class"}, nil
		})}
	dispatcher := fastloopDispatch{requests: make(chan IncidentRequest, 1), results: make(chan IncidentResult, 1)}
	config := ControllerConfig{Application: "app", Namespace: "app", SourceCommit: nativeGit(t, accepted, "rev-parse", "HEAD"), DeployedCommit: nativeGit(t, accepted, "rev-parse", "HEAD"), RepositoryWorktree: accepted,
		CorpusSessionRoot: t.TempDir(), ResponseTimeout: time.Minute, VerificationTimeout: time.Minute, FiringThreshold: 1, ClearThreshold: 1, RepairPolicy: "recorded-actions"}
	start := time.Now().UTC()
	controller, err := NewController(config, []sdk.Detector{fastloopDetector{}}, fastloopObjects{client: client}, dispatcher, start)
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.SetIncidentBroker(native); err != nil {
		t.Fatal(err)
	}
	closed := make(chan IncidentClosure, 1)
	controller.OnIncidentClosed = func(closure IncidentClosure) { closed <- closure }
	if err := controller.Step(ctx, start, nil); err != nil {
		t.Fatal(err)
	}
	workspaceEffect, ok := controller.PendingWorkspaceEffect()
	if !ok {
		t.Fatalf("controller did not prepare the broker workspace: %+v", controller.ExportState())
	}
	if err := controller.ExecuteWorkspaceEffect(ctx, workspaceEffect); err != nil {
		t.Fatal(err)
	}
	awaitBrokerResult(t, controller.workspaceResults)
	if err := controller.Step(ctx, start.Add(10*time.Millisecond), nil); err != nil {
		t.Fatal(err)
	}
	executePendingEffect(t, controller)
	request := awaitRequest(t, dispatcher.requests)
	if request.RepositoryWorktree == accepted || request.IncidentID != workspaceEffect.IncidentID {
		t.Fatalf("dispatch did not use broker checkout: %+v", request)
	}
	for _, item := range []struct {
		ref   sdk.ObjectRef
		patch string
	}{
		{sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}, `{"spec":{"ports":[{"port":80,"targetPort":8080}]}}`},
		{sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}, `{"spec":{"template":{"spec":{"containers":[{"name":"api","image":"good:v1","ports":[{"containerPort":8080}]}]}}}}`},
	} {
		if err := (TypedPlanClient{Client: client}).Patch(ctx, item.ref, types.MergePatchType, []byte(item.patch)); err != nil {
			t.Fatal(err)
		}
	}
	result := completedResult(request.IncidentID)
	result.Timing.StartedAt = start.Add(100 * time.Millisecond)
	result.Timing.CompletedAt = start.Add(200 * time.Millisecond)
	result.ConfirmedRootCauses = []ConfirmedRootCause{{Summary: "Service port drift and image pull failure", Resources: []sdk.ObjectRef{{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}, {APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}}, Evidence: []RootCauseEvidence{{Kind: "live-observation", Source: "fake Kubernetes API", Observation: "port and image returned to their source values"}}, ExplainedDetectors: []string{"availability"}}}
	result.RepairActions = []RepairActionReceipt{{ActionID: "repair", Kind: "kubectl", Target: "app", Summary: "restored port and image", Details: "applied source values", Resources: result.ConfirmedRootCauses[0].Resources, StartedAt: result.Timing.StartedAt, CompletedAt: result.Timing.CompletedAt, Success: true}}
	result.VerificationEvidence = []VerificationEvidence{{Name: "scripted verification", Passed: true, Details: "detectors clear", ObservedAt: result.Timing.CompletedAt}}
	dispatcher.results <- result
	awaitDispatchCompletionQueued(t, controller)
	if err := controller.Step(ctx, start.Add(time.Second), nil); err != nil {
		t.Fatal(err)
	}
	var closure IncidentClosure
	select {
	case closure = <-closed:
	case <-time.After(time.Second):
		t.Fatal("controller did not close after verified repair")
	}
	if closure.Request.IncidentID != request.IncidentID || closure.HealthClearedAt == nil {
		t.Fatalf("closure lost controller evidence: %+v", closure)
	}
	receipt, err := native.ProcessClosure(ctx, closure)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.OutcomeCommit == "" {
		t.Fatal("native broker did not commit the outcome")
	}
	stored, err := memory.OpenReader(accepted).Get(ctx, request.IncidentID)
	if err != nil || stored.Record.GetClassification() != "success" {
		t.Fatalf("outcome was not derived from verified closure: %+v %v", stored.Record, err)
	}
	if err := native.AcknowledgeClosure(ctx, receipt); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(request.RepositoryWorktree); !os.IsNotExist(err) {
		t.Fatalf("acknowledged proposal checkout remains: %v", err)
	}
	if _, err := client.CoreV1().Services("app").Get(ctx, "api", metav1.GetOptions{}); err != nil {
		t.Fatal(err)
	}
}
