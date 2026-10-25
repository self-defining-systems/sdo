package runtime

import (
	"context"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes/fake"
	"path/filepath"
	"sdo.dev/operator/sdk"
	"testing"
	"time"
)

func TestResponderMixedFailuresNoProgress(t *testing.T) {
	client := fake.NewSimpleClientset()
	request := goldenRequest(t)
	request.ResponseDeadline = time.Now().Add(time.Minute)
	request.RepositoryWorktree = filepath.Join(t.TempDir(), "proposal")
	request.MaxResponseLoops = 5
	role := inventoryObject("rbac.authorization.k8s.io/v1", "ClusterRole", "", "cleanup")
	sa := inventoryObject("v1", "ServiceAccount", "app", "cleanup")
	binding := inventoryObject("rbac.authorization.k8s.io/v1", "ClusterRoleBinding", "", "cleanup")
	binding.Object["roleRef"] = map[string]any{"kind": "ClusterRole", "name": "cleanup"}
	binding.Object["subjects"] = []any{map[string]any{"kind": "ServiceAccount", "namespace": "app", "name": "cleanup"}}
	findings := []sdk.Finding{}
	for _, finding := range NewSourceInventory("app", "accepted", nil).Findings([]*unstructured.Unstructured{role, sa, binding}, time.Now()) {
		if finding.PrimaryResource.Kind != "ClusterRoleBinding" {
			findings = append(findings, finding)
		}
	}
	if len(findings) != 2 {
		t.Fatalf("mixed-failure inventory incomplete: %+v", findings)
	}
	// Keep resource ordering explicit for the mixed execution/authorization receipts.
	if findings[0].PrimaryResource.Kind != "ClusterRole" {
		findings[0], findings[1] = findings[1], findings[0]
	}

	loop := NewResponderLoop(request, nil, nil, nil, PlanExecutor{}, 5)
	loop.VerifyResult = func(context.Context, IncidentResult) ToolResponse {
		return ToolResponse{Summary: "remaining unexplained inventory", Findings: findings}
	}
	d := KubernetesJobDispatcher{Client: client, Namespace: "demo", Registry: testIncidentRegistry(t, "responder:test"), AgentID: testIncidentAgentID(t), RepositoryPVC: "repo", RepositoryMountPath: filepath.Dir(request.RepositoryWorktree), ServiceAccount: "responder", PollInterval: time.Millisecond, LoopFactory: func(IncidentRequest) *ResponderLoop { return loop }}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	type reply struct {
		result IncidentResult
		err    error
	}
	done := make(chan reply, 1)
	go func() {
		result, err := d.Dispatch(ctx, request)
		t.Logf("dispatch returned status=%s error=%v resultError=%s", result.Status, err, result.Error)
		done <- reply{result, err}
	}()
	result := completedResult(request.IncidentID)
	result.RepairActions = []RepairActionReceipt{
		{ActionID: "failed-role", Kind: "sdk-plan-delete", Summary: "failed removal", Target: "ClusterRole/cleanup", Details: "execution failed", StartedAt: time.Now(), CompletedAt: time.Now(), Resources: []sdk.ObjectRef{findings[0].PrimaryResource}, FailureReason: "execution_failed"},
		{ActionID: "denied-sa", Kind: "sdk-plan-delete", Summary: "denied removal", Target: "ServiceAccount/app/cleanup", Details: "authorization denied", StartedAt: time.Now(), CompletedAt: time.Now(), Resources: []sdk.ObjectRef{findings[1].PrimaryResource}, FailureReason: "authorization_denied"},
	}
	first := awaitJob(t, client, IncidentJobName(request.IncidentID))
	publishContinuationResult(t, client, first, result)
	second := awaitJob(t, client, IncidentJobName(request.IncidentID+"-continue-1"))
	publishContinuationResult(t, client, second, result)
	outcome := <-done
	if outcome.err != nil || outcome.result.Status != IncidentFailed || outcome.result.Error != "no_progress" || outcome.result.ResponseLoopsUsed != 2 || outcome.result.ResponseLoopExhausted {
		t.Fatalf("mixed failures did not stop after one unchanged continuation: %+v error=%v", outcome.result, outcome.err)
	}
	resumed, err := d.ContinueAfterVerification(context.Background(), request, ToolResponse{Summary: "remaining unexplained inventory", Findings: findings}, time.Now())
	if err != nil || resumed {
		t.Fatalf("terminal no progress resumed: %v %v", resumed, err)
	}
}

func TestResponderNoProgressRequiresUnchangedFindingsAndSuccesses(t *testing.T) {
	finding := noProgressInventory(t)
	verification := ToolResponse{Findings: []sdk.Finding{finding}}
	failed := noProgressFailure(finding)
	previous, err := continuationFeedback(1, verification, failed)
	if err != nil {
		t.Fatal(err)
	}
	if !responderNoProgress(string(previous), verification, []continuationRepair{failed}) {
		t.Fatal("unchanged known failures prevented terminal no progress")
	}
	newSuccess := failed
	newSuccess.Receipt.Success = true
	newSuccess.ActionID = "second"
	if responderNoProgress(string(previous), verification, []continuationRepair{failed, newSuccess}) {
		t.Fatal("new successful mutation was ignored")
	}
	changed := ToolResponse{Findings: []sdk.Finding{{Fingerprint: "changed"}}}
	if responderNoProgress(string(previous), changed, nil) {
		t.Fatal("changed still-broken set considered unchanged")
	}
	if responderNoProgress("", verification, nil) || responderNoProgress(string(previous), ToolResponse{}, nil) {
		t.Fatal("first response or pending health observation ended early")
	}
	verification.Error = "inventory read unavailable"
	if responderNoProgress(string(previous), verification, nil) {
		t.Fatal("unavailable verification ended incident")
	}
}
