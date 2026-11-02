package runtime

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

type menuExecClient struct {
	*lifecycleTestClient
	calls int
}

func (c *menuExecClient) Exec(context.Context, sdk.ObjectRef, string, []string) (int, error) {
	c.calls++
	return 0, nil
}

func TestResponderMenuRawExecUsesOrdinaryClient(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "worker"}
	object := planObject(ref, map[string]any{"containers": []any{map[string]any{"name": "app", "image": "synthetic.invalid/app"}}})
	object.SetUID("pod-uid")
	object.SetResourceVersion("1")
	client := &menuExecClient{lifecycleTestClient: &lifecycleTestClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: object}}}}
	loop := NewResponderLoop(IncidentRequest{IncidentID: "exec-fixture", Namespace: "app", RepairPolicy: "recorded-actions"}, nil, nil, nil, PlanExecutor{Client: client, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}, 5)
	loop.MenuConfig = productionResponderMenu()
	loop.Handle(context.Background(), ToolRequest{Command: "menu"})
	response := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: "Exec", RoundLimit: 1}, Parameters: map[string]string{"pod": "Pod/app/worker", "container": "app", "command": `["true"]`}})
	if response.Report == nil || response.Report.Status != contracts.MitigationReport_STATUS_SUCCESS || client.calls != 1 {
		t.Fatalf("exec capability lost: %v", response)
	}
	results := response.MenuExecution[0].ActionResults
	if len(results) != 1 || results[0].ExitCode == nil || *results[0].ExitCode != 0 || len(results[0].Values) != 1 {
		t.Fatal("exec report omitted exit result or provenance")
	}
}
