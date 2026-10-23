package runtime

import (
	"context"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"testing"
)

func TestResponderMenuTypedProgramParameterAndProvenance(t *testing.T) {
	loop, cluster := menuB3Loop(t)
	loop.Request.RepairPolicy = "recorded-actions"
	loop.Executor.Verifier = planFakeVerifier{true}
	listing := loop.Handle(context.Background(), ToolRequest{Command: "menu"})
	option := plainProgramOption(t, listing.Menu, 1)
	found := false
	for _, param := range listing.MenuParameters[option] {
		if param.Name == "port" && param.Kind == "int" && param.Default == 8080 {
			found = true
		}
	}
	if !found {
		t.Fatal("declared scalar schema or default missing")
	}
	response := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: option, RoundLimit: 1}, Parameters: map[string]string{"port": "8082"}})
	if response.Report == nil || response.Report.Status != contracts.MitigationReport_STATUS_SUCCESS {
		t.Fatalf("typed parameter failed: %v", response)
	}
	if len(response.MenuExecution) != 1 || len(response.MenuExecution[0].ActionResults) != 1 {
		t.Fatal("per-action result missing")
	}
	values := response.MenuExecution[0].ActionResults[0].Values
	if len(values) != 1 || values[0].Origin != program.ParameterValue || values[0].Parameter != "port" {
		t.Fatal("value provenance missing")
	}
	ports, _, _ := unstructuredPorts(cluster.objects[menuService].Object)
	if ports[0].(map[string]any)["targetPort"] != float64(8082) {
		t.Fatal("typed override did not reach ordinary executor")
	}
}

func TestResponderMenuRawSetFieldParameterAndObservedProvenance(t *testing.T) {
	for _, observed := range []bool{false, true} {
		t.Run(map[bool]string{false: "parameter", true: "observed"}[observed], func(t *testing.T) {
			loop, cluster := menuB3Loop(t)
			loop.Request.RepairPolicy = "recorded-actions"
			loop.Executor.Verifier = planFakeVerifier{true}
			peer := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "peer"}
			cluster.objects[peer] = cluster.objects[menuService].DeepCopy()
			cluster.objects[peer].SetName("peer")
			_ = program.ApplySetField(cluster.objects[peer].Object, "spec.ports[0].targetPort", int64(8083))
			value := "8083"
			origin := program.ParameterValue
			if observed {
				value = `{"observed":{"ref":"Service/app/peer","path":"spec.ports[0].targetPort"}}`
				origin = program.ObservedValue
			}
			loop.Handle(context.Background(), ToolRequest{Command: "menu"})
			response := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: "SetField", RoundLimit: 1}, Parameters: map[string]string{"ref": "Service/app/backend", "path": "spec.ports[0].targetPort", "value": value, "reason": "synthetic raw action"}})
			if response.Report == nil || response.Report.Status != contracts.MitigationReport_STATUS_SUCCESS {
				t.Fatalf("raw action failed: %v", response)
			}
			values := response.MenuExecution[0].ActionResults[0].Values
			if len(values) != 1 || values[0].Origin != origin {
				t.Fatal("raw value lost provenance")
			}
		})
	}
}

type menuExecClient struct {
	*lifecycleTestClient
	calls int
}

func (c *menuExecClient) Exec(context.Context, sdk.ObjectRef, string, []string) (int, error) {
	c.calls++
	return 0, nil
}

func TestResponderMenuRawExecUsesOrdinaryGuardedClient(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "worker"}
	object := planObject(ref, map[string]any{"containers": []any{map[string]any{"name": "app", "image": "synthetic.invalid/app"}}})
	object.SetUID("pod-uid")
	object.SetResourceVersion("1")
	client := &menuExecClient{lifecycleTestClient: &lifecycleTestClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: object}}}}
	checks := 0
	loop := NewResponderLoop(IncidentRequest{IncidentID: "exec-fixture", Namespace: "app", RepairPolicy: "recorded-actions"}, nil, nil, nil, PlanExecutor{Client: healthGuardedPlanClient{PlanObjectClient: client, Check: func(context.Context) error { checks++; return nil }}, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}, 5)
	loop.MenuConfig = productionResponderMenu()
	loop.Handle(context.Background(), ToolRequest{Command: "menu"})
	response := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: "Exec", RoundLimit: 1}, Parameters: map[string]string{"pod": "Pod/app/worker", "container": "app", "command": `["true"]`}})
	if response.Report == nil || response.Report.Status != contracts.MitigationReport_STATUS_SUCCESS || client.calls != 1 || checks == 0 {
		t.Fatalf("exec capability or guard lost: %v", response)
	}
	results := response.MenuExecution[0].ActionResults
	if len(results) != 1 || results[0].ExitCode == nil || *results[0].ExitCode != 0 || len(results[0].Values) != 1 {
		t.Fatal("exec report omitted exit result or provenance")
	}
}

func TestResponderMenuPlanningErrorReturnsTypedError(t *testing.T) {
	loop, cluster := menuB3Loop(t)
	loop.Request.RepairPolicy = "recorded-actions"
	menu := loop.Handle(context.Background(), ToolRequest{Command: "menu"})
	before := len(cluster.patches)
	response := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: plainProgramOption(t, menu.Menu, 1), RoundLimit: 1}, Parameters: map[string]string{"port": "invalid-int"}})
	if response.Report == nil || response.Report.Status != contracts.MitigationReport_STATUS_EXECUTION_ERROR || response.Report.FailedPhase != "planning" {
		t.Fatal("parameter planning error omitted typed report")
	}
	if len(response.MenuExecution) != 1 || response.MenuExecution[0].Error == "" || len(cluster.patches) != before {
		t.Fatal("planning error lost context or performed a write")
	}
}

func TestResponderMenuReportNamesSelectedOption(t *testing.T) {
	loop, _ := menuB3Loop(t)
	loop.Request.RepairPolicy = "recorded-actions"
	loop.Executor.Verifier = planFakeVerifier{true}
	listing := loop.Handle(context.Background(), ToolRequest{Command: "menu"})
	option := plainProgramOption(t, listing.Menu, 1)
	response := menuCall(t, loop, map[string]any{"command": "mitigate", "selection": map[string]any{"option_id": option, "round_limit": 1}})
	report := response["report"].(map[string]any)
	execution := report["execution"].([]any)[0].(map[string]any)
	if execution["option_id"] != option || execution["program_id"] != "routing-then-restart" {
		t.Fatal("execution report does not say which option ran")
	}
}
