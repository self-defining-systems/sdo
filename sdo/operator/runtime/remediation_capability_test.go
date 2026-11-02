package runtime

import (
	"context"
	"testing"

	kubefake "k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/sdk/program"
)

func TestMenuHidesUnavailableAPIAndShowsActualBudget(t *testing.T) {
	loop := NewResponderLoop(IncidentRequest{IncidentID: "capabilities"}, nil, nil, nil, PlanExecutor{Client: GenericPlanClient{TypedPlanClient: TypedPlanClient{Client: kubefake.NewSimpleClientset()}}}, 10)
	loop.MenuConfig = productionResponderMenu()
	response := loop.Handle(context.Background(), ToolRequest{Command: "menu"})
	for _, option := range response.Menu.GetOptions() {
		if option.OptionId == "KubernetesRequest" {
			t.Fatal("advertised unavailable API transport")
		}
	}
	if response.LoopsAllowed != 5 {
		t.Fatalf("advertised %d rounds but execution caps at five", response.LoopsAllowed)
	}
}

func TestUnavailableAPIDoesNotClaimDispatchedWrite(t *testing.T) {
	executor := PlanExecutor{Client: TypedPlanClient{Client: kubefake.NewSimpleClientset()}, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
	result, err := executor.Apply(context.Background(), IncidentRequest{}, program.Plan{Actions: []program.Action{program.KubernetesRequest{Method: "DELETE", Path: "/api/v1/namespaces/app/serviceaccounts/cleanup", Reason: "remove injected object"}}}, false)
	if err == nil || result.Applied || len(result.ActionsApplied) > 0 {
		t.Fatalf("missing transport claimed a write: %+v %v", result, err)
	}
}

