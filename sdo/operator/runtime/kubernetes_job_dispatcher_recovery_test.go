package runtime

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type toolStateRestorer interface {
	restoreToolState(context.Context, IncidentRequest, *ResponderLoop) error
}

func attachRecoveryStore(t *testing.T, d KubernetesJobDispatcher, loop *ResponderLoop) {
	t.Helper()
	restorer, ok := any(d).(toolStateRestorer)
	if !ok {
		t.Fatal("dispatcher has no durable responder tool state")
	}
	if err := restorer.restoreToolState(context.Background(), loop.Request, loop); err != nil {
		t.Fatal(err)
	}
}
func recoveryLoop(t *testing.T, client PlanObjectClient) *ResponderLoop {
	t.Helper()
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	p := loopProgram{candidateProgram: candidateProgram{spec: program.ProgramSpec{ID: "targetport"}, run: func(context.Context, program.Bindings) program.Verdict {
		return program.Verdict{Result: program.NoMatch}
	}}, ref: ref}
	registry, err := program.NewRegistry([]program.Diagnoser{p}, []program.Mitigator{p})
	if err != nil {
		t.Fatal(err)
	}
	loop := NewResponderLoop(IncidentRequest{IncidentID: "recovery", Namespace: "app", RepairPolicy: "recorded-actions"}, registry, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}, []sdk.Detector{loopDetector{}}, PlanExecutor{Client: client, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}}, 2)
	loop.ReadView = func(sdk.DetectionContext) program.ReadView { return &sdktest.ReplayReadView{} }
	return loop
}

func TestDispatcherRestoresFailureSignalsAndDeniesBudgetReset(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: planObject(ref, map[string]any{"ports": []any{map[string]any{"targetPort": float64(9999)}}})}}
	dispatcher := KubernetesJobDispatcher{Client: fake.NewSimpleClientset(), Namespace: "operator"}
	loop := recoveryLoop(t, client)
	loop.MaxBatches = 1
	attachRecoveryStore(t, dispatcher, loop)
	planned := loop.Handle(context.Background(), ToolRequest{Command: "plan", ProgramID: "targetport"})
	applied := loop.Handle(context.Background(), ToolRequest{Command: "apply", PlanIDs: []string{planned.PlanID}})
	if applied.Error != "" {
		t.Fatal(applied.Error)
	}
	loop.mu.Lock()
	loop.addFailureSignal("fast_path_failure:targetport@revision-a")
	if err := loop.persistToolState(context.Background()); err != nil {
		t.Fatal(err)
	}
	loop.mu.Unlock()
	restarted := recoveryLoop(t, client)
	restarted.MaxBatches = 9
	attachRecoveryStore(t, dispatcher, restarted)
	if len(restarted.FailureSignals()) != 1 {
		t.Fatalf("lost signals: %v", restarted.FailureSignals())
	}
	next := restarted.Handle(context.Background(), ToolRequest{Command: "plan", ProgramID: "targetport"})
	denied := restarted.Handle(context.Background(), ToolRequest{Command: "apply", PlanIDs: []string{next.PlanID}})
	if denied.Error != "response loop budget exhausted" {
		t.Fatalf("budget reset on restart: %+v", denied)
	}
}
