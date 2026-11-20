package worker_registry

import (
	"fmt"
	"testing"

	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
)

func mapTask(t *testing.T) *contracts.AgentTask {
	fields := map[string]any{}
	bindings := map[string]*contracts.ObjectRef{}
	for i := 0; i < 40; i++ {
		fields[fmt.Sprintf("k%02d", i)] = "v"
		bindings[fmt.Sprintf("b%02d", i)] = &contracts.ObjectRef{Name: "x"}
	}
	meta, err := structpb.NewStruct(fields)
	if err != nil {
		t.Fatal(err)
	}
	request, err := anypb.New(&contracts.IncidentRequest{Findings: []*contracts.Finding{{Metadata: meta, ParameterBindings: bindings}}})
	if err != nil {
		t.Fatal(err)
	}
	return &contracts.AgentTask{SchemaVersion: "sdo.dev/v1alpha1", TaskId: "t", AgentId: "a", TaskType: "x", Request: request}
}

// Preparing and validating the same task twice must agree, however the maps
// inside its Any request were ordered when each copy was encoded.
func TestVerifyPreparedTaskIgnoresMapOrderInsideRequest(t *testing.T) {
	for attempt := 0; attempt < 20; attempt++ {
		prepared, validated := mapTask(t), mapTask(t)
		payload, err := wire.Canonical(prepared)
		if err != nil {
			t.Fatal(err)
		}
		if err := VerifyPreparedTask(string(payload), validated); err != nil {
			t.Fatalf("attempt %d: %v", attempt, err)
		}
	}
}

func TestVerifyPreparedTaskRejectsDifferentTask(t *testing.T) {
	task := &contracts.AgentTask{SchemaVersion: "sdo.dev/v1alpha1", TaskId: "t", AgentId: "a", TaskType: "x"}
	payload, _ := wire.Canonical(task)
	other := &contracts.AgentTask{SchemaVersion: "sdo.dev/v1alpha1", TaskId: "u", AgentId: "a", TaskType: "x"}
	if err := VerifyPreparedTask(string(payload), other); err == nil {
		t.Fatal("different task accepted")
	}
}
