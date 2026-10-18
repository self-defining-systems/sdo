package worker_registry

import (
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/types/known/anypb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func TestLoadDirectoryRejectsUnknownManifestFields(t *testing.T) {
	dir := t.TempDir()
	data := `{"schema_version":"sdo.dev/v1alpha1","agent_id":"worker","capabilities":[{"task_type":"incident-response","request_type_url":"x","result_type_url":"y"}],"local_process":{"command":["/worker"]},"agent_language":"python"}`
	if err := os.WriteFile(filepath.Join(dir, "worker.json"), []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadDirectory(dir); err == nil {
		t.Fatal("unknown field accepted")
	}
}

func TestRegistryResolvesWorkerByCapability(t *testing.T) {
	reg, err := New(&contracts.AgentRegistration{
		SchemaVersion: "sdo.dev/v1alpha1",
		AgentId:       "external-responder",
		Capabilities: []*contracts.AgentCapability{{
			TaskType:       "incident-response",
			RequestTypeUrl: "type.googleapis.com/sdodev.contracts.v1alpha1.IncidentRequest",
			ResultTypeUrl:  "type.googleapis.com/sdodev.contracts.v1alpha1.IncidentResult",
		}},
		Launch: &contracts.AgentRegistration_KubernetesJob{KubernetesJob: &contracts.KubernetesJobLaunch{
			Image: "example/worker:1", Command: []string{"/worker"},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := reg.Resolve("external-responder", "incident-response"); err != nil || got.GetAgentId() != "external-responder" {
		t.Fatalf("Resolve = %v, %v", got, err)
	}
	if _, err := reg.Resolve("external-responder", "source-deployment"); err == nil {
		t.Fatal("worker must not be selected for an undeclared task type")
	}
}

func TestRegistryChecksTypedEnvelope(t *testing.T) {
	capability := &contracts.AgentCapability{TaskType: "incident-response", RequestTypeUrl: "type.googleapis.com/sdodev.contracts.v1alpha1.IncidentRequest", ResultTypeUrl: "type.googleapis.com/sdodev.contracts.v1alpha1.IncidentResult"}
	reg, err := New(&contracts.AgentRegistration{
		SchemaVersion: "sdo.dev/v1alpha1", AgentId: "worker", Capabilities: []*contracts.AgentCapability{capability},
		Launch: &contracts.AgentRegistration_LocalProcess{LocalProcess: &contracts.LocalProcessLaunch{Command: []string{"/worker"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	task := &contracts.AgentTask{SchemaVersion: "sdo.dev/v1alpha1", TaskId: "t-1", AgentId: "worker", TaskType: "incident-response", Request: &anypb.Any{TypeUrl: capability.RequestTypeUrl}}
	if err := reg.ValidateTask(task); err != nil {
		t.Fatal(err)
	}
	task.Request.TypeUrl = "type.googleapis.com/unknown.Request"
	if err := reg.ValidateTask(task); err == nil {
		t.Fatal("unregistered request type accepted")
	}
	task.Request.TypeUrl = capability.RequestTypeUrl
	result := &contracts.AgentResult{SchemaVersion: "sdo.dev/v1alpha1", TaskId: "t-1", Status: "completed", Response: &anypb.Any{TypeUrl: capability.ResultTypeUrl}}
	if err := reg.ValidateResult(task, result); err != nil {
		t.Fatal(err)
	}
	result.Response.TypeUrl = "type.googleapis.com/unknown.Result"
	if err := reg.ValidateResult(task, result); err == nil {
		t.Fatal("unregistered result type accepted")
	}
}

func TestRegistryRejectsDuplicateAndMalformedRegistrations(t *testing.T) {
	valid := &contracts.AgentRegistration{
		SchemaVersion: "sdo.dev/v1alpha1", AgentId: "worker",
		Capabilities: []*contracts.AgentCapability{{TaskType: "incident-response", RequestTypeUrl: "x", ResultTypeUrl: "y"}},
		Launch:       &contracts.AgentRegistration_LocalProcess{LocalProcess: &contracts.LocalProcessLaunch{Command: []string{"/worker"}}},
	}
	if _, err := New(valid, valid); err == nil {
		t.Fatal("duplicate ID accepted")
	}
	if _, err := New(&contracts.AgentRegistration{AgentId: "empty"}); err == nil {
		t.Fatal("missing capability/launch accepted")
	}
}

func TestBuiltInAgentManifestsRegisterIndependentCapabilities(t *testing.T) {
	registry, err := LoadDirectory(filepath.Join("..", "..", "agents", "registrations"))
	if err != nil {
		t.Fatal(err)
	}
	for _, choice := range []struct{ id, task string }{
		{"sdo-deployer", "source-deployment"},
		{"sdo-deployer", "source-architecture"},
		{"sdo-health-judge", "health-judgment"},
		{"sdo-responder", "incident-response"},
		{"sdo-reflector", "outcome-reflection"},
	} {
		if _, err := registry.Resolve(choice.id, choice.task); err != nil {
			t.Errorf("%s/%s: %v", choice.id, choice.task, err)
		}
	}
	if _, err := registry.Resolve("sdo-health-judge", "incident-response"); err == nil {
		t.Fatal("judge obtained responder capability")
	}
}
