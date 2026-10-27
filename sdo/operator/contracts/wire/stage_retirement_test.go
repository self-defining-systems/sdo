package wire

import (
	"encoding/json"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// A lane's controller restarts on a new build with state the old build wrote.
// The automatic response stage is gone, but the stored documents still carry
// its fields; every durable reader must accept them.
const (
	oldStageReport = `{"stage":"deterministic","outcome":"verified","mitigations":[{"program_id":"p","applied":true}]}`
)

// oldIncidentRequest is the JSON of a valid incident request with the retired
// prior_stages field added, as an earlier build persisted it.
func oldIncidentRequest(t *testing.T) map[string]any {
	t.Helper()
	ref := &contracts.ObjectRef{ApiVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	request := &contracts.IncidentRequest{
		SchemaVersion: "sdo.dev/v1alpha1", IncidentId: "inc-1", Application: "app", Namespace: "app",
		Findings:        []*contracts.Finding{{DetectorId: "health", RuleId: "r", Status: "active", Severity: "warn", Summary: "s", Evidence: "e", PrimaryResource: ref}},
		DetectorHistory: []*contracts.DetectorEvaluation{{DetectorId: "health", EvaluatedAt: timestamppb.New(time.Unix(1, 0)), Status: "firing"}},
		SourceCommit:    "abc", DeployedCommit: "abc", RepositoryBaseCommit: "abc", RepositoryWorktree: "/w",
		ResponseDeadline: timestamppb.New(time.Unix(2, 0)), CancellationToken: "token", RepairPolicy: "commit",
	}
	data, err := Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	var object map[string]any
	if err := json.Unmarshal(data, &object); err != nil {
		t.Fatal(err)
	}
	var stage any
	if err := json.Unmarshal([]byte(oldStageReport), &stage); err != nil {
		t.Fatal(err)
	}
	object["prior_stages"] = []any{stage}
	return object
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestDurableControllerStateDropsRetiredStageReports(t *testing.T) {
	data := mustJSON(t, map[string]any{"version": "sdo.controller/v1", "incident_open": true, "incident_request": oldIncidentRequest(t)})
	if err := Unmarshal(data, new(contracts.ControllerState)); err == nil {
		t.Fatal("plain decode accepted prior_stages; the migration would be untested")
	}
	state := new(contracts.ControllerState)
	if err := UnmarshalDurable(data, state); err != nil {
		t.Fatalf("cannot restore a controller state written before stage deletion: %v", err)
	}
	if state.GetIncidentRequest().GetIncidentId() != "inc-1" || !state.GetIncidentOpen() {
		t.Fatalf("migration lost the incident request: %v", state.GetIncidentRequest())
	}
}

func TestStoredOutcomeMigratesRetiredStageFields(t *testing.T) {
	data := []byte(`{"schema_version":1,"incident_id":"inc-1","classification":"success","resolution_path":"deterministic","prior_stages":[` + oldStageReport + `]}`)
	if err := UnmarshalLenient(data, new(contracts.OutcomeRecord)); err == nil {
		t.Fatal("plain lenient decode accepted resolution_path deterministic; the migration would be untested")
	}
	record := new(contracts.OutcomeRecord)
	if err := UnmarshalLenientMigrating(data, record); err != nil {
		t.Fatalf("cannot read an outcome written before stage deletion: %v", err)
	}
	if record.GetResolutionPath() != "responder" || record.GetIncidentId() != "inc-1" {
		t.Fatalf("outcome not migrated: %v", record)
	}
}

func TestDurableAgentTaskDropsRetiredStageReportsInsideAny(t *testing.T) {
	request := oldIncidentRequest(t)
	request["@type"] = "type.googleapis.com/sdodev.contracts.v1alpha1.IncidentRequest"
	data := mustJSON(t, map[string]any{"schema_version": "sdo.dev/v1alpha1", "task_id": "inc-1", "agent_id": "responder", "task_type": "incident", "request": request})
	if err := Unmarshal(data, new(contracts.AgentTask)); err == nil {
		t.Fatal("plain decode accepted prior_stages; the migration would be untested")
	}
	task := new(contracts.AgentTask)
	if err := UnmarshalDurable(data, task); err != nil {
		t.Fatalf("cannot read a stored agent task written before stage deletion: %v", err)
	}
	typed := new(contracts.IncidentRequest)
	if err := task.GetRequest().UnmarshalTo(typed); err != nil || typed.GetIncidentId() != "inc-1" {
		t.Fatalf("task request lost: %v %v", typed, err)
	}
}

func TestDurableControllerConfigDropsRetiredResponseStages(t *testing.T) {
	data := []byte(`{"namespace":"app","app_root":"/workspace/app","dispatcher_mode":"job","repair_policy":"commit","code_path":{"name":"go","overrides":{"coverage":"go","response_stages":"deterministic"}}}`)
	if err := Unmarshal(data, new(contracts.ControllerConfig)); err == nil {
		t.Fatal("plain decode accepted response_stages; the migration would be untested")
	}
	config := new(contracts.ControllerConfig)
	if err := UnmarshalDurable(data, config); err != nil {
		t.Fatalf("cannot load a config written before stage deletion: %v", err)
	}
	if config.GetCodePath().GetOverrides().GetCoverage() != "go" {
		t.Fatalf("migration lost sibling overrides: %v", config)
	}
}

func TestDurableControllerStateRenamesStoredMatchCounts(t *testing.T) {
	match := `{"playbook_id":"targetport","program_revision":"r","verified_successes":2,"verified_failures":1}`
	coverage := `{"matches":[` + match + `]}`
	request := oldIncidentRequest(t)
	var parsed any
	if err := json.Unmarshal([]byte(coverage), &parsed); err != nil {
		t.Fatal(err)
	}
	request["playbook_coverage"] = parsed
	data := mustJSON(t, map[string]any{"version": "sdo.controller/v1", "incident_open": true, "incident_request": request})
	state := new(contracts.ControllerState)
	if err := UnmarshalDurable(data, state); err != nil {
		t.Fatalf("cannot restore stored match counts: %v", err)
	}
	got := state.GetIncidentRequest().GetPlaybookCoverage().GetMatches()[0]
	if got.GetSuccessfulApplications() != 2 || got.GetFailedApplications() != 1 {
		t.Fatalf("counts not carried to the renamed fields: %v", got)
	}
}
