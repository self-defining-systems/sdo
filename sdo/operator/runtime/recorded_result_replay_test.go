package runtime

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/worker_registry"
)

// Each fixture is the agent-result.json value from a real worker result
// ConfigMap. Free text is redacted while the typed envelope is preserved.
func TestRecordedWorkerResultsReplayThroughContracts(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("testdata", "recorded_results", "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatal("recorded worker results are missing")
	}
	counts := map[string]int{}
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			wire, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(wire), ".sdo/") || strings.Contains(string(wire), "/sdo/session/") {
				t.Fatal("physical path in redacted fixture")
			}
			result := &contracts.AgentResult{}
			if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(wire, result); err != nil {
				t.Fatal(err)
			}
			var agentID, taskType string
			var request *anypb.Any
			switch result.GetResponse().GetTypeUrl() {
			case "type.googleapis.com/sdodev.contracts.v1alpha1.IncidentResult":
				agentID, taskType = "sdo-responder", "incident-response"
				request, err = anypb.New(&contracts.IncidentRequest{})
			case "type.googleapis.com/sdodev.contracts.v1alpha1.ReflectionResult":
				agentID, taskType = "sdo-reflector", "outcome-reflection"
				request, err = anypb.New(&contracts.ReflectionRequest{})
			default:
				t.Fatalf("unexpected response type %q", result.GetResponse().GetTypeUrl())
			}
			if err != nil {
				t.Fatal(err)
			}
			registry, err := worker_registry.LoadDirectory(filepath.Join("..", "..", "agents", "registrations"))
			if err != nil {
				t.Fatal(err)
			}
			task := &contracts.AgentTask{SchemaVersion: ProtocolSchemaVersion, TaskId: result.GetTaskId(), AgentId: agentID, TaskType: taskType, Request: request, Workspace: &contracts.ProposalWorkspace{ProposalId: result.GetProposalId()}}
			if err := registry.ValidateResult(task, result); err != nil {
				t.Fatal(err)
			}
			counts[taskType]++
			if taskType == "incident-response" {
				decoded, err := decodeIncidentAgentResult(wire, task, registry)
				if err != nil {
					t.Fatal(err)
				}
				if err := decoded.ValidateFor(IncidentRequest{IncidentID: decoded.IncidentID}); err != nil {
					t.Fatal(err)
				}
				return
			}
			reflection := &contracts.ReflectionResult{}
			if err := result.GetResponse().UnmarshalTo(reflection); err != nil {
				t.Fatal(err)
			}
			// The registered worker applies the same decision validation before the
			// native broker may commit any reflection proposal.
			workspace := IncidentWorkspace{IncidentID: "replay", Worktree: t.TempDir(), BaseCommit: "base"}
			worker := RegisteredReflectionWorker{AgentID: agentID, Outcomes: memory.NewJSONStore(t.TempDir()), Timeout: time.Second, SessionMode: "fresh"}
			worker.Executor = reflectionTaskExecutorFunc(func(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
				return &contracts.AgentResult{SchemaVersion: task.GetSchemaVersion(), TaskId: task.GetTaskId(), ProposalId: task.GetWorkspace().GetProposalId(), Status: result.GetStatus(), Response: result.GetResponse()}, nil
			})
			decision, err := worker.Reflect(context.Background(), nativeClosure(workspace), workspace, &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: workspace.IncidentID}, "repair", "outcome")
			if err != nil {
				t.Fatal(err)
			}
			if decision.LearningDecision != reflection.GetLearningDecision() {
				t.Fatalf("decision changed: %+v", decision)
			}
		})
	}
	if counts["incident-response"] == 0 || counts["outcome-reflection"] == 0 {
		t.Fatalf("fixture set must include both worker roles: %v", counts)
	}
}

func TestRecordedIncidentAndReflectionReachNativeBroker(t *testing.T) {
	read := func(name string) *contracts.AgentResult {
		t.Helper()
		wire, err := os.ReadFile(filepath.Join("testdata", "recorded_results", name))
		if err != nil {
			t.Fatal(err)
		}
		result := &contracts.AgentResult{}
		if err := protojson.Unmarshal(wire, result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	responder := read("evidence_smoke4-responder-1.json")
	reflection := read("evidence_smoke4-reflection-1.json")
	typed := &contracts.IncidentResult{}
	if err := responder.GetResponse().UnmarshalTo(typed); err != nil {
		t.Fatal(err)
	}
	result := incidentResultState(typed)
	accepted := nativeRepository(t)
	repository, err := broker.New(accepted, filepath.Join(t.TempDir(), "proposals"), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	reflector := RegisteredReflectionWorker{AgentID: "sdo-reflector", Outcomes: memory.OpenReader(accepted), Timeout: time.Second, SessionMode: "fresh"}
	reflector.Executor = reflectionTaskExecutorFunc(func(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
		return &contracts.AgentResult{SchemaVersion: task.GetSchemaVersion(), TaskId: task.GetTaskId(), ProposalId: task.GetWorkspace().GetProposalId(), Status: "completed", Response: reflection.GetResponse()}, nil
	})
	native := NativeIncidentBroker{Repository: repository, LedgerRoot: filepath.Join(t.TempDir(), "ledger"), Deriver: ProtoOutcomeDeriver{}, Reflector: reflector, ResponderBackend: "recorded", ResponderModel: "none"}
	workspace, err := native.PrepareIncident(context.Background(), result.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	closure := nativeClosure(workspace)
	closure.Request.Namespace = "hotel-reservation"
	closure.Request.RepairPolicy = "recorded-actions"
	closure.Request.Findings = []sdk.Finding{{DetectorID: "health-objective", RuleID: "availability", Status: sdk.FindingActive, Severity: sdk.SeverityCritical, PrimaryResource: sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "hotel-reservation", Name: "reservation"}}}
	closure.Request.DetectorHistory = []DetectorEvaluation{{DetectorID: "health-objective", EvaluatedAt: result.Timing.StartedAt, Status: DetectorEvaluationFiring}}
	closure.Result = &result
	closure.DetectedAt = result.Timing.StartedAt.Add(-time.Second)
	closure.DispatchedAt = result.Timing.StartedAt
	closure.ResponderCompletedAt = result.Timing.CompletedAt
	closure.VerifiedAt = result.Timing.CompletedAt.Add(time.Second)
	cleared := result.Timing.CompletedAt
	closure.HealthClearedAt = &cleared
	closure.FinalDetectorStates = []DetectorEvaluation{{DetectorID: "health-objective", EvaluatedAt: closure.VerifiedAt, Status: DetectorEvaluationClear}}
	receipt, err := native.ProcessClosure(context.Background(), closure)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := memory.OpenReader(accepted).Get(context.Background(), result.IncidentID)
	if err != nil || receipt.OutcomeCommit == "" || stored.Record.GetClassification() != "success" {
		t.Fatalf("recorded results did not reach accepted outcome: %+v %v", stored.Record, err)
	}
}
