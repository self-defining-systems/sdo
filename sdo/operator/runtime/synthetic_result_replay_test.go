package runtime

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/worker_registry"
)

// Exercise the native broker with hand-constructed worker responses, not a recording.
func TestSyntheticWorkerResultReachesNativeBroker(t *testing.T) {
	t.Parallel()
	responderPayload, err := anypb.New(syntheticIncidentResult())
	if err != nil {
		t.Fatal(err)
	}
	responder := &contracts.AgentResult{Response: responderPayload}
	typed := &contracts.IncidentResult{}
	if err := responder.GetResponse().UnmarshalTo(typed); err != nil {
		t.Fatal(err)
	}
	result := incidentResultState(typed)
	result.RepairActions = []RepairActionReceipt{{ActionID: "synthetic-repair", Kind: "patch", Target: "Service/app/reservation", Summary: "synthetic repair", Details: "test-only patch", Success: true, StartedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), CompletedAt: time.Date(2026, 1, 1, 0, 0, 1, 0, time.UTC)}}
	result.Timing = TimingMetrics{StartedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), CompletedAt: time.Date(2026, 1, 1, 0, 0, 1, 0, time.UTC)}
	accepted := nativeRepository(t)
	repository, err := broker.New(accepted, filepath.Join(t.TempDir(), "proposals"), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	native := NativeIncidentBroker{Repository: repository, LedgerRoot: filepath.Join(t.TempDir(), "ledger"), Deriver: ProtoOutcomeDeriver{}, ResponderBackend: "synthetic", ResponderModel: "none"}
	workspace, err := native.PrepareIncident(context.Background(), result.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	closure := nativeClosure(workspace)
	closure.Request.Namespace = "app"
	closure.Request.RepairPolicy = "recorded-actions"
	closure.Request.Findings = []sdk.Finding{{DetectorID: "health-objective", RuleID: "availability", Summary: "service unavailable", Evidence: "service has no ready endpoint", Status: sdk.FindingActive, Severity: sdk.SeverityCritical, PrimaryResource: sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "reservation"}}}
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
	if err != nil || receipt.OutcomeCommit == "" || stored.Record.GetClassification() != "false_positive" {
		t.Fatalf("synthetic results did not reach accepted outcome: %+v %v", stored.Record, err)
	}
}

func TestSyntheticWorkerResultsThroughContracts(t *testing.T) {
	registry, err := worker_registry.LoadDirectory(filepath.Join("..", "..", "agents", "registrations"))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		agent, taskType   string
		request, response proto.Message
	}{
		{"sdo-responder", "incident-response", &contracts.IncidentRequest{}, syntheticIncidentResult()},
		{"sdo-reflector", "outcome-reflection-direct-v1", &contracts.ReflectionRequest{}, &contracts.ReflectionResult{LearningDecision: "no_change", NoChangeReason: "synthetic fixture"}},
	} {
		t.Run(tc.agent, func(t *testing.T) {
			request, err := anypb.New(tc.request)
			if err != nil {
				t.Fatal(err)
			}
			response, err := anypb.New(tc.response)
			if err != nil {
				t.Fatal(err)
			}
			task := &contracts.AgentTask{SchemaVersion: ProtocolSchemaVersion, TaskId: "synthetic-task", AgentId: tc.agent, TaskType: tc.taskType, Request: request, Workspace: &contracts.ProposalWorkspace{ProposalId: "synthetic-proposal"}}
			result := &contracts.AgentResult{SchemaVersion: ProtocolSchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: response}
			wire, err := protojson.Marshal(result)
			if err != nil {
				t.Fatal(err)
			}
			decoded := &contracts.AgentResult{}
			if err := protojson.Unmarshal(wire, decoded); err != nil {
				t.Fatal(err)
			}
			if err := registry.ValidateResult(task, decoded); err != nil {
				t.Fatal(err)
			}
			if tc.agent == "sdo-responder" {
				state, err := decodeIncidentAgentResult(wire, task, registry)
				if err != nil || state.IncidentID != "synthetic-incident" {
					t.Fatalf("decode response: %+v %v", state, err)
				}
			}
			decoded.ProposalId = "another-proposal"
			if err := registry.ValidateResult(task, decoded); err == nil {
				t.Fatal("cross-proposal result accepted")
			}
		})
	}
}

func syntheticIncidentResult() *contracts.IncidentResult {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	return &contracts.IncidentResult{SchemaVersion: ProtocolSchemaVersion, IncidentId: "synthetic-incident", Status: "completed", Usage: &contracts.UsageMetrics{}, Timing: &contracts.TimingMetrics{StartedAt: timestamppb.New(start), CompletedAt: timestamppb.New(start.Add(time.Second))}}
}
