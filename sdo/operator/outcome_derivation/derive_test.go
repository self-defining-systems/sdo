package outcome_derivation

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
	contractsv1alpha1 "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func fixtureClosure() *contractsv1alpha1.IncidentClosure {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	stamp := func(offset time.Duration) *timestamppb.Timestamp { return timestamppb.New(now.Add(offset)) }
	return &contractsv1alpha1.IncidentClosure{
		Request: &contractsv1alpha1.IncidentRequest{
			IncidentId: "incident-1", SourceCommit: "source", DeployedCommit: "deployed",
			Findings:        []*contractsv1alpha1.Finding{{DetectorId: "health", RuleId: "unhealthy", Status: "active"}},
			DetectorHistory: []*contractsv1alpha1.DetectorEvaluation{{DetectorId: "health", Status: "firing", EvaluatedAt: stamp(0)}},
		},
		Result: &contractsv1alpha1.IncidentResult{
			IncidentId: "incident-1", Status: "completed",
			ConfirmedRootCauses: []*contractsv1alpha1.ConfirmedRootCause{{
				Summary: "bad replica", Resources: []*contractsv1alpha1.ObjectRef{{Kind: "Deployment", Name: "app"}},
				Evidence:           []*contractsv1alpha1.RootCauseEvidence{{Kind: "detector-finding", Source: "health"}},
				ExplainedDetectors: []string{"health"},
			}},
			RepairActions: []*contractsv1alpha1.RepairActionReceipt{{
				ActionId: "repair-1", Target: "Deployment/app", Success: true, StartedAt: stamp(time.Minute),
				Resources: []*contractsv1alpha1.ObjectRef{{Kind: "Deployment", Name: "app"}},
			}},
			Usage: &contractsv1alpha1.UsageMetrics{},
		},
		FinalDetectorStates: []*contractsv1alpha1.DetectorEvaluation{{DetectorId: "health", Status: "clear", EvaluatedAt: stamp(3 * time.Minute)}},
		DetectedAt:          stamp(0), DispatchedAt: stamp(time.Minute), ResponderCompletedAt: stamp(2 * time.Minute),
		VerifiedAt: stamp(3 * time.Minute), HealthClearedAt: stamp(2 * time.Minute),
	}
}

func TestDeriveGoldenClosure(t *testing.T) {
	path := filepath.Join("..", "..", "..", "tests", "fixtures", "sdo", "contracts", "go", "closure_repaired.json")
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	closure := new(contractsv1alpha1.IncidentClosure)
	if err := protojson.Unmarshal(payload, closure); err != nil {
		t.Fatal(err)
	}
	result, err := Derive(context.Background(), closure, "repair-commit", "codex", "test-model")
	if err != nil {
		t.Fatal(err)
	}
	if result.Record.GetClassification() != "success" || !result.RequiresReflection {
		t.Fatalf("unexpected derived outcome: %+v, reflect=%v", result.Record, result.RequiresReflection)
	}
}

func TestDeriveClassifiesVerifiedRepairAsSuccess(t *testing.T) {
	result, err := Derive(context.Background(), fixtureClosure(), "repair-commit", "codex", "test-model")
	if err != nil {
		t.Fatal(err)
	}
	if result.Record == nil || result.Record.GetIncidentId() != "incident-1" || result.Record.GetClassification() != "success" {
		t.Fatalf("derivation did not return a typed outcome: %+v", result.Record)
	}
	record := result.Record
	if record.GetClassification() != "success" || record.GetTimestamps().GetCompletedAt() == nil || !result.RequiresReflection {
		t.Fatalf("unexpected outcome: %+v, reflect=%v", record, result.RequiresReflection)
	}
}

func TestDeriveCarriesClassifiedDriftEvidence(t *testing.T) {
	closure := fixtureClosure()
	closure.Request.DriftEvents = []*contractsv1alpha1.DriftEvent{{Ref: &contractsv1alpha1.ObjectRef{Kind: "Service", Namespace: "app", Name: "api"},
		Fields: []*contractsv1alpha1.DriftField{{Path: "spec.ports[0].targetPort", Old: "8080", New: "9999"}},
		Source: "differs from accepted manifest", Manager: "unknown-editor", Classification: "fault", ProgramId: "targetport", ObservedAt: closure.DetectedAt}}
	result, err := Derive(context.Background(), closure, "repair-commit", "codex", "test-model")
	if err != nil || len(result.Record.GetDriftEvents()) != 1 || result.Record.GetDriftEvents()[0].GetProgramId() != "targetport" {
		t.Fatalf("classified drift lost from outcome: %+v, %v", result.Record, err)
	}
}

func TestDeriveMitigationTimeFromVerifiedRepair(t *testing.T) {
	closure := fixtureClosure()
	closure.Result.RepairActions[0].CompletedAt = timestamppb.New(closure.DetectedAt.AsTime().Add(90 * time.Second))
	closure.ResponderCompletedAt = timestamppb.New(closure.DetectedAt.AsTime().Add(10 * time.Minute))
	closure.VerifiedAt = timestamppb.New(closure.DetectedAt.AsTime().Add(11 * time.Minute))
	result, err := Derive(context.Background(), closure, "repair-commit", "codex", "test-model")
	if err != nil {
		t.Fatal(err)
	}
	if got := result.Record.GetTimestamps().GetMitigatedAt().AsTime(); !got.Equal(closure.Result.RepairActions[0].CompletedAt.AsTime()) {
		t.Fatalf("mitigated at %v, want verified repair completion %v", got, closure.Result.RepairActions[0].CompletedAt.AsTime())
	}
}

func TestDerivePreservesResponderProgramEvidenceAsProtoJSON(t *testing.T) {
	closure := fixtureClosure()
	closure.Result.ProgramToolCalls = []*contractsv1alpha1.ProgramToolCall{{Command: "apply", At: timestamppb.Now(), RequestJson: `{"plan_ids":["plan-1"]}`}}
	closure.Result.ProgramRuns = []*contractsv1alpha1.ProgramRun{{ProgramId: "targetport", Verdict: &contractsv1alpha1.Verdict{Result: contractsv1alpha1.Verdict_RESULT_NO_MATCH}}}
	closure.Result.PlanApplications = []*contractsv1alpha1.PlanApplication{{ApplicationId: "application-1", ProgramIds: []string{"targetport"}, Applied: true, Verification: &contractsv1alpha1.Verdict{Result: contractsv1alpha1.Verdict_RESULT_NO_MATCH}}}
	closure.Result.ResponseLoopsUsed = 1
	result, err := Derive(context.Background(), closure, "repair-commit", "codex", "test-model")
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := protojson.Marshal(result.Record)
	if err != nil {
		t.Fatal(err)
	}
	recorded := new(contractsv1alpha1.OutcomeRecord)
	if err := protojson.Unmarshal(encoded, recorded); err != nil {
		t.Fatal(err)
	}
	if len(recorded.GetProgramToolCalls()) != 1 || len(recorded.GetProgramRuns()) != 1 || len(recorded.GetPlanApplications()) != 1 || recorded.GetResponseLoopsUsed() != 1 {
		t.Fatalf("program outcome evidence lost: %+v", recorded)
	}
}

func TestDeriveClearedWithoutAction(t *testing.T) {
	for _, tc := range []struct {
		name           string
		mutate         func(*contractsv1alpha1.IncidentClosure)
		classification string
	}{
		{"no-action", func(c *contractsv1alpha1.IncidentClosure) { c.Result.RepairActions = nil }, "cleared_without_action"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			closure := fixtureClosure()
			tc.mutate(closure)
			result, err := Derive(context.Background(), closure, "", "codex", "test-model")
			if err != nil {
				t.Fatal(err)
			}
			if result.Record.GetClassification() != tc.classification || result.RequiresReflection {
				t.Fatalf("classification=%s reflect=%v", result.Record.GetClassification(), result.RequiresReflection)
			}
		})
	}
}

func TestDeriveRejectsReversedControllerTimes(t *testing.T) {
	closure := fixtureClosure()
	closure.DispatchedAt = timestamppb.New(closure.GetResponderCompletedAt().AsTime().Add(time.Minute))
	if _, err := Derive(context.Background(), closure, "repair-commit", "codex", "test-model"); err == nil {
		t.Fatal("reversed controller timeline must not produce a durable outcome")
	}
}
