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

func TestDeriveGoldenClosureDiagnosisParity(t *testing.T) {
	for _, tc := range []struct {
		file, verdict string
		reflect       bool
	}{
		{"closure_repaired.json", "confirmed", true},
		{"closure_own_edit.json", "contradicted", false},
	} {
		t.Run(tc.file, func(t *testing.T) {
			path := filepath.Join("..", "..", "..", "tests", "fixtures", "sdo", "contracts", "go", tc.file)
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
			record := result.Record
			if record.GetClassification() != "success" || len(record.GetDiagnosisVerification()) != 1 || record.GetDiagnosisVerification()[0].GetVerdict() != tc.verdict || result.RequiresReflection != tc.reflect {
				t.Fatalf("unexpected derived outcome: %+v, reflect=%v", record, result.RequiresReflection)
			}
		})
	}
}

func TestDeriveAttributesVerifiedRepair(t *testing.T) {
	result, err := Derive(context.Background(), fixtureClosure(), "repair-commit", "codex", "test-model")
	if err != nil {
		t.Fatal(err)
	}
	if result.Record == nil || result.Record.GetIncidentId() != "incident-1" || result.Record.GetClassification() != "success" {
		t.Fatalf("derivation did not return a typed outcome: %+v", result.Record)
	}
	record := result.Record
	if record.GetClassification() != "success" || len(record.GetDiagnosisVerification()) != 1 || record.GetDiagnosisVerification()[0].GetVerdict() != "confirmed" || !record.GetDiagnosisVerification()[0].GetRepair().GetAttributed() || record.GetTimestamps().GetCompletedAt() == nil || !result.RequiresReflection {
		t.Fatalf("unexpected outcome: %+v, reflect=%v", record, result.RequiresReflection)
	}
}

func TestDeriveRejectsExternalRecoveryAndNoAction(t *testing.T) {
	for _, tc := range []struct {
		name           string
		mutate         func(*contractsv1alpha1.IncidentClosure)
		classification string
	}{
		{"external", func(c *contractsv1alpha1.IncidentClosure) { c.Result.RepairActions[0].Resources[0].Name = "other" }, "external_recovery"},
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
