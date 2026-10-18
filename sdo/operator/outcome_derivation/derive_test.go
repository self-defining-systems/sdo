package outcome_derivation

import (
	"context"
	"encoding/json"
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
			var document struct {
				Classification        string `json:"classification"`
				DiagnosisVerification []struct {
					Verdict string `json:"verdict"`
				} `json:"diagnosis_verification"`
			}
			if err := json.Unmarshal(result.Document, &document); err != nil {
				t.Fatal(err)
			}
			if document.Classification != "success" || len(document.DiagnosisVerification) != 1 || document.DiagnosisVerification[0].Verdict != tc.verdict || result.RequiresReflection != tc.reflect {
				t.Fatalf("unexpected derived outcome: %+v, reflect=%v", document, result.RequiresReflection)
			}
		})
	}
}

func TestDeriveAttributesVerifiedRepair(t *testing.T) {
	result, err := Derive(context.Background(), fixtureClosure(), "repair-commit", "codex", "test-model")
	if err != nil {
		t.Fatal(err)
	}
	var record struct {
		Classification        string `json:"classification"`
		DiagnosisVerification []struct {
			Verdict string `json:"verdict"`
			Repair  struct {
				Attributed bool `json:"attributed"`
			} `json:"repair"`
		} `json:"diagnosis_verification"`
		Timestamps struct {
			CompletedAt string `json:"completed_at"`
		} `json:"timestamps"`
	}
	if err := json.Unmarshal(result.Document, &record); err != nil {
		t.Fatal(err)
	}
	if record.Classification != "success" || len(record.DiagnosisVerification) != 1 || record.DiagnosisVerification[0].Verdict != "confirmed" || !record.DiagnosisVerification[0].Repair.Attributed || !result.RequiresReflection {
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
			var record struct {
				Classification string `json:"classification"`
			}
			if err := json.Unmarshal(result.Document, &record); err != nil {
				t.Fatal(err)
			}
			if record.Classification != tc.classification || result.RequiresReflection {
				t.Fatalf("classification=%s reflect=%v", record.Classification, result.RequiresReflection)
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
