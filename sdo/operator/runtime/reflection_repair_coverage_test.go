package runtime

import (
	"os"
	"path/filepath"
	"sdo.dev/operator/memory"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// Exercise the real receipt producer and contract projection into the shared
// acceptance rule. Neither receipt IDs nor receipt kinds are hand-written.
func TestControllerRepairReceiptsFeedReflectionCoverage(t *testing.T) {
	for _, kind := range []string{"playbook", "hand", "script", "hand-script"} {
		t.Run(kind, func(t *testing.T) {
			ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
			at := time.Unix(100, 0)
			action := program.Action(program.SetField{Ref: ref, Path: "spec.ports[0].targetPort", Value: testPlanValue(8080)})
			var reports []PlanActionReport
			if kind == "script" || kind == "hand-script" {
				script := program.RunScript{Script: "true"}
				action = script
				reports = []PlanActionReport{{Status: "applied", Script: &ScriptExecutionResult{ScriptDigest: program.ScriptDigest(script.Script)}}}
			}
			app := RecordedPlanApplication{ID: "application-1", StartedAt: at, CompletedAt: at.Add(time.Second), Result: PlanApplicationResult{Applied: true, Plan: program.Plan{Actions: []program.Action{action}}, ActionsApplied: []program.Action{action}, ActionReports: reports, VerificationCompleted: true, VerificationPassed: true}}
			if kind == "playbook" || kind == "script" {
				app.ProgramIDs = []string{"restore-port"}
			}
			receipts := ProgramRepairReceipts([]RecordedPlanApplication{app}, nil)
			if len(receipts) == 0 {
				t.Fatal("producer emitted no repair evidence")
			}
			result, err := incidentResultContract(IncidentResult{RepairActions: receipts, PlanApplications: []RecordedPlanApplication{app}})
			if err != nil {
				t.Fatal(err)
			}
			o := &contracts.OutcomeRecord{Classification: "success", RepairActions: result.RepairActions, PlanApplications: result.PlanApplications}
			ds := validate.ReflectionRepairDiagnostics(t.TempDir(), o, &contracts.ReflectionDecision{LearningDecision: "no_change", NoChangeReason: "covered"}, nil)
			wantGap := kind == "hand" || kind == "hand-script"
			if (len(ds) > 0) != wantGap {
				t.Fatalf("kind=%s diagnostics=%v", kind, ds)
			}
			for _, d := range ds {
				if d.Rule != validate.RuleReflectionProgramRequired {
					t.Fatalf("unexpected rejection: %v", d)
				}
			}
		})
	}
}

// Exercise the production capture producer when a scripted reflector copies
// original receipts into an authored positive case, as prepare_corpus does.
func appendControllerRepairReceipts(t *testing.T, root, caseID string, outcome *contracts.OutcomeRecord) {
	t.Helper()
	target := filepath.Join(root, ".sdo/corpus", caseID, "recordings.json")
	raw, err := os.ReadFile(target)
	if os.IsNotExist(err) {
		raw = []byte("[]")
	} else if err != nil {
		t.Fatal(err)
	}
	sessions := t.TempDir()
	capture, err := memory.IncidentCorpusDir(sessions, outcome.IncidentId)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(capture, 0755); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(capture, "open@detect-recordings.json")
	if err := os.WriteFile(source, raw, 0644); err != nil {
		t.Fatal(err)
	}
	if err := memory.CaptureRepairReceipts(sessions, outcome.IncidentId, outcome); err != nil {
		t.Fatal(err)
	}
	raw, err = os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, raw, 0644); err != nil {
		t.Fatal(err)
	}
}
