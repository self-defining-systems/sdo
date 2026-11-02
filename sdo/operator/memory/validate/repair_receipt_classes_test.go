package validate

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	programvalidation "sdo.dev/operator/memory/validate/program"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// Ratchet: every mutating responder command class records a receipt that
// reflection can save as an executable RunScript step. The receipt kind never
// decides; a class added to the shared list without passing here fails.
func TestEveryRepairReceiptClassEncodesAsRunScriptStep(t *testing.T) {
	raw, err := os.ReadFile("../../../../tests/fixtures/sdo/repair_receipt_classes.json")
	if err != nil {
		t.Fatal(err)
	}
	var classes struct {
		SdoAct []struct {
			Kind string `json:"kind"`
		} `json:"sdo_act"`
		Controller []string `json:"controller"`
	}
	if err := json.Unmarshal(raw, &classes); err != nil {
		t.Fatal(err)
	}
	kinds := append([]string(nil), classes.Controller...)
	for _, class := range classes.SdoAct {
		kinds = append(kinds, class.Kind)
	}
	if len(kinds) < 10 {
		t.Fatalf("receipt class list shrank: %v", kinds)
	}
	for _, kind := range append(kinds, "recorded-other-bytes") {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			copyTree(t, "../../../../tests/fixtures/sdo/hotel_run_script_upgrade/.sdo", filepath.Join(root, ".sdo"))
			copyTree(t, "../../../../tests/fixtures/sdo/playbook_program_app/.sdo/diagnostics/go.mod", filepath.Join(root, ".sdo/diagnostics/go.mod"))
			at := time.Date(2026, 1, 1, 0, 0, 19, 0, time.UTC)
			ref := &contracts.ObjectRef{ApiVersion: "v1", Kind: "Pod", Namespace: "demo", Name: "mongodb-geo"}
			receipt := &contracts.RepairActionReceipt{ActionId: "manual-mongo-user", Kind: kind, Target: "Pod/demo/mongodb-geo", Summary: "hand repair", Details: "synthetic", Success: true, Resources: []*contracts.ObjectRef{ref}, StartedAt: timestamppb.New(at), CompletedAt: timestamppb.New(at.Add(time.Second))}
			if kind == "recorded-other-bytes" {
				// Negative control: a receipt naming other executed bytes.
				receipt.Kind, receipt.ScriptDigest = "run-script", program.ScriptDigest("echo other")
			}
			outcome := &contracts.OutcomeRecord{IncidentId: "mongo-auth-upgrade", Classification: "success", RepairActions: []*contracts.RepairActionReceipt{receipt}}
			// The controller's capture shape (memory.CaptureRepairReceipts).
			recordings := filepath.Join(root, ".sdo/corpus/mongo-auth/recordings.json")
			data, err := os.ReadFile(recordings)
			if err != nil {
				t.Fatal(err)
			}
			var calls []sdktest.PrimitiveCall
			if err := json.Unmarshal(data, &calls); err != nil {
				t.Fatal(err)
			}
			for i, call := range calls {
				if call.Method == "RepairReceipt" {
					calls[i].Result = mustRepairJSON(t, program.ScriptOriginReceipt{ScriptDigest: receipt.GetScriptDigest(), ID: receipt.ActionId, IncidentID: outcome.IncidentId, Success: true, StartedAt: at, CompletedAt: at.Add(time.Second), Resources: []sdk.ObjectRef{{APIVersion: "v1", Kind: "Pod", Namespace: "demo", Name: "mongodb-geo"}}})
				}
			}
			if data, err = json.Marshal(calls); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(recordings, data, 0o644); err != nil {
				t.Fatal(err)
			}
			paths := []string{".sdo/playbooks/mongo-auth-bootstrap/README.md", ".sdo/playbooks/mongo-auth-bootstrap/diagnose.go", ".sdo/playbooks/mongo-auth-bootstrap/mitigate.go", ".sdo/corpus/mongo-auth/labels.json", ".sdo/corpus/mongo-auth/recordings.json"}
			decision := &contracts.ReflectionDecision{LearningDecision: "updated", PlaybookIds: []string{"mongo-auth-bootstrap"}}
			var problems []string
			for _, d := range ReflectionRepairDiagnostics(root, outcome, decision, paths) {
				problems = append(problems, d.Rule+": "+d.Message)
			}
			if err := programvalidation.ValidateScriptOrigins(root, outcome, paths); err != nil {
				problems = append(problems, err.Error())
			}
			if _, err := programvalidation.InspectMemory(filepath.Join(root, ".sdo"), "fixture-diagnostics"); err != nil {
				problems = append(problems, err.Error())
			}
			if kind == "recorded-other-bytes" {
				if len(problems) == 0 {
					t.Fatal("script differing from recorded executed bytes accepted")
				}
				return
			}
			if len(problems) != 0 {
				t.Fatalf("%s hand repair cannot be saved as a RunScript step:\n%s", kind, strings.Join(problems, "\n"))
			}
		})
	}
}
