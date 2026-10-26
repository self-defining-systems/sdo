package runtime

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"sdo.dev/operator/apihistory"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func TestUnwrappedShellWriteBindsRunScriptOnlyAfterIndependentClearance(t *testing.T) {
	for _, clear := range []bool{false, true} {
		t.Run(map[bool]string{false: "unverified", true: "verified"}[clear], func(t *testing.T) {
			root := t.TempDir()
			const id = "shell-repair"
			sub, err := memory.PrepareIncidentSession(root, "", id)
			if err != nil {
				t.Fatal(err)
			}
			store, err := apihistory.Open(filepath.Join(root, sub, "api-history"), id, apihistory.Limits{})
			if err != nil {
				t.Fatal(err)
			}
			at := time.Unix(100, 0)
			event := apihistory.Event{RequestID: "unwrapped-patch", Phase: "start", Started: at.Add(time.Second), Verb: "patch", Group: "apps", Version: "v1", Resource: "deployments", Namespace: "app", Name: "web"}
			store.Record(event, nil)
			event.Phase = "end"
			event.Status = 200
			event.Finished = at.Add(2 * time.Second)
			store.Record(event, []byte(`{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"web","namespace":"app","resourceVersion":"2"}}`))
			store.Close()
			ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "web"}
			closure := IncidentClosure{Request: IncidentRequest{IncidentID: id, Namespace: "app", SourceCommit: "source", Findings: []sdk.Finding{{DetectorID: "workload", RuleID: "probe", Status: sdk.FindingActive, PrimaryResource: ref, Summary: "invalid probe", Evidence: "synthetic observed failing probe"}}, DetectorHistory: []DetectorEvaluation{{DetectorID: "workload", EvaluatedAt: at, Status: DetectorEvaluationFiring}}}, Result: &IncidentResult{SchemaVersion: ProtocolSchemaVersion, IncidentID: id, Status: IncidentCompleted, ConfirmedRootCauses: []ConfirmedRootCause{{Summary: "invalid probe", Resources: []sdk.ObjectRef{ref}, ExplainedDetectors: []string{"workload"}}}}, DetectedAt: at, DispatchedAt: at, ResponderCompletedAt: at.Add(4 * time.Second)}
			if clear {
				closure.VerifiedAt = at.Add(5 * time.Second)
				closure.FinalDetectorStates = []DetectorEvaluation{{DetectorID: "workload", EvaluatedAt: closure.VerifiedAt, Status: DetectorEvaluationClear}}
			}
			derived, err := (ProtoOutcomeDeriver{SessionRoot: root}).Derive(context.Background(), closure, "", "test", "test")
			if err != nil {
				t.Fatal(err)
			}
			if len(closure.Result.RepairActions) != 0 {
				t.Fatal("derivation mutated frozen closure")
			}
			if len(derived.Record.RepairActions) != 1 {
				t.Fatalf("observed write lost: %+v", derived.Record)
			}
			causes := validate.VerifiedReflectionCauses(derived.Record)
			if !clear {
				if len(causes) != 0 {
					t.Fatal("API success alone earned learning authority")
				}
				return
			}
			if len(causes) != 1 {
				t.Fatalf("independently cleared shell repair cannot bind: %+v", derived.Record)
			}
			if err := memory.CaptureIncidentSnapshot(root, id, "open@detect", sdktest.Snapshot{NamespaceName: "app"}, "app"); err != nil {
				t.Fatal(err)
			}
			if err := memory.CaptureRepairReceipts(root, id, derived.Record); err != nil {
				t.Fatal(err)
			}
			dir, err := memory.IncidentCorpusDir(root, id)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := os.ReadFile(filepath.Join(dir, "open@detect-recordings.json"))
			if err != nil {
				t.Fatal(err)
			}
			var calls []sdktest.PrimitiveCall
			if err := json.Unmarshal(raw, &calls); err != nil {
				t.Fatal(err)
			}
			script := program.RunScript{Script: `kubectl patch deployment "$WORKLOAD" --type=json -p "$PATCH"`}
			corpus := sdktest.CorpusCase{Provenance: sdktest.CorpusProvenance{IncidentID: id}, ScriptOrigins: map[string]string{program.ScriptDigest(script.Script): derived.Record.RepairActions[0].ActionId}, Recordings: calls}
			origin, err := sdktest.ScriptOrigin(script, corpus)
			if err != nil || origin.ID != derived.Record.RepairActions[0].ActionId {
				t.Fatalf("verified API receipt cannot bind RunScript: %+v %v", origin, err)
			}
		})
	}
}
