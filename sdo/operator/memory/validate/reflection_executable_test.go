package validate

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func TestRecordedHandRepairsRequireExecutablePrograms(t *testing.T) {
	for _, mode := range []string{"README only", "not encoded SDK gap", "available SDK named as gap", "typed step metadata only", "wrong receipt", "wrong incident", "one of two repairs", "diagnose only"} {
		t.Run(mode, func(t *testing.T) {
			root := goodTree(t)
			at := time.Unix(100, 0)
			ref := &contracts.ObjectRef{Kind: "Service", Namespace: "demo", Name: "front"}
			receipt := &contracts.RepairActionReceipt{ActionId: "manual-port", Kind: "exec", Success: true, Resources: []*contracts.ObjectRef{ref}, StartedAt: timestamppb.New(at), CompletedAt: timestamppb.New(at.Add(time.Second))}
			o := &contracts.OutcomeRecord{IncidentId: "learn", Classification: "success", RepairActions: []*contracts.RepairActionReceipt{receipt}}
			d := &contracts.ReflectionDecision{LearningDecision: "updated", PlaybookIds: []string{"service-targetport-mismatch"}}
			paths := []string{".sdo/playbooks/service-targetport-mismatch/README.md", ".sdo/playbooks/service-targetport-mismatch/mitigate.go"}
			if strings.Contains(mode, "SDK gap") || strings.Contains(mode, "named as gap") {
				d.LearningDecision = "sdk_gap"
				d.PlaybookIds = nil
				d.NoChangeReason = "not encoded yet"
				primitive := "not encoded yet"
				if mode == "available SDK named as gap" {
					primitive = "ReconcileFromSource"
				}
				d.SdkGaps = []*contracts.SdkGap{{FaultClass: "routing", Primitive: primitive, Reason: "not encoded yet", ScratchScript: "manual-port", Resources: []*contracts.ObjectRef{ref}}}
			}
			if mode != "README only" && !strings.Contains(mode, "gap") {
				copyTree(t, "../../../../tests/fixtures/sdo/playbook_program_app/.sdo", filepath.Join(root, ".sdo"))
				// Coverage metadata is sufficient even with no replay snapshots or readings.
				dir := filepath.Join(root, ".sdo/corpus/targetport-mismatch")
				for _, name := range []string{"open.json", "fixed.json", "healthy.json", "recordings.json"} {
					remove(t, dir, name)
				}
				link := program.ScriptOriginReceipt{ID: receipt.ActionId, IncidentID: o.IncidentId, Success: true, StartedAt: at, CompletedAt: at.Add(time.Second), Resources: []sdk.ObjectRef{{Kind: "Service", Namespace: "demo", Name: "front"}}}
				if mode == "wrong receipt" {
					link.ID = "peer"
				}
				if mode == "wrong incident" {
					link.IncidentID = "peer"
				}
				raw, _ := json.Marshal([]sdktest.PrimitiveCall{{Method: "RepairReceipt", Result: mustRepairJSON(t, link)}})
				if err := os.WriteFile(filepath.Join(dir, "recordings.json"), raw, 0600); err != nil {
					t.Fatal(err)
				}
				if mode == "one of two repairs" {
					other := *receipt
					other.ActionId = "manual-second"
					o.RepairActions = append(o.RepairActions, &other)
				}
				if mode == "diagnose only" {
					remove(t, root, ".sdo/playbooks/service-targetport-mismatch/mitigate.go")
				}
			}
			ds := ReflectionRepairDiagnostics(root, o, d, paths)
			if mode == "typed step metadata only" {
				if len(ds) != 0 {
					t.Fatalf("executable step with unavailable replay was blocked: %v", ds)
				}
				return
			}
			for _, diag := range ds {
				if diag.Rule == RuleReflectionProgramRequired && strings.Contains(diag.Message, "manual-") {
					return
				}
			}
			t.Fatalf("recorded hand repair escaped executable coverage: %v", ds)
		})
	}
}

func mustRepairJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestRecordedRepairCannotClaimAnAvailableSDKPrimitiveIsMissing(t *testing.T) {
	o := &contracts.OutcomeRecord{Classification: "success", RepairActions: []*contracts.RepairActionReceipt{{ActionId: "recorded", Success: true}}}
	for _, primitive := range []string{"EnsureDatabaseUser", "ReconcileFromSource", "DiffAgainstSource", "RunScript", "not encoded yet"} {
		t.Run(primitive, func(t *testing.T) {
			d := &contracts.ReflectionDecision{LearningDecision: "updated", SdkGaps: []*contracts.SdkGap{{Primitive: primitive, Reason: "not encoded yet"}}}
			ds := ReflectionRepairDiagnostics(t.TempDir(), o, d, nil)
			for _, diag := range ds {
				if diag.Rule == "reflection.sdk-gap-unavailable" && strings.Contains(diag.Message, primitive) {
					return
				}
			}
			t.Fatalf("available primitive accepted as missing: %v", ds)
		})
	}
}

// Minimal synthetic positive metadata linking a real outcome receipt to one
// program. It adds no observations or replay verdicts and works without captures.
func linkRecordedRepair(t *testing.T, root, caseID, programID string, o *contracts.OutcomeRecord, receipt *contracts.RepairActionReceipt) {
	t.Helper()
	dir := filepath.Join(root, ".sdo/corpus", caseID)
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	labels := map[string]any{}
	if raw, err := os.ReadFile(filepath.Join(dir, "labels.json")); err == nil {
		if err := json.Unmarshal(raw, &labels); err != nil {
			t.Fatal(err)
		}
	}
	if len(labels) == 0 {
		labels["root_cause_program_ids"] = []string{programID}
		bindings := map[string]sdk.ObjectRef{}
		for _, ref := range receipt.GetResources() {
			bindings["service"] = sdk.ObjectRef{APIVersion: ref.ApiVersion, Kind: ref.Kind, Namespace: ref.Namespace, Name: ref.Name}
		}
		labels["bindings"] = program.Bindings{Objects: bindings}
		labels["provenance"] = sdktest.CorpusProvenance{IncidentID: o.IncidentId}
	}
	raw := mustRepairJSON(t, labels)
	if err := os.WriteFile(filepath.Join(dir, "labels.json"), raw, 0644); err != nil {
		t.Fatal(err)
	}
	var calls []sdktest.PrimitiveCall
	if raw, err := os.ReadFile(filepath.Join(dir, "recordings.json")); err == nil {
		if err := json.Unmarshal(raw, &calls); err != nil {
			t.Fatal(err)
		}
	}
	origin := program.ScriptOriginReceipt{ID: receipt.ActionId, IncidentID: o.IncidentId, Success: receipt.Success, ScriptDigest: receipt.ScriptDigest, StartedAt: receipt.StartedAt.AsTime(), CompletedAt: receipt.CompletedAt.AsTime()}
	for _, ref := range receipt.GetResources() {
		origin.Resources = append(origin.Resources, sdk.ObjectRef{APIVersion: ref.ApiVersion, Kind: ref.Kind, Namespace: ref.Namespace, Name: ref.Name})
	}
	calls = append(calls, sdktest.PrimitiveCall{Method: "RepairReceipt", Result: mustRepairJSON(t, origin)})
	if err := os.WriteFile(filepath.Join(dir, "recordings.json"), mustRepairJSON(t, calls), 0644); err != nil {
		t.Fatal(err)
	}
}

func TestSDKGapCapabilityValidationTracksTheInstalledVocabulary(t *testing.T) {
	o := &contracts.OutcomeRecord{}
	fields := (&contracts.Action{}).ProtoReflect().Descriptor().Oneofs().ByName("kind").Fields()
	for index := 0; index < fields.Len(); index++ {
		name := string(fields.Get(index).Name())
		if ds := recordedRepairSDKGapDiagnostics(o, []*contracts.SdkGap{{Primitive: name}}); len(ds) != 1 {
			t.Fatalf("installed effect %s escaped gap validation: %v", name, ds)
		}
	}
	readView := reflect.TypeOf((*program.ReadView)(nil)).Elem()
	for index := 0; index < readView.NumMethod(); index++ {
		name := readView.Method(index).Name
		if ds := recordedRepairSDKGapDiagnostics(o, []*contracts.SdkGap{{Primitive: "ReadView." + name}}); len(ds) != 1 {
			t.Fatalf("installed read %s escaped gap validation: %v", name, ds)
		}
	}
	o.ProgramRuns = []*contracts.ProgramRun{{Error: "DatabaseUsers observation unavailable: original backend failed"}}
	if ds := recordedRepairSDKGapDiagnostics(o, []*contracts.SdkGap{{Primitive: "DatabaseUsers"}}); len(ds) != 0 {
		t.Fatalf("authoritatively unavailable observation rejected: %v", ds)
	}
	if ds := recordedRepairSDKGapDiagnostics(o, []*contracts.SdkGap{{Primitive: "DiffAgainstSource"}}); len(ds) != 1 {
		t.Fatalf("peer error excused a different available read: %v", ds)
	}
}

func TestUnverifiedPeerDoesNotBlockExecutablePartialPublication(t *testing.T) {
	root := goodTree(t)
	copyTree(t, "../../../../tests/fixtures/sdo/playbook_program_app/.sdo", filepath.Join(root, ".sdo"))
	front := &contracts.ObjectRef{Kind: "Service", Namespace: "demo", Name: "front"}
	peer := &contracts.ObjectRef{Kind: "Service", Namespace: "demo", Name: "peer"}
	o := &contracts.OutcomeRecord{IncidentId: "partial", Classification: "failed", ConfirmedRootCauses: []*contracts.ConfirmedRootCause{{Resources: []*contracts.ObjectRef{front}}, {Resources: []*contracts.ObjectRef{peer}}}, ReflectionEligibility: &contracts.ReflectionEligibility{Version: 1, Causes: []*contracts.ReflectionCauseWork{{CauseIndex: 0, Resources: []*contracts.ObjectRef{front}}}}}
	addSyntheticFindingRepairProof(o)
	linkRecordedRepair(t, root, "targetport-mismatch", "service-targetport-mismatch", o, o.RepairActions[0])
	ds := ReflectionRepairDiagnostics(root, o, &contracts.ReflectionDecision{LearningDecision: "updated", PlaybookIds: []string{"service-targetport-mismatch"}}, []string{".sdo/playbooks/service-targetport-mismatch/mitigate.go"})
	if len(ds) != 0 {
		t.Fatalf("unverified peer made executable partial learning all-or-nothing: %v", ds)
	}
}

// The program-required hint is the exact change: appending its recordings.json
// entry to a case that labels and binds the program clears the diagnostic.
func TestProgramRequiredHintEntryClearsTheDiagnostic(t *testing.T) {
	root := goodTree(t)
	copyTree(t, "../../../../tests/fixtures/sdo/playbook_program_app/.sdo", filepath.Join(root, ".sdo"))
	at := time.Date(2026, 10, 28, 16, 1, 2, 345678901, time.UTC)
	ref := &contracts.ObjectRef{ApiVersion: "v1", Kind: "Service", Namespace: "demo", Name: "front"}
	receipt := &contracts.RepairActionReceipt{ActionId: "manual-port", Kind: "exec", Success: true, ScriptDigest: "abc", Resources: []*contracts.ObjectRef{ref}, StartedAt: timestamppb.New(at), CompletedAt: timestamppb.New(at.Add(time.Second))}
	o := &contracts.OutcomeRecord{IncidentId: "learn", Classification: "success", RepairActions: []*contracts.RepairActionReceipt{receipt}}
	d := &contracts.ReflectionDecision{LearningDecision: "updated", PlaybookIds: []string{"service-targetport-mismatch"}}
	paths := []string{".sdo/playbooks/service-targetport-mismatch/mitigate.go"}
	ds := ReflectionRepairDiagnostics(root, o, d, paths)
	if len(ds) != 1 || ds[0].Rule != RuleReflectionProgramRequired {
		t.Fatalf("unlinked repair: %v", ds)
	}
	for _, want := range []string{"proposed_artifact_ids", "root_cause_program_ids", `{"api_version":"v1","kind":"Service","namespace":"demo","name":"front"}`, "recordings.json"} {
		if !strings.Contains(ds[0].Hint, want) {
			t.Fatalf("hint does not say %q: %s", want, ds[0].Hint)
		}
	}
	_, entry, ok := strings.Cut(ds[0].Hint, "append this entry to its recordings.json: ")
	if !ok {
		t.Fatalf("hint carries no recordings entry: %s", ds[0].Hint)
	}
	var call sdktest.PrimitiveCall
	if err := json.Unmarshal([]byte(entry), &call); err != nil {
		t.Fatalf("hint entry is not one recording: %v %s", err, entry)
	}
	path := filepath.Join(root, ".sdo/corpus/targetport-mismatch/recordings.json")
	var calls []json.RawMessage
	raw, err := os.ReadFile(path)
	if err != nil || json.Unmarshal(raw, &calls) != nil {
		t.Fatal(err)
	}
	calls = append(calls, json.RawMessage(entry))
	if err := os.WriteFile(path, mustRepairJSON(t, calls), 0644); err != nil {
		t.Fatal(err)
	}
	if ds := ReflectionRepairDiagnostics(root, o, d, paths); len(ds) != 0 {
		t.Fatalf("hint entry did not link the repair: %v", ds)
	}
}

// A linked case that leaves a repaired resource unbound names that resource.
func TestProgramRequiredNamesTheUnboundResource(t *testing.T) {
	root := goodTree(t)
	copyTree(t, "../../../../tests/fixtures/sdo/playbook_program_app/.sdo", filepath.Join(root, ".sdo"))
	at := time.Unix(100, 0)
	front := &contracts.ObjectRef{Kind: "Service", Namespace: "demo", Name: "front"}
	extra := &contracts.ObjectRef{Kind: "ConfigMap", Namespace: "demo", Name: "front-config"}
	receipt := &contracts.RepairActionReceipt{ActionId: "manual-port", Kind: "exec", Success: true, Resources: []*contracts.ObjectRef{front, extra}, StartedAt: timestamppb.New(at), CompletedAt: timestamppb.New(at.Add(time.Second))}
	o := &contracts.OutcomeRecord{IncidentId: "learn", Classification: "success", RepairActions: []*contracts.RepairActionReceipt{receipt}}
	linkRecordedRepair(t, root, "targetport-mismatch", "service-targetport-mismatch", o, receipt)
	d := &contracts.ReflectionDecision{LearningDecision: "updated", PlaybookIds: []string{"service-targetport-mismatch"}}
	ds := ReflectionRepairDiagnostics(root, o, d, []string{".sdo/playbooks/service-targetport-mismatch/mitigate.go"})
	if len(ds) != 1 || !strings.Contains(ds[0].Hint, `{"kind":"ConfigMap","namespace":"demo","name":"front-config"}`) || strings.Contains(ds[0].Hint, `"name":"front"}`) {
		t.Fatalf("unbound resource not named: %v", ds)
	}
}
