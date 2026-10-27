package validate

import (
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/layout"
)

func TestReflectionRecordsGapForVerifiedCauseWithoutProgram(t *testing.T) {
	root := goodTree(t)
	outcome := &contracts.OutcomeRecord{IncidentId: "first", Classification: "success", ConfirmedRootCauses: []*contracts.ConfirmedRootCause{{Summary: "routing fault", Resources: []*contracts.ObjectRef{{Kind: "Service", Namespace: "app", Name: "api"}}}}}
	addSyntheticFindingRepairProof(outcome)
	decision := &contracts.ReflectionDecision{LearningDecision: "updated", PlaybookIds: []string{"base"}}
	ds, blocking, err := ReflectionCoverageGaps(root, outcome, decision)
	if err != nil || len(blocking) != 0 || len(ds) != 1 || ds[0].Rule != "reflection.program-required" {
		t.Fatalf("README-only reflection gap not recorded: %v %v", ds, err)
	}
	decision.LearningDecision = "no_change"
	ds, blocking, err = ReflectionCoverageGaps(root, outcome, decision)
	if err != nil || len(ds) != 1 {
		t.Fatalf("unlearned no-op gap not recorded: %v %v", ds, err)
	}
	decision.DeclineReason = "no_recorded_repair"
	ds, blocking, err = ReflectionCoverageGaps(root, outcome, decision)
	if err != nil || len(ds) != 1 {
		t.Fatalf("decline reason hid the gap: %v %v", ds, err)
	}
}

func TestReflectionLearnsFromSessionFixtureWithoutControllerCapture(t *testing.T) {
	root := t.TempDir()
	copyTree(t, "../../../../tests/fixtures/sdo/playbook_program_app/.sdo", filepath.Join(root, ".sdo"))
	initial, err := layout.ReadDetectors(filepath.Join(root, layout.DiagnosticsDir))
	if err != nil {
		t.Fatal(err)
	}
	for _, detector := range initial {
		detector.Manifest.PossiblePlaybooks = slices.DeleteFunc(detector.Manifest.PossiblePlaybooks, func(id string) bool { return id == "service-targetport-mismatch" })
		if err := layout.WriteDetector(root, detector); err != nil {
			t.Fatal(err)
		}
	}
	// No protected capture directory or provenance: these are session observations.
	outcome := &contracts.OutcomeRecord{IncidentId: "first", Classification: "success", ConfirmedRootCauses: []*contracts.ConfirmedRootCause{{Resources: []*contracts.ObjectRef{{Kind: "Service", Namespace: "demo", Name: "front"}}}}}
	addSyntheticFindingRepairProof(outcome)
	decision := &contracts.ReflectionDecision{LearningDecision: "updated", PlaybookIds: []string{"service-targetport-mismatch"}}
	ds, blocking, err := ReflectionCoverageGaps(root, outcome, decision)
	if err != nil || len(blocking) != 0 || len(ds) != 1 || ds[0].Rule != "reflection.detector-required" {
		t.Fatalf("missing detector gap not recorded: %v %v", ds, err)
	}
	detectors, err := layout.ReadDetectors(filepath.Join(root, layout.DiagnosticsDir))
	if err != nil {
		t.Fatal(err)
	}
	// A shared detector covers both its old playbook and the learned program.
	detectors[0].Manifest.PossiblePlaybooks = append(detectors[0].Manifest.PossiblePlaybooks, "service-targetport-mismatch")
	if err := layout.WriteDetector(root, detectors[0]); err != nil {
		t.Fatal(err)
	}
	ds, blocking, err = ReflectionCoverageGaps(root, outcome, decision)
	if err != nil || len(ds) != 0 {
		t.Fatalf("shared detector and session fixture left a gap: %v %v", ds, err)
	}
	if err := os.Remove(filepath.Join(root, ".sdo/playbooks/service-targetport-mismatch/mitigate.go")); err != nil {
		t.Fatal(err)
	}
	ds, blocking, err = ReflectionCoverageGaps(root, outcome, decision)
	if err != nil || len(ds) != 1 || ds[0].Rule != "reflection.program-required" {
		t.Fatalf("diagnose-only cause gap not recorded: %v %v", ds, err)
	}
	decision.SdkGaps = []*contracts.SdkGap{{FaultClass: "routing", Primitive: "listener", Reason: "listener capability unavailable", ScratchScript: "scratch/listener", Resources: outcome.ConfirmedRootCauses[0].Resources}}
	ds, blocking, err = ReflectionCoverageGaps(root, outcome, decision)
	if err != nil || len(ds) != 1 {
		t.Fatalf("scoped SDK gap changed the recorded gap: %v %v", ds, err)
	}
	outcome.ConfirmedRootCauses = append(outcome.ConfirmedRootCauses, &contracts.ConfirmedRootCause{Resources: []*contracts.ObjectRef{{Kind: "Service", Namespace: "demo", Name: "other"}}})
	addSyntheticFindingRepairProof(outcome)
	ds, blocking, err = ReflectionCoverageGaps(root, outcome, decision)
	if err != nil || len(ds) != 2 {
		t.Fatalf("unrelated cause gap not recorded: %v %v", ds, err)
	}
}

func TestFailedReflectionRecordsGapsOnlyForIndependentlyVerifiedPartialCauses(t *testing.T) {
	root := goodTree(t)
	verified := []*contracts.ObjectRef{{Kind: "Service", Namespace: "app", Name: "repaired"}}
	outcome := &contracts.OutcomeRecord{Classification: "failed", ConfirmedRootCauses: []*contracts.ConfirmedRootCause{{Resources: verified}, {Resources: []*contracts.ObjectRef{{Kind: "Service", Namespace: "app", Name: "still-broken"}}}}, ReflectionEligibility: &contracts.ReflectionEligibility{Version: 1, Causes: []*contracts.ReflectionCauseWork{{CauseIndex: 0, Resources: verified}}}}
	addSyntheticFindingRepairProof(outcome)
	decision := &contracts.ReflectionDecision{LearningDecision: "updated"}
	ds, _, err := ReflectionCoverageGaps(root, outcome, decision)
	if err != nil || len(ds) != 1 || ds[0].Rule != RuleReflectionProgramRequired {
		t.Fatalf("partial coverage was skipped or expanded: %v %v", ds, err)
	}
	decision.SdkGaps = []*contracts.SdkGap{{FaultClass: "routing", Primitive: "listener", Reason: "SDK capability unavailable", ScratchScript: "scratch/listener", Resources: verified}}
	ds, _, err = ReflectionCoverageGaps(root, outcome, decision)
	if err != nil || len(ds) != 1 {
		t.Fatalf("unverified cause became a gap: %v %v", ds, err)
	}
}

func TestReflectionCoverageCheckPreservesAcceptedCauseEvidence(t *testing.T) {
	root := goodTree(t)
	a := []*contracts.ObjectRef{{Kind: "Service", Namespace: "app", Name: "a"}}
	b := []*contracts.ObjectRef{{Kind: "Service", Namespace: "app", Name: "b"}}
	outcome := &contracts.OutcomeRecord{Classification: "failed", ReflectionEligibility: &contracts.ReflectionEligibility{Version: 1, Causes: []*contracts.ReflectionCauseWork{{CauseIndex: 0, Resources: a}, {CauseIndex: 1, Resources: b}}}}
	before := proto.Clone(outcome)
	_, _, err := ReflectionCoverageGaps(root, outcome, &contracts.ReflectionDecision{LearningDecision: "updated", SdkGaps: []*contracts.SdkGap{{FaultClass: "a", Primitive: "missing", Reason: "SDK gap for A", ScratchScript: "scratch/a", Resources: a}}})
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(before, outcome) {
		t.Fatal("validation changed accepted cause evidence")
	}
}

// Hand-authored controller evidence for the verified causes these tests name.
func addSyntheticFindingRepairProof(outcome *contracts.OutcomeRecord) {
	at := time.Unix(100, 0)
	for _, cause := range outcome.GetConfirmedRootCauses() {
		for _, ref := range cause.GetResources() {
			outcome.Findings = append(outcome.Findings, &contracts.Finding{DetectorId: "fixture", Status: "active", Fingerprint: ref.GetName(), PrimaryResource: ref})
			outcome.RepairActions = append(outcome.RepairActions, &contracts.RepairActionReceipt{ActionId: "fixture-" + ref.GetName(), Success: true, Resources: []*contracts.ObjectRef{ref}, StartedAt: timestamppb.New(at), CompletedAt: timestamppb.New(at.Add(time.Second))})
		}
	}
	outcome.FinalHealthDetectorState = []*contracts.DetectorEvaluation{{DetectorId: "fixture", Status: "clear", EvaluatedAt: timestamppb.New(at.Add(2 * time.Second))}}
}
