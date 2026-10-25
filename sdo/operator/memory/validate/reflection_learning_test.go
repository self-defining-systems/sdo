package validate

import (
	"google.golang.org/protobuf/proto"
	"os"
	"path/filepath"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/layout"
)

func TestReflectionRequiresProgramsAndDetectorForEveryVerifiedCause(t *testing.T) {
	root := goodTree(t)
	outcome := &contracts.OutcomeRecord{IncidentId: "first", Classification: "success", ConfirmedRootCauses: []*contracts.ConfirmedRootCause{{Summary: "routing fault", Resources: []*contracts.ObjectRef{{Kind: "Service", Namespace: "app", Name: "api"}}}}}
	decision := &contracts.ReflectionDecision{LearningDecision: "updated", PlaybookIds: []string{"base"}}
	ds, err := ReflectionLearningDiagnostics(root, outcome, decision)
	if err != nil || len(ds) != 1 || ds[0].Rule != "reflection.program-required" {
		t.Fatalf("README-only reflection escaped: %v %v", ds, err)
	}
	decision.LearningDecision = "no_change"
	ds, err = ReflectionLearningDiagnostics(root, outcome, decision)
	if err != nil || len(ds) != 1 {
		t.Fatalf("unlearned no-op escaped: %v %v", ds, err)
	}
	decision.DeclineReason = "evidence_gap"
	ds, err = ReflectionLearningDiagnostics(root, outcome, decision)
	if err != nil || len(ds) != 1 {
		t.Fatalf("missing captures excused missing program: %v %v", ds, err)
	}
}

func TestReflectionLearnsFromSessionFixtureWithoutControllerCapture(t *testing.T) {
	root := t.TempDir()
	copyTree(t, "../../../../tests/fixtures/sdo/playbook_program_app/.sdo", filepath.Join(root, ".sdo"))
	// No protected capture directory or provenance: these are session observations.
	outcome := &contracts.OutcomeRecord{IncidentId: "first", Classification: "success", ConfirmedRootCauses: []*contracts.ConfirmedRootCause{{Resources: []*contracts.ObjectRef{{Kind: "Service", Namespace: "demo", Name: "front"}}}}}
	decision := &contracts.ReflectionDecision{LearningDecision: "updated", PlaybookIds: []string{"service-targetport-mismatch"}}
	ds, err := ReflectionLearningDiagnostics(root, outcome, decision)
	if err != nil || len(ds) != 1 || ds[0].Rule != "reflection.detector-required" {
		t.Fatalf("missing detector escaped: %v %v", ds, err)
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
	ds, err = ReflectionLearningDiagnostics(root, outcome, decision)
	if err != nil || len(ds) != 0 {
		t.Fatalf("shared detector and session fixture rejected: %v %v", ds, err)
	}
	if err := os.Remove(filepath.Join(root, ".sdo/playbooks/service-targetport-mismatch/mitigate.go")); err != nil {
		t.Fatal(err)
	}
	ds, err = ReflectionLearningDiagnostics(root, outcome, decision)
	if err != nil || len(ds) != 1 || ds[0].Rule != "reflection.program-required" {
		t.Fatalf("diagnose-only cause accepted: %v %v", ds, err)
	}
	decision.SdkGaps = []*contracts.SdkGap{{FaultClass: "routing", Primitive: "listener", Reason: "listener capability unavailable", ScratchScript: "scratch/listener", Resources: outcome.ConfirmedRootCauses[0].Resources}}
	ds, err = ReflectionLearningDiagnostics(root, outcome, decision)
	if err != nil || len(ds) != 0 {
		t.Fatalf("scoped SDK gap rejected: %v %v", ds, err)
	}
	outcome.ConfirmedRootCauses = append(outcome.ConfirmedRootCauses, &contracts.ConfirmedRootCause{Resources: []*contracts.ObjectRef{{Kind: "Service", Namespace: "demo", Name: "other"}}})
	ds, err = ReflectionLearningDiagnostics(root, outcome, decision)
	if err != nil || len(ds) != 1 {
		t.Fatalf("SDK gap excused an unrelated cause: %v %v", ds, err)
	}
}

func TestFailedReflectionRequiresOnlyIndependentlyVerifiedPartialCauses(t *testing.T) {
	root := goodTree(t)
	verified := []*contracts.ObjectRef{{Kind: "Service", Namespace: "app", Name: "repaired"}}
	outcome := &contracts.OutcomeRecord{Classification: "failed", ConfirmedRootCauses: []*contracts.ConfirmedRootCause{{Resources: verified}, {Resources: []*contracts.ObjectRef{{Kind: "Service", Namespace: "app", Name: "still-broken"}}}}, ReflectionEligibility: &contracts.ReflectionEligibility{Version: 1, Causes: []*contracts.ReflectionCauseWork{{CauseIndex: 0, Resources: verified}}}}
	decision := &contracts.ReflectionDecision{LearningDecision: "updated"}
	ds, err := ReflectionLearningDiagnostics(root, outcome, decision)
	if err != nil || len(ds) != 1 || ds[0].Rule != RuleReflectionProgramRequired {
		t.Fatalf("partial coverage was skipped or expanded: %v %v", ds, err)
	}
	decision.SdkGaps = []*contracts.SdkGap{{FaultClass: "routing", Primitive: "listener", Reason: "SDK capability unavailable", ScratchScript: "scratch/listener", Resources: verified}}
	ds, err = ReflectionLearningDiagnostics(root, outcome, decision)
	if err != nil || len(ds) != 0 {
		t.Fatalf("unverified cause was made mandatory: %v %v", ds, err)
	}
}

func TestReflectionCoverageCheckPreservesAcceptedCauseEvidence(t *testing.T) {
	root := goodTree(t)
	a := []*contracts.ObjectRef{{Kind: "Service", Namespace: "app", Name: "a"}}
	b := []*contracts.ObjectRef{{Kind: "Service", Namespace: "app", Name: "b"}}
	outcome := &contracts.OutcomeRecord{Classification: "failed", ReflectionEligibility: &contracts.ReflectionEligibility{Version: 1, Causes: []*contracts.ReflectionCauseWork{{CauseIndex: 0, Resources: a}, {CauseIndex: 1, Resources: b}}}}
	before := proto.Clone(outcome)
	_, err := ReflectionLearningDiagnostics(root, outcome, &contracts.ReflectionDecision{LearningDecision: "updated", SdkGaps: []*contracts.SdkGap{{FaultClass: "a", Primitive: "missing", Reason: "SDK gap for A", ScratchScript: "scratch/a", Resources: a}}})
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(before, outcome) {
		t.Fatal("validation changed accepted cause evidence")
	}
}
