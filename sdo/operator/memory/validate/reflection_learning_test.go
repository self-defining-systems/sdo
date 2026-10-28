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
	ds := ReflectionCoverageGaps(root, outcome, decision)
	if len(ds) != 1 || ds[0].Rule != "reflection.program-required" {
		t.Fatalf("README-only reflection gap not recorded: %v", ds)
	}
	decision.LearningDecision = "no_change"
	ds = ReflectionCoverageGaps(root, outcome, decision)
	if len(ds) != 1 {
		t.Fatalf("unlearned no-op gap not recorded: %v", ds)
	}
	decision.DeclineReason = "no_recorded_repair"
	ds = ReflectionCoverageGaps(root, outcome, decision)
	if len(ds) != 1 {
		t.Fatalf("decline reason hid the gap: %v", ds)
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
	ds := ReflectionCoverageGaps(root, outcome, decision)
	if len(ds) != 0 {
		t.Fatalf("a finding that binds the program's roles must link it without a detector list: %v", ds)
	}
	// A finding on another object cannot bind the program.
	bound := outcome.Findings
	other := &contracts.Finding{DetectorId: "fixture", Status: "active", Fingerprint: "other", PrimaryResource: &contracts.ObjectRef{Kind: "Service", Namespace: "demo", Name: "other"}}
	outcome.Findings = []*contracts.Finding{other}
	ds = ReflectionCoverageGaps(root, outcome, decision)
	if len(ds) != 1 || ds[0].Rule != "reflection.detector-required" {
		t.Fatalf("unlinked program gap not recorded: %v", ds)
	}
	detectors, err := layout.ReadDetectors(filepath.Join(root, layout.DiagnosticsDir))
	if err != nil {
		t.Fatal(err)
	}
	// Listing the playbook does not bind its roles: the automatic stage would
	// diagnose it unbound on every repeat, so the gap stays.
	detectors[0].Manifest.PossiblePlaybooks = append(detectors[0].Manifest.PossiblePlaybooks, "service-targetport-mismatch")
	if err := layout.WriteDetector(root, detectors[0]); err != nil {
		t.Fatal(err)
	}
	ds = ReflectionCoverageGaps(root, outcome, decision)
	if len(ds) != 1 || ds[0].Rule != "reflection.detector-required" {
		t.Fatalf("a detector list without role bindings hid the gap: %v", ds)
	}
	// The finding's explicit role binding links it.
	other.ParameterBindings = map[string]*contracts.ObjectRef{"service": {Kind: "Service", Namespace: "demo", Name: "front"}}
	ds = ReflectionCoverageGaps(root, outcome, decision)
	other.ParameterBindings = nil
	outcome.Findings = bound
	if len(ds) != 0 {
		t.Fatalf("a finding that binds the role left a gap: %v", ds)
	}
	if err := os.Remove(filepath.Join(root, ".sdo/playbooks/service-targetport-mismatch/mitigate.go")); err != nil {
		t.Fatal(err)
	}
	ds = ReflectionCoverageGaps(root, outcome, decision)
	if len(ds) != 1 || ds[0].Rule != "reflection.program-required" {
		t.Fatalf("diagnose-only cause gap not recorded: %v", ds)
	}
	decision.SdkGaps = []*contracts.SdkGap{{FaultClass: "routing", Primitive: "listener", Reason: "listener capability unavailable", ScratchScript: "scratch/listener", Resources: outcome.ConfirmedRootCauses[0].Resources}}
	ds = ReflectionCoverageGaps(root, outcome, decision)
	if len(ds) != 1 {
		t.Fatalf("scoped SDK gap changed the recorded gap: %v", ds)
	}
	outcome.ConfirmedRootCauses = append(outcome.ConfirmedRootCauses, &contracts.ConfirmedRootCause{Resources: []*contracts.ObjectRef{{Kind: "Service", Namespace: "demo", Name: "other"}}})
	addSyntheticFindingRepairProof(outcome)
	ds = ReflectionCoverageGaps(root, outcome, decision)
	if len(ds) != 2 {
		t.Fatalf("unrelated cause gap not recorded: %v", ds)
	}
}

// The reflection link is the automatic stage's binding rule (program.BindFinding):
// a learned program is linked only when one incident finding binds every role
// its recorded case binds to the same object. Every other shape leaves the
// stage diagnosing the program unbound on every repeat (the webhook CA program
// whose detector listed it but bound only the Namespace never ran).
func TestReflectionDetectorLinkIsTheStageBindingRule(t *testing.T) {
	root := t.TempDir()
	copyTree(t, "../../../../tests/fixtures/sdo/playbook_program_app/.sdo", filepath.Join(root, ".sdo"))
	detectors, err := layout.ReadDetectors(filepath.Join(root, layout.DiagnosticsDir))
	if err != nil {
		t.Fatal(err)
	}
	for _, detector := range detectors {
		if !slices.Contains(detector.Manifest.PossiblePlaybooks, "service-targetport-mismatch") {
			detector.Manifest.PossiblePlaybooks = append(detector.Manifest.PossiblePlaybooks, "service-targetport-mismatch")
		}
		if err := layout.WriteDetector(root, detector); err != nil {
			t.Fatal(err)
		}
	}
	front := &contracts.ObjectRef{Kind: "Service", Namespace: "demo", Name: "front"}
	peer := &contracts.ObjectRef{Kind: "Service", Namespace: "demo", Name: "peer"}
	namespace := &contracts.ObjectRef{ApiVersion: "v1", Kind: "Namespace", Name: "demo"}
	for _, tc := range []struct {
		name    string
		finding *contracts.Finding
		linked  bool
	}{
		{"primary", &contracts.Finding{PrimaryResource: front}, true},
		{"unique related", &contracts.Finding{PrimaryResource: namespace, RelatedResources: []*contracts.ObjectRef{front}}, true},
		{"parameter binding", &contracts.Finding{PrimaryResource: namespace, ParameterBindings: map[string]*contracts.ObjectRef{"service": front}}, true},
		{"listed playbook only", &contracts.Finding{PrimaryResource: namespace, PlaybookIds: []string{"service-targetport-mismatch"}}, false},
		{"ambiguous kind", &contracts.Finding{PrimaryResource: peer, RelatedResources: []*contracts.ObjectRef{front}}, false},
		{"other object", &contracts.Finding{PrimaryResource: peer}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			outcome := &contracts.OutcomeRecord{IncidentId: "first", Classification: "success", ConfirmedRootCauses: []*contracts.ConfirmedRootCause{{Resources: []*contracts.ObjectRef{front}}}}
			addSyntheticFindingRepairProof(outcome)
			tc.finding.DetectorId, tc.finding.Status, tc.finding.Fingerprint = "fixture", "active", "case"
			outcome.Findings = []*contracts.Finding{tc.finding}
			ds := ReflectionCoverageGaps(root, outcome, &contracts.ReflectionDecision{LearningDecision: "updated", PlaybookIds: []string{"service-targetport-mismatch"}})
			if tc.linked != (len(ds) == 0) || (!tc.linked && (len(ds) != 1 || ds[0].Rule != RuleReflectionDetectorRequired)) {
				t.Fatalf("linked=%v, gaps=%v", tc.linked, ds)
			}
		})
	}
}

func TestFailedReflectionRecordsGapsOnlyForIndependentlyVerifiedPartialCauses(t *testing.T) {
	root := goodTree(t)
	verified := []*contracts.ObjectRef{{Kind: "Service", Namespace: "app", Name: "repaired"}}
	outcome := &contracts.OutcomeRecord{Classification: "failed", ConfirmedRootCauses: []*contracts.ConfirmedRootCause{{Resources: verified}, {Resources: []*contracts.ObjectRef{{Kind: "Service", Namespace: "app", Name: "still-broken"}}}}, ReflectionEligibility: &contracts.ReflectionEligibility{Version: 1, Causes: []*contracts.ReflectionCauseWork{{CauseIndex: 0, Resources: verified}}}}
	addSyntheticFindingRepairProof(outcome)
	decision := &contracts.ReflectionDecision{LearningDecision: "updated"}
	ds := ReflectionCoverageGaps(root, outcome, decision)
	if len(ds) != 1 || ds[0].Rule != RuleReflectionProgramRequired {
		t.Fatalf("partial coverage was skipped or expanded: %v", ds)
	}
	decision.SdkGaps = []*contracts.SdkGap{{FaultClass: "routing", Primitive: "listener", Reason: "SDK capability unavailable", ScratchScript: "scratch/listener", Resources: verified}}
	ds = ReflectionCoverageGaps(root, outcome, decision)
	if len(ds) != 1 {
		t.Fatalf("unverified cause became a gap: %v", ds)
	}
}

func TestReflectionCoverageCheckPreservesAcceptedCauseEvidence(t *testing.T) {
	root := goodTree(t)
	a := []*contracts.ObjectRef{{Kind: "Service", Namespace: "app", Name: "a"}}
	b := []*contracts.ObjectRef{{Kind: "Service", Namespace: "app", Name: "b"}}
	outcome := &contracts.OutcomeRecord{Classification: "failed", ReflectionEligibility: &contracts.ReflectionEligibility{Version: 1, Causes: []*contracts.ReflectionCauseWork{{CauseIndex: 0, Resources: a}, {CauseIndex: 1, Resources: b}}}}
	before := proto.Clone(outcome)
	ReflectionCoverageGaps(root, outcome, &contracts.ReflectionDecision{LearningDecision: "updated", SdkGaps: []*contracts.SdkGap{{FaultClass: "a", Primitive: "missing", Reason: "SDK gap for A", ScratchScript: "scratch/a", Resources: a}}})
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
