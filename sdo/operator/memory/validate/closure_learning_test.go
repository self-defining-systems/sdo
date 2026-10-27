package validate

import (
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// A closure verified healthy is the single precondition for learning the
// incident's repair steps. The repeat's act-verify loop checks them again, so
// no per-cause attribution, finding clearance or ownership proof is required.
func closureOutcome(classification string, causes ...*contracts.ConfirmedRootCause) *contracts.OutcomeRecord {
	at := time.Unix(100, 0)
	outcome := &contracts.OutcomeRecord{Classification: classification, ConfirmedRootCauses: causes}
	for _, cause := range causes {
		outcome.RepairActions = append(outcome.RepairActions, &contracts.RepairActionReceipt{
			ActionId: "delete-" + cause.GetResources()[0].GetName(), Kind: "kubectl-delete", Success: true,
			Resources: cause.GetResources(), StartedAt: timestamppb.New(at), CompletedAt: timestamppb.New(at.Add(time.Second)),
		})
	}
	return outcome
}

func TestVerifiedClosureLearnsDeleteOnlyRepairWithoutAttribution(t *testing.T) {
	pvc := []*contracts.ObjectRef{{Kind: "PersistentVolumeClaim", Namespace: "app", Name: "stale-pvc"}}
	cause := &contracts.ConfirmedRootCause{Summary: "stale PVC", Resources: pvc}
	// No opening finding, no detector history, and a peer cause claiming the
	// same object: nothing attributes the repair to one cause.
	outcome := closureOutcome("success", cause, &contracts.ConfirmedRootCause{Summary: "peer", Resources: pvc})
	if got := VerifiedReflectionCauses(outcome); len(got) != 2 {
		t.Fatalf("verified closure with a recorded repair verified %d causes, want 2", len(got))
	}
}

func TestVerifiedClosureNeedsARecordedRepair(t *testing.T) {
	cause := &contracts.ConfirmedRootCause{Resources: []*contracts.ObjectRef{{Kind: "Service", Namespace: "app", Name: "api"}}}
	outcome := closureOutcome("success", cause)
	outcome.RepairActions = nil
	if got := VerifiedReflectionCauses(outcome); len(got) != 0 {
		t.Fatalf("cause without any recorded repair was verified: %v", got)
	}
	outcome = closureOutcome("success", cause)
	outcome.RepairActions[0].Success = false
	if got := VerifiedReflectionCauses(outcome); len(got) != 0 {
		t.Fatalf("failed repair was verified: %v", got)
	}
}

func TestUnverifiedClosureStillRequiresPerCauseProof(t *testing.T) {
	cause := &contracts.ConfirmedRootCause{Resources: []*contracts.ObjectRef{{Kind: "Service", Namespace: "app", Name: "api"}}}
	outcome := closureOutcome("failed", cause)
	outcome.ReflectionEligibility = &contracts.ReflectionEligibility{Version: 1, Causes: []*contracts.ReflectionCauseWork{{CauseIndex: 0, Resources: cause.Resources}}}
	if got := VerifiedReflectionCauses(outcome); len(got) != 0 {
		t.Fatalf("unverified closure learned without cleared findings: %v", got)
	}
}

// observedCauses applies the per-cause clearance contract that still governs
// trust credit and failed-closure partial learning: a recorded repair plus
// cleared opening findings. A verified closure does not need it to learn.
func observedCauses(outcome *contracts.OutcomeRecord) []*contracts.ReflectionCauseWork {
	var causes []*contracts.ReflectionCauseWork
	for index, cause := range outcome.GetConfirmedRootCauses() {
		if RepairedCauseObserved(outcome, cause.GetResources()) {
			causes = append(causes, &contracts.ReflectionCauseWork{CauseIndex: uint32(index), Resources: cause.GetResources()})
		}
	}
	return causes
}

// The reflection of a closure verified healthy saves the repair program and
// detector link with no pre-incident baseline, no per-cause attribution and a
// delete-only receipt; the learning gate passes (the executable gate is covered
// in healthy_authority_test.go).
func TestVerifiedClosureSavesProgramWithoutBaselineAttributionOrObservation(t *testing.T) {
	root := t.TempDir()
	copyTree(t, "../../../../tests/fixtures/sdo/playbook_program_app/.sdo", filepath.Join(root, ".sdo"))
	outcome := closureOutcome("success", &contracts.ConfirmedRootCause{Resources: []*contracts.ObjectRef{{Kind: "Service", Namespace: "demo", Name: "front"}}})
	decision := &contracts.ReflectionDecision{LearningDecision: "updated", PlaybookIds: []string{"service-targetport-mismatch"}}
	ds, err := ReflectionLearningDiagnostics(root, outcome, decision)
	if err != nil || len(ds) != 0 {
		t.Fatalf("verified closure with a delete-only receipt not learnable: %v %v", ds, err)
	}
	// The same evidence on a closure that did not verify healthy learns nothing.
	outcome.Classification = "failed"
	if got := VerifiedReflectionCauses(outcome); len(got) != 0 {
		t.Fatalf("unverified closure authorized learning: %v", got)
	}
}
