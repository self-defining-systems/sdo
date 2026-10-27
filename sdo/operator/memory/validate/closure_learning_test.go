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

// A repair must touch the cause's own resources or objects they control. A
// successful write to an unrelated Pod has no recorded owner chain, which bounds
// every cause for clearance, but it never credits a cause with a repair.
func TestUnrelatedPodDeleteCreditsNoCause(t *testing.T) {
	at := time.Unix(100, 0)
	api := &contracts.ObjectRef{Kind: "Service", Namespace: "app", Name: "api"}
	db := &contracts.ObjectRef{Kind: "Service", Namespace: "app", Name: "db"}
	outcome := &contracts.OutcomeRecord{Classification: "success", ConfirmedRootCauses: []*contracts.ConfirmedRootCause{{Resources: []*contracts.ObjectRef{api}}, {Resources: []*contracts.ObjectRef{db}}}}
	outcome.RepairActions = []*contracts.RepairActionReceipt{{
		ActionId: "delete-pod", Kind: "kubectl-delete", Success: true,
		Resources: []*contracts.ObjectRef{{Kind: "Pod", Namespace: "app", Name: "unrelated-0"}},
		StartedAt: timestamppb.New(at), CompletedAt: timestamppb.New(at.Add(time.Second)),
	}}
	if got := VerifiedReflectionCauses(outcome); len(got) != 0 {
		t.Fatalf("an unrelated pod delete credited causes with a repair: %v", got)
	}
	// The same delete with a recorded owner chain to one cause credits only it.
	outcome.RepairActions[0].Owners = []*contracts.ResourceOwners{{Resource: outcome.RepairActions[0].Resources[0], OwnerChain: []*contracts.ObjectRef{api}}}
	got := VerifiedReflectionCauses(outcome)
	if len(got) != 1 || got[0].GetCauseIndex() != 0 {
		t.Fatalf("a pod controlled by the api cause credited %v, want only cause 0", got)
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

// A verified cause whose own resources have no recorded repair write (only an
// owner-chained write to an object they control) has no transition a program
// can bind or replay, so "a program or a typed SDK gap" has no reachable exit.
// A no_recorded_repair decline records it and the replayable cause still learns.
func TestNoRecordedRepairDeclineRecordsACauseWithoutATransition(t *testing.T) {
	root := t.TempDir()
	copyTree(t, "../../../../tests/fixtures/sdo/playbook_program_app/.sdo", filepath.Join(root, ".sdo"))
	front := &contracts.ConfirmedRootCause{Resources: []*contracts.ObjectRef{{Kind: "Service", Namespace: "demo", Name: "front"}}}
	worker := &contracts.ConfirmedRootCause{Resources: []*contracts.ObjectRef{{Kind: "Deployment", Namespace: "demo", Name: "worker"}}}
	outcome := closureOutcome("success", front)
	outcome.ConfirmedRootCauses = append(outcome.ConfirmedRootCauses, worker)
	pod := &contracts.ObjectRef{Kind: "Pod", Namespace: "demo", Name: "worker-0"}
	at := time.Unix(100, 0)
	outcome.RepairActions = append(outcome.RepairActions, &contracts.RepairActionReceipt{
		ActionId: "delete-worker-pod", Kind: "kubectl-delete", Success: true, Resources: []*contracts.ObjectRef{pod},
		Owners:    []*contracts.ResourceOwners{{Resource: pod, OwnerChain: worker.Resources}},
		StartedAt: timestamppb.New(at), CompletedAt: timestamppb.New(at.Add(time.Second)),
	})
	if got := VerifiedReflectionCauses(outcome); len(got) != 2 {
		t.Fatalf("setup: both causes should be verified, got %v", got)
	}
	decision := &contracts.ReflectionDecision{LearningDecision: "updated", PlaybookIds: []string{"service-targetport-mismatch"}}
	ds, err := ReflectionLearningDiagnostics(root, outcome, decision)
	if err != nil || len(ds) != 1 || ds[0].Rule != RuleReflectionProgramRequired {
		t.Fatalf("expected the unreachable program demand for the cause without a transition: %v %v", ds, err)
	}
	decision.DeclineReason = "no_recorded_repair"
	if ds, err = ReflectionLearningDiagnostics(root, outcome, decision); err != nil || len(ds) != 0 {
		t.Fatalf("decline did not record the cause without a transition: %v %v", ds, err)
	}
	if ds := ReflectionDiagnostics(decision, []string{".sdo/playbooks/service-targetport-mismatch/README.md"}); len(ds) != 0 {
		t.Fatalf("decision with the typed decline reason was rejected: %v", ds)
	}
	decision.DeclineReason = "anything-else"
	if ds := ReflectionDiagnostics(decision, []string{".sdo/playbooks/service-targetport-mismatch/README.md"}); len(ds) == 0 {
		t.Fatal("unknown decline reason accepted")
	}
}
