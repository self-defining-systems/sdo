package runtime

import (
	"slices"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// The lane 53 h6 row 004 shape: after patching the cause's Deployment, the
// responder scales the Deployment's old ReplicaSet to 0. The receipt records
// the ReplicaSet's owner chain at write time, so the write is cause work.
func ownedRolloutReflectionOutcome(owners []*contracts.ResourceOwners) *contracts.OutcomeRecord {
	base := time.Date(2026, 10, 26, 15, 18, 0, 0, time.UTC)
	deployment := &contracts.ObjectRef{ApiVersion: "apps/v1", Kind: "Deployment", Namespace: "social-network", Name: "home-timeline-service"}
	replicaSet := &contracts.ObjectRef{ApiVersion: "apps/v1", Kind: "ReplicaSet", Namespace: "social-network", Name: "home-timeline-service-68876fd868"}
	return &contracts.OutcomeRecord{
		Classification:      "success",
		ConfirmedRootCauses: []*contracts.ConfirmedRootCause{{Summary: "unavailable replicas", Resources: []*contracts.ObjectRef{deployment}}},
		Findings:            []*contracts.Finding{{DetectorId: "health-objective", Status: "active", Fingerprint: "fp-deployment", PrimaryResource: deployment}},
		RepairActions: []*contracts.RepairActionReceipt{
			{ActionId: "patch", Kind: "program-plan", Target: "Deployment/home-timeline-service", Resources: []*contracts.ObjectRef{deployment}, Summary: "patch", Details: "patch", StartedAt: timestamppb.New(base), CompletedAt: timestamppb.New(base.Add(time.Second)), Success: true},
			{ActionId: "scale-old", Kind: "kubectl-scale", Target: "ReplicaSet/home-timeline-service-68876fd868", Resources: []*contracts.ObjectRef{replicaSet}, Owners: owners, Summary: "scale old", Details: "scale old", StartedAt: timestamppb.New(base.Add(5 * time.Minute)), CompletedAt: timestamppb.New(base.Add(5*time.Minute + time.Second)), Success: true},
		},
		DetectorHistory: []*contracts.DetectorEvaluation{{DetectorId: "health-objective", Status: "clear", EvaluatedAt: timestamppb.New(base.Add(6 * time.Minute))}},
	}
}

func hasWrite(items []*contracts.ReflectionWorkItem, kind string) bool {
	return slices.ContainsFunc(items, func(item *contracts.ReflectionWorkItem) bool {
		return item.GetReason() == "unbound_write" && len(item.GetResources()) == 1 && item.GetResources()[0].GetKind() == kind
	})
}

func TestOwnedChildWriteIsCauseWorkNotUnassigned(t *testing.T) {
	owners := []*contracts.ResourceOwners{{
		Resource:   &contracts.ObjectRef{ApiVersion: "apps/v1", Kind: "ReplicaSet", Namespace: "social-network", Name: "home-timeline-service-68876fd868"},
		OwnerChain: []*contracts.ObjectRef{{ApiVersion: "apps/v1", Kind: "Deployment", Namespace: "social-network", Name: "home-timeline-service"}},
	}}
	outcome := ownedRolloutReflectionOutcome(owners)
	work := classifyCauseWork(outcome, nil)
	if len(work) != 1 || !hasWrite(work[0].GetItems(), "ReplicaSet") || !hasWrite(work[0].GetItems(), "Deployment") {
		t.Fatalf("owned ReplicaSet write is not bound to its Deployment's cause: %v", work)
	}
	if hasWrite(unassignedReflectionQuestions(outcome, nil), "ReplicaSet") {
		t.Fatal("owned ReplicaSet write is still unassigned")
	}

	unowned := ownedRolloutReflectionOutcome(nil)
	if hasWrite(classifyCauseWork(unowned, nil)[0].GetItems(), "ReplicaSet") || !hasWrite(unassignedReflectionQuestions(unowned, nil), "ReplicaSet") {
		t.Fatal("a write without a recorded owner chain must stay unassigned; names never bind")
	}
}
