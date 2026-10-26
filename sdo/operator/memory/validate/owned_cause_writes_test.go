package validate_test

import (
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/validate"
)

// ownedRolloutOutcome is the duplicate-PVC rollout-deadlock shape (lane 53
// h6 row 004): the responder patches the cause's Deployment, then scales the
// Deployment's old ReplicaSet to 0 before health clears.
func ownedRolloutOutcome(owners []*contracts.ResourceOwners, clearedAt time.Time) *contracts.OutcomeRecord {
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
		DetectorHistory: []*contracts.DetectorEvaluation{{DetectorId: "health-objective", Status: "clear", EvaluatedAt: timestamppb.New(clearedAt)}},
	}
}

func rolloutOwners(owner string) []*contracts.ResourceOwners {
	return []*contracts.ResourceOwners{{
		Resource:   &contracts.ObjectRef{ApiVersion: "apps/v1", Kind: "ReplicaSet", Namespace: "social-network", Name: "home-timeline-service-68876fd868"},
		OwnerChain: []*contracts.ObjectRef{{ApiVersion: "apps/v1", Kind: "Deployment", Namespace: "social-network", Name: owner}},
	}}
}

func TestOwnedChildWriteBindsToItsOwnerCause(t *testing.T) {
	afterChild := time.Date(2026, 10, 26, 15, 24, 0, 0, time.UTC)
	betweenWrites := time.Date(2026, 10, 26, 15, 20, 0, 0, time.UTC)
	for _, test := range []struct {
		name     string
		owners   []*contracts.ResourceOwners
		cleared  time.Time
		owned    int
		verified bool
	}{
		{name: "owner chain reaches cause", owners: rolloutOwners("home-timeline-service"), cleared: afterChild, owned: 1, verified: true},
		{name: "cause finding must clear after the child write", owners: rolloutOwners("home-timeline-service"), cleared: betweenWrites, owned: 1},
		{name: "no recorded owner chain stays unbound", cleared: betweenWrites, verified: true},
		{name: "another owner stays unbound", owners: rolloutOwners("user-service"), cleared: betweenWrites, verified: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			outcome := ownedRolloutOutcome(test.owners, test.cleared)
			cause := outcome.GetConfirmedRootCauses()[0].GetResources()
			if got := validate.OwnedCauseWrites(outcome.GetRepairActions()[1], cause); len(got) != test.owned {
				t.Fatalf("owned writes = %v, want %d", got, test.owned)
			}
			if got := validate.RepairedCauseObserved(outcome, cause); got != test.verified {
				t.Fatalf("RepairedCauseObserved = %v, want %v", got, test.verified)
			}
			if got := len(validate.VerifiedReflectionCauses(outcome)) == 1; got != test.verified {
				t.Fatalf("VerifiedReflectionCauses verified = %v, want %v", got, test.verified)
			}
		})
	}
}
