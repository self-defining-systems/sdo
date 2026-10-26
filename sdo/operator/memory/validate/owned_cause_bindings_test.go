package validate

import (
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

// A learned program binds the cause's Deployment and reaches the old
// ReplicaSet through its owner relation, so the corpus never needs the
// ReplicaSet's generated name as a binding.
func TestOwnedChildWriteNeedsNoProgramBinding(t *testing.T) {
	base := time.Date(2026, 10, 26, 15, 18, 0, 0, time.UTC)
	deployment := &contracts.ObjectRef{ApiVersion: "apps/v1", Kind: "Deployment", Namespace: "social-network", Name: "home-timeline-service"}
	replicaSet := &contracts.ObjectRef{ApiVersion: "apps/v1", Kind: "ReplicaSet", Namespace: "social-network", Name: "home-timeline-service-68876fd868"}
	outcome := &contracts.OutcomeRecord{RepairActions: []*contracts.RepairActionReceipt{
		{ActionId: "patch", Resources: []*contracts.ObjectRef{deployment}, StartedAt: timestamppb.New(base), CompletedAt: timestamppb.New(base.Add(time.Second)), Success: true},
		{ActionId: "scale-old", Resources: []*contracts.ObjectRef{replicaSet}, Owners: []*contracts.ResourceOwners{{Resource: replicaSet, OwnerChain: []*contracts.ObjectRef{deployment}}}, StartedAt: timestamppb.New(base.Add(time.Minute)), CompletedAt: timestamppb.New(base.Add(time.Minute + time.Second)), Success: true},
	}}
	bindings := map[string]sdk.ObjectRef{"deployment": {APIVersion: "apps/v1", Kind: "Deployment", Namespace: "social-network", Name: "home-timeline-service"}}
	if !bindingsCoverRepairs(outcome, bindings, []*contracts.ObjectRef{deployment}) {
		t.Fatal("a Deployment-bound program must cover a cause whose repair also wrote an owned ReplicaSet")
	}
	_, after, _ := causeRepairs(outcome, []*contracts.ObjectRef{deployment})
	if !after.Equal(base.Add(time.Minute + time.Second)) {
		t.Fatalf("repair boundary %v ignores the owned child write", after)
	}
}
