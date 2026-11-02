package validate

import (
	"slices"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

// The responder patches the cause's Deployment, then scales its old
// ReplicaSet. The receipt carries no owner chain (`sdo act` reads none); the
// open capture shows the ReplicaSet controlled by the Deployment.
func observedRolloutOutcome(base, cleared time.Time, written string) *contracts.OutcomeRecord {
	deployment := &contracts.ObjectRef{ApiVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "search"}
	replicaSet := &contracts.ObjectRef{ApiVersion: "apps/v1", Kind: "ReplicaSet", Namespace: "app", Name: written}
	return &contracts.OutcomeRecord{
		Classification:      "success",
		ConfirmedRootCauses: []*contracts.ConfirmedRootCause{{Summary: "unavailable replicas", Resources: []*contracts.ObjectRef{deployment}}},
		Findings:            []*contracts.Finding{{DetectorId: "health-objective", Status: "active", Fingerprint: "fp-search", PrimaryResource: deployment}},
		RepairActions: []*contracts.RepairActionReceipt{
			{ActionId: "patch", Resources: []*contracts.ObjectRef{deployment}, StartedAt: timestamppb.New(base), CompletedAt: timestamppb.New(base.Add(time.Second)), Success: true},
			{ActionId: "sdo-act-scale-old", Resources: []*contracts.ObjectRef{replicaSet}, StartedAt: timestamppb.New(base.Add(time.Minute)), CompletedAt: timestamppb.New(base.Add(time.Minute + time.Second)), Success: true},
		},
		DetectorHistory: []*contracts.DetectorEvaluation{{DetectorId: "health-objective", Status: "clear", EvaluatedAt: timestamppb.New(cleared)}},
	}
}

// A learned program binds the cause's Deployment and reaches the old
// ReplicaSet through its owner relation: the SDO-observed owned write moves
// the cause's clearance boundary but never becomes a binding by name. On a
// base without observed owners the between-writes clearance verifies, so the
// test fails first; adding owned writes to the binding targets also fails it.
func TestObservedOwnedChildWriteNeedsNoProgramBinding(t *testing.T) {
	base := time.Date(2026, 10, 26, 15, 18, 0, 0, time.UTC)
	capture := writeRolloutCapture(t, base.Add(-time.Minute), []corev1.Pod{pod("search-old-1", "search-old")}, nil)
	bindings := map[string]sdk.ObjectRef{"deployment": {APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "search"}}
	for _, test := range []struct {
		name     string
		cleared  time.Time
		verified bool
	}{
		{name: "clears after the owned child write", cleared: base.Add(2 * time.Minute), verified: true},
		{name: "clears between the writes", cleared: base.Add(30 * time.Second)},
	} {
		t.Run(test.name, func(t *testing.T) {
			outcome := observedRolloutOutcome(base, test.cleared, "search-old")
			ObservedRepairOwners(outcome, capture)
			cause := outcome.GetConfirmedRootCauses()[0].GetResources()
			if !bindingsCoverRepairs(outcome, bindings, cause) {
				t.Fatal("a Deployment-bound program must cover a cause whose repair also wrote an owned ReplicaSet")
			}
			if got := len(observedCauses(outcome)) == 1; got != test.verified {
				t.Fatalf("verified = %v, want %v: the owned child write must bound the cause's clearance", got, test.verified)
			}
		})
	}
}

func TestObservedRepairOwnersComeFromTheOpenCapture(t *testing.T) {
	base := time.Date(2026, 10, 26, 15, 18, 0, 0, time.UTC)
	capture := writeRolloutCapture(t, base.Add(-time.Minute), []corev1.Pod{pod("search-old-1", "search-old")}, nil)
	deployment := &contracts.ObjectRef{ApiVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "search"}
	for _, test := range []struct {
		name  string
		write *contracts.ObjectRef
		chain []string
		owned bool
	}{
		{name: "observed ReplicaSet binds to its Deployment", write: &contracts.ObjectRef{ApiVersion: "apps/v1", Kind: "ReplicaSet", Namespace: "app", Name: "search-old"}, chain: []string{"Deployment/search"}, owned: true},
		{name: "observed Pod chain follows each controller", write: &contracts.ObjectRef{ApiVersion: "v1", Kind: "Pod", Namespace: "app", Name: "search-old-1"}, chain: []string{"ReplicaSet/search-old", "Deployment/search"}, owned: true},
		{name: "another owner stays unbound", write: &contracts.ObjectRef{ApiVersion: "apps/v1", Kind: "ReplicaSet", Namespace: "app", Name: "other-old"}, chain: []string{"Deployment/other"}},
		{name: "an unobserved object gets no chain", write: &contracts.ObjectRef{ApiVersion: "apps/v1", Kind: "ReplicaSet", Namespace: "app", Name: "search-7f9c"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			outcome := &contracts.OutcomeRecord{RepairActions: []*contracts.RepairActionReceipt{{ActionId: "sdo-act-1", Resources: []*contracts.ObjectRef{test.write}, Success: true}}}
			ObservedRepairOwners(outcome, capture)
			var chain []string
			for _, owners := range outcome.GetRepairActions()[0].GetOwners() {
				for _, owner := range owners.GetOwnerChain() {
					chain = append(chain, owner.GetKind()+"/"+owner.GetName())
				}
			}
			if !slices.Equal(chain, test.chain) {
				t.Fatalf("chain = %v, want %v", chain, test.chain)
			}
			if got := len(OwnedCauseWrites(outcome.GetRepairActions()[0], []*contracts.ObjectRef{deployment})) == 1; got != test.owned {
				t.Fatalf("owned = %v, want %v", got, test.owned)
			}
		})
	}
	// A chain the controller recorded from plan pre-images is kept as is.
	replicaSet := &contracts.ObjectRef{ApiVersion: "apps/v1", Kind: "ReplicaSet", Namespace: "app", Name: "search-old"}
	preImage := &contracts.ResourceOwners{Resource: replicaSet, OwnerChain: []*contracts.ObjectRef{{ApiVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "search"}}}
	outcome := &contracts.OutcomeRecord{RepairActions: []*contracts.RepairActionReceipt{{ActionId: "plan", Resources: []*contracts.ObjectRef{replicaSet}, Owners: []*contracts.ResourceOwners{preImage}, Success: true}}}
	ObservedRepairOwners(outcome, capture)
	if owners := outcome.GetRepairActions()[0].GetOwners(); len(owners) != 1 || owners[0] != preImage {
		t.Fatalf("pre-image chain replaced: %v", owners)
	}
}

// REV14A F1: the rollout creates ReplicaSet search-7f9c after the incident
// opened, so no open phase observed it and it gets no owner chain. Its write
// can still be part of any cause's repair, so an unchained Pod/ReplicaSet/Job
// write moves every cause's clearance boundary: a clearance between the
// Deployment patch and that write must not verify. On LEARN2B's base it does.
func TestUnchainedControlledWriteBoundsEveryCause(t *testing.T) {
	base := time.Date(2026, 10, 26, 15, 18, 0, 0, time.UTC)
	capture := writeRolloutCapture(t, base.Add(-time.Minute), []corev1.Pod{pod("search-old-1", "search-old")}, nil)
	for _, test := range []struct {
		name     string
		cleared  time.Time
		verified bool
	}{
		{name: "clears after the post-open write", cleared: base.Add(2 * time.Minute), verified: true},
		{name: "clears between the writes", cleared: base.Add(30 * time.Second)},
	} {
		t.Run(test.name, func(t *testing.T) {
			outcome := observedRolloutOutcome(base, test.cleared, "search-7f9c")
			ObservedRepairOwners(outcome, capture)
			if owners := outcome.GetRepairActions()[1].GetOwners(); len(owners) != 0 {
				t.Fatalf("post-open ReplicaSet got an owner chain: %v", owners)
			}
			if got := len(observedCauses(outcome)) == 1; got != test.verified {
				t.Fatalf("verified = %v, want %v: an unchained controlled write must bound every cause's clearance", got, test.verified)
			}
		})
	}
}
