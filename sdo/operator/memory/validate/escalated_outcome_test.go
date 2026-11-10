package validate

import (
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	outcomeclass "sdo.dev/operator/contracts/outcome"
)

// An escalated outcome localized an out-of-reach cause from pre-repair
// controller evidence, but health never returned: it grants no repair
// learning, however its cause is classified.
func TestEscalatedOutcomeGrantsNoRepairLearning(t *testing.T) {
	store := &contracts.ObjectRef{ApiVersion: "v1", Kind: "Service", Namespace: "app", Name: "store"}
	finding := &contracts.Finding{DetectorId: "traffic-links", RuleId: "link-reachability.web.store.5432", Status: "active", PrimaryResource: store}
	cause := &contracts.ConfirmedRootCause{
		Summary: "delay on the network path to store", Resources: []*contracts.ObjectRef{store}, CauseClass: "network-delay", Reach: outcomeclass.ReachOutOfReach,
		Evidence: []*contracts.RootCauseEvidence{{Kind: "detector-finding", Source: "traffic-links", Observation: "p50 2s against 5ms healthy"}},
	}
	if !outcomeclass.OutOfReachConfirmed(cause, []*contracts.Finding{finding}, nil) {
		t.Fatal("fixture cause must be a confirmed out-of-reach cause")
	}
	record := &contracts.OutcomeRecord{
		Classification: outcomeclass.Escalated, Findings: []*contracts.Finding{finding}, ConfirmedRootCauses: []*contracts.ConfirmedRootCause{cause},
	}
	if ReflectionRepairVerified(record) || len(VerifiedReflectionCauses(record)) != 0 {
		t.Fatal("an escalated outcome was credited with a verified repair")
	}
}
