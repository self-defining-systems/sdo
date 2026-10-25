package lifecycleio

import (
	"context"
	"testing"
	"time"

	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/runtime/lifecyclecore"
)

func TestReflectionReviewDoesNotUseUncommittedOutcomeForLearning(t *testing.T) {
	// An effect snapshot is not the broker-accepted outcome document. Content
	// acceptance must use Check at publication, never an independent effect input.
	driver := ReflectionDriver{Repository: &broker.Broker{}, Now: func() time.Time { return time.Unix(1, 0) }}
	effect := lifecyclecore.ReflectionEffect{
		Kind: lifecyclecore.ReviewReflection, ID: "review",
		Workspace: &contracts.ProposalWorkspace{CheckoutPath: t.TempDir()},
		Decision:  &contracts.ReflectionDecision{LearningDecision: "no_change", NoChangeReason: "already covered"},
		Outcome:   &contracts.OutcomeRecord{Classification: "success", ConfirmedRootCauses: []*contracts.ConfirmedRootCause{{Resources: []*contracts.ObjectRef{{Kind: "Service", Namespace: "app", Name: "stale"}}}}},
	}
	event, err := driver.Execute(context.Background(), effect)
	if err != nil || event.Kind != lifecyclecore.ReflectionReviewed {
		t.Fatalf("review independently used a non-accepted outcome: %v %v", event, err)
	}
}
