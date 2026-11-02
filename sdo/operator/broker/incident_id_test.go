package broker

import (
	"context"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// The incident ID is interpolated into the commit message that carries the
// acceptance trailers other code trusts. An ID with a line break could append
// trailers of its own, so no broker entry point may accept one.
func TestBrokerRejectsIncidentIDsThatCouldForgeTrailers(t *testing.T) {
	ctx := context.Background()
	b, _ := judgeBroker(t)
	for _, id := range []string{"inc\nSDO-Actor: health_judge", "inc\rSDO-Phase: initial", "inc\x00", "inc x"} {
		if _, err := b.CreateProposalForPhase(ctx, id, InitialPhase); err == nil {
			t.Errorf("CreateProposalForPhase accepted %q", id)
		}
		if err := b.ReleaseProposal(id, InitialPhase); err == nil {
			t.Errorf("ReleaseProposal accepted %q", id)
		}
		if _, err := b.RecordOutcome(ctx, id, &contracts.OutcomeRecord{}); err == nil {
			t.Errorf("RecordOutcome accepted %q", id)
		}
	}
}
