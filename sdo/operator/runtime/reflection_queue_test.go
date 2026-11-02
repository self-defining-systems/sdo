package runtime

import (
	"context"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

type queuedReflectionBroker interface {
	ProcessClosure(context.Context, IncidentClosure) (ClosureReceipt, error)
	AcknowledgeClosure(context.Context, ClosureReceipt) error
	ProcessQueuedReflection(context.Context, string) error
	readLedger(string) (*contracts.BrokerLedger, error)
}

// closeAndReflect drives one closure the way the controller does: commit the
// outcome, acknowledge it, then run any reflection queued behind the ACK.
func closeAndReflect(t *testing.T, ctx context.Context, native queuedReflectionBroker, closure IncidentClosure) (ClosureReceipt, error) {
	t.Helper()
	receipt, err := native.ProcessClosure(ctx, closure)
	if err != nil {
		return receipt, err
	}
	ledger, err := native.readLedger(closure.Request.IncidentID)
	if err != nil {
		return receipt, err
	}
	if !ledger.Acknowledged {
		if err := native.AcknowledgeClosure(ctx, receipt); err != nil {
			return receipt, err
		}
	}
	return receipt, native.ProcessQueuedReflection(ctx, closure.Request.IncidentID)
}

