package runtime

import (
	"context"
	"testing"
)

// closeAndReflect drives one closure the way the controller does: commit the
// outcome, acknowledge it, then run any reflection queued behind the ACK.
func closeAndReflect(t *testing.T, ctx context.Context, native NativeIncidentBroker, closure IncidentClosure) (ClosureReceipt, error) {
	t.Helper()
	receipt, err := native.ProcessClosure(ctx, closure)
	if err != nil {
		return receipt, err
	}
	if err := native.AcknowledgeClosure(ctx, receipt); err != nil {
		return receipt, err
	}
	return receipt, native.ProcessQueuedReflection(ctx, closure.Request.IncidentID)
}
