//go:build integration

package runtime

import (
	"context"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"testing"
	"time"
)

func fastloopGateWait(t *testing.T, limit time.Duration, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for controller effect")
}

func fastloopGateClose(t *testing.T, ctx context.Context, controller *Controller, dispatcher *fastloopGateDispatch, result IncidentResult, at time.Time) IncidentClosure {
	t.Helper()
	closed := make(chan IncidentClosure, 1)
	controller.OnIncidentClosed = func(closure IncidentClosure) { closed <- closure }
	dispatcher.results <- result
	fastloopGateWait(t, 90*time.Second, func() bool { return len(controller.results) > 0 })
	if err := controller.Step(ctx, at.Add(time.Second), nil); err != nil {
		t.Fatal(err)
	}
	if err := controller.PersistState(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case closure := <-closed:
		return closure
	case <-time.After(time.Second):
		t.Fatal("controller did not verify and close")
		return IncidentClosure{}
	}
}

func fastloopGateProcessClosure(t *testing.T, ctx context.Context, controller *Controller, incidentID string) ClosureReceipt {
	t.Helper()
	effect, ok := controller.PendingClosureEffect()
	if !ok || effect.Closure.Request.IncidentID != incidentID {
		t.Fatalf("controller did not persist closure for %s", incidentID)
	}
	if err := controller.ExecuteClosureEffect(ctx, effect); err != nil {
		t.Fatal(err)
	}
	fastloopGateWait(t, 7*time.Minute, func() bool { return len(controller.closureResults) > 0 })
	controller.processBrokerCompletions()
	if err := controller.PersistState(ctx); err != nil {
		t.Fatal(err)
	}
	state := exportState(t, controller)
	if state.ClosureState != contracts.ClosureState_CLOSURE_STATE_COMMITTED || state.ClosureReceipt == nil {
		t.Fatalf("native broker did not commit controller closure: %s", state.ClosureState)
	}
	return *closureReceiptState(state.ClosureReceipt)
}

func fastloopGateAcknowledge(t *testing.T, ctx context.Context, controller *Controller) {
	t.Helper()
	effect, ok := controller.PendingClosureAcknowledgmentEffect()
	if !ok {
		t.Fatal("controller did not persist closure acknowledgment")
	}
	if err := controller.ExecuteClosureAcknowledgmentEffect(ctx, effect); err != nil {
		t.Fatal(err)
	}
	fastloopGateWait(t, 90*time.Second, func() bool { return len(controller.acknowledgmentResults) > 0 })
	controller.processBrokerCompletions()
	if err := controller.PersistState(ctx); err != nil {
		t.Fatal(err)
	}
}
