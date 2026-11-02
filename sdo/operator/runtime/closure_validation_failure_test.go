package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/validate"
)

type erringIncidentBroker struct {
	rejectingIncidentBroker
	err error
}

func (b *erringIncidentBroker) ProcessClosure(context.Context, IncidentClosure) (ClosureReceipt, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.processed++
	return ClosureReceipt{}, b.err
}

// Deterministic validation diagnostics can only repeat on resubmission, so a
// closure that fails them ends at once with the diagnostics as its reason.
// Transient failures, including a memory conflict with a newer head, retry.
func TestClosureValidationFailureEndsWithoutResubmission(t *testing.T) {
	diagnostics := validate.Err([]validate.Diagnostic{{Rule: "ownership.denied", Path: ".sdo/goal.md", Message: "actor responder may not change the goal"}})
	for _, test := range []struct {
		name    string
		err     error
		retried bool
	}{
		{"validation diagnostics", fmt.Errorf("candidate validation: %w", diagnostics), false},
		{"memory conflict", &broker.MemoryConflict{Conflict: &contracts.MemoryConflict{BaseCommit: "base", AcceptedCommit: "newer"}}, true},
		{"api error", errors.New("validator Job create: connection refused"), true},
	} {
		t.Run(test.name, func(t *testing.T) {
			now := time.Unix(1000, 0).UTC()
			incidents := &erringIncidentBroker{err: test.err}
			controller := newClosureRetryController(t, ClosureRetryPolicy{MaxAttempts: 8, InitialBackoff: time.Second, MaxBackoff: 9 * time.Second}, incidents, &now)
			var failed []ClosureFailure
			controller.OnClosureFailed = func(failure ClosureFailure) { failed = append(failed, failure) }
			attemptClosure(t, controller)
			now = now.Add(time.Hour)
			_, due := controller.PendingClosureEffect()
			state := exportState(t, controller)
			if test.retried {
				if !due || state.ClosureState != contracts.ClosureState_CLOSURE_STATE_PENDING || state.ClosureFailure.GetPermanent() {
					t.Fatalf("transient closure failure was not retried: state=%s failure=%v", state.ClosureState, state.ClosureFailure)
				}
				return
			}
			if due || incidents.processCalls() != 1 || state.ClosureState != contracts.ClosureState_CLOSURE_STATE_FAILED {
				t.Fatalf("validation failure was resubmitted: due=%v calls=%d state=%s", due, incidents.processCalls(), state.ClosureState)
			}
			failure := state.ClosureFailure
			if !failure.GetPermanent() || failure.GetAttempts() != 1 || !strings.Contains(failure.GetLastError(), "[ownership.denied]") || len(failed) != 1 {
				t.Fatalf("failed closure lacks its diagnostics: %v reported=%d", failure, len(failed))
			}
		})
	}
}

type countingIncidentBroker struct {
	IncidentBroker
	calls int
}

func (b *countingIncidentBroker) ProcessClosure(ctx context.Context, closure IncidentClosure) (ClosureReceipt, error) {
	b.calls++
	return b.IncidentBroker.ProcessClosure(ctx, closure)
}

