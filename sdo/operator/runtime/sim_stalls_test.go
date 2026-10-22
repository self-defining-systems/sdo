package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/runtime/internal/lifecycleio"
)

// The broker edge reports its expired drain budget as DeadlineExceeded. The
// virtual test releases that edge explicitly; no real-time timer drives policy.
type simDrainBroker struct {
	*simBroker
	blocked chan struct{}
	expire  chan struct{}
	cause   error
}

func (b *simDrainBroker) ProcessClosure(ctx context.Context, closure IncidentClosure) (ClosureReceipt, error) {
	select {
	case b.blocked <- struct{}{}:
	default:
	}
	select {
	case <-ctx.Done():
		return ClosureReceipt{}, ctx.Err()
	case <-b.expire:
		return ClosureReceipt{}, b.cause
	}
}

func simAdvanceToVerifiedClosure(t *testing.T, w *simWorld) ClosureEffect {
	t.Helper()
	for tick := 0; tick < 300; tick++ {
		w.clock.now = w.clock.now.Add(time.Second)
		w.step()
		if effect, ok := w.controller.PendingClosureEffect(); ok {
			return effect
		}
		ctx := context.Background()
		if effect, ok := w.controller.PendingWorkspaceEffect(); ok {
			if err := w.controller.ExecuteWorkspaceEffect(ctx, effect); err != nil {
				t.Fatal(err)
			}
			w.await(func() bool { return len(w.controller.workspaceResults) > 0 })
		} else if effect, ok := w.controller.PendingDispatchEffect(); ok {
			if err := w.controller.ExecuteDispatchEffect(ctx, effect); err != nil {
				t.Fatal(err)
			}
			w.await(func() bool { return len(w.controller.results) > 0 })
		}
	}
	t.Fatal("incident never reached a verified closure")
	return ClosureEffect{}
}

func TestSimVerifiedClosureDrainFailureAdmitsNextIncident(t *testing.T) {
	for _, lost := range []bool{false, true} {
		name := "drain-timeout"
		if lost {
			name = "controller-lost"
		}
		t.Run(name, func(t *testing.T) {
			w := newSimWorld(t, 37)
			w.client.ReactionChain = w.client.ReactionChain[2:]
			w.responder.behavior = func() simBehavior { return simFixAll }
			w.broker.reject = func() bool { return false }
			w.setPort(simServiceName(0), 9999)
			effect := simAdvanceToVerifiedClosure(t, w)
			if len(effect.Closure.FinalDetectorStates) == 0 {
				t.Fatal("predecessor was not verified")
			}
			cause := error(context.DeadlineExceeded)
			if lost {
				cause = errors.New("controller lost after health verification")
			}
			stalled := &simDrainBroker{simBroker: w.broker, blocked: make(chan struct{}, 1), expire: make(chan struct{}), cause: cause}
			if err := w.controller.SetIncidentBroker(stalled); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if err := w.controller.ExecuteClosureEffect(ctx, effect); err != nil {
				t.Fatal(err)
			}
			w.await(func() bool { return len(stalled.blocked) > 0 })
			if lost {
				// The old process stops during the broker effect. A new one restores the
				// same durable incident; cancellation alone must not consume retries.
				old := w.controller
				cancel()
				w.await(func() bool { return len(old.closureResults) > 0 })
				w.restart()
				if err := w.controller.SetIncidentBroker(stalled); err != nil {
					t.Fatal(err)
				}
			}
			close(stalled.expire)
			if !lost {
				w.await(func() bool { return len(w.controller.closureResults) > 0 })
			}
			for tick := 0; tick < 600; tick++ {
				w.clock.now = w.clock.now.Add(time.Second)
				w.step()
				w.drain()
				if failure, ok := w.controller.ClosureFailure(); ok && failure.Permanent {
					break
				}
			}
			failure, ok := w.controller.ClosureFailure()
			if !ok || !failure.Permanent || failure.IncidentID != effect.Closure.Request.IncidentID || !strings.Contains(failure.LastError, cause.Error()) {
				t.Fatalf("stall did not retain its terminal cause: %+v", failure)
			}
			// Verify evidence at the durable archive edge, rather than the callback.
			archives, err := w.client.CoreV1().ConfigMaps("app").List(context.Background(), metav1.ListOptions{LabelSelector: "sdo.dev/closure-archive=failed"})
			if err != nil || len(archives.Items) != 1 {
				t.Fatalf("failed closure archive: %d, %v", len(archives.Items), err)
			}
			record := &contracts.FailedIncidentClosure{}
			if err := wire.Unmarshal([]byte(archives.Items[0].Data[lifecycleio.FailedClosureDataKey]), record); err != nil {
				t.Fatal(err)
			}
			if record.GetFailure().GetLastError() != failure.LastError || record.GetClosure().GetRequest().GetIncidentId() != failure.IncidentID {
				t.Fatal("archive lost incident or cause")
			}
			if err := w.controller.SetIncidentBroker(w.broker); err != nil {
				t.Fatal(err)
			}
			w.restart()
			before := w.responder.calls
			w.setPort(simServiceName(1), 9999)
			for tick := 0; tick < 300 && (w.responder.calls == before || !w.idle()); tick++ {
				w.clock.now = w.clock.now.Add(time.Second)
				w.step()
				w.drain()
			}
			if w.responder.calls == before || !w.idle() {
				t.Fatal("failed drain blocked next incident")
			}
			last := w.closed[len(w.closed)-1]
			if last.Request.IncidentID == failure.IncidentID || len(last.FinalDetectorStates) == 0 {
				t.Fatal("next incident was not independently verified")
			}
			if len(w.violations) > 0 {
				t.Fatal(w.violations)
			}
		})
	}
}
