package runtime

import (
	"errors"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/runtime/lifecyclecore"
)

// A closure that makes no health claim (no final detector states, no
// health-cleared time) records an incident that ended without Recovered: an
// escalated incident handing its alerts to the next one, or a terminal
// dispatch failure, or a page the responder did not verify. Its user tests still fail by definition. Such a closure
// must commit, or fail once with an actionable error; it must never wait on
// Recovered, which cannot hold, while it blocks admission of the next
// incident.
func TestAClosureWithoutAHealthClaimCommitsWhileItsUserTestsStillFail(t *testing.T) {
	cases := map[string]struct {
		setup func(w *lateWorld)
		waits bool
	}{
		"escalated handover": {func(w *lateWorld) { w.late.failing.Store(true) }, true},
		"unverified page": {func(w *lateWorld) {
			w.c.mu.Lock()
			request := w.c.currentIncidentRequest
			// The read test joined the page's alert set and still fails.
			request.Page, request.JoinedKeys, request.OpeningKeys = "synthetic page: reads fail", request.OpeningKeys, nil
			w.c.currentIncidentResult.Status = IncidentFailed
			w.c.mu.Unlock()
		}, false},
		"dispatch failure": {func(w *lateWorld) {
			w.c.mu.Lock()
			w.c.dispatchError = "synthetic dispatch failure"
			w.c.mu.Unlock()
		}, false},
	}
	for name, tc := range cases {
		setup, waits := tc.setup, tc.waits
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				w := newUserTestLateWorld(t)
				w.c.config.VerificationTimeout = 3 * time.Second
				queue := &publishingQueue{release: make(chan struct{})}
				close(queue.release)
				w.c.broker = queue
				first := w.incidentID()
				setup(w)
				settled := func() bool {
					w.c.mu.Lock()
					defer w.c.mu.Unlock()
					return len(w.closed) > 0 && w.c.pendingClosure == nil
				}
				w.drive(60, settled)
				if !settled() {
					w.c.mu.Lock()
					state := w.c.closureState
					w.c.mu.Unlock()
					t.Fatalf("the closure never committed while its user test still failed: closed=%d closure state=%s", len(w.closed), state)
				}
				if closed := w.closed[0]; closed.Request.IncidentID != first || len(closed.FinalDetectorStates) != 0 || closed.HealthClearedAt != nil {
					t.Fatalf("want an unrecovered close of the first incident: %+v", closed)
				}
				if waits {
					w.drive(10, func() bool { return w.incidentID() != "" && w.incidentID() != first })
					if w.incidentID() == "" || w.incidentID() == first {
						t.Fatal("the waiting opener was never admitted after the closure committed")
					}
				}
			})
		})
	}
}

// newUserTestLateWorld is a late world whose first incident opened on a user
// test that keeps failing.
func newUserTestLateWorld(t *testing.T) *lateWorld {
	t.Helper()
	opener := &switchedProbe{id: memory.TrafficDetectorPrefix + "opener", scenario: "read"}
	opener.failing.Store(true)
	return newLateWorldOpenedBy(t, false, opener)
}

// An unconfirmed decision on a closure that makes no health claim changes
// nothing in the reducer. The driver reports that as an error and commits the
// closure as recorded, so a recheck that cannot transition is loud once
// rather than repeated on every pass.
func TestAnUnconfirmedDecisionThatChangesNothingIsAnError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newUserTestLateWorld(t)
		w.c.config.VerificationTimeout = 3 * time.Second
		w.late.failing.Store(true)
		w.runUntil(30, func() bool { return len(w.closed) > 0 })
		w.c.mu.Lock()
		defer w.c.mu.Unlock()
		if w.c.pendingClosure == nil || claimsHealth(*w.c.pendingClosure) {
			t.Fatalf("want a pending closure without a health claim: %+v", w.c.pendingClosure)
		}
		_, err := w.c.stepClosureLocked(lifecyclecore.Event{Kind: lifecyclecore.Unconfirmed, At: w.now, Error: "synthetic",
			UnconfirmedReason: contracts.ClosureUnconfirmedReason_CLOSURE_UNCONFIRMED_REASON_OBSERVATION_INCOMPLETE})
		if !errors.Is(err, errUnconfirmedNoop) || !strings.Contains(err.Error(), w.c.pendingClosure.Request.IncidentID) {
			t.Fatalf("an unconfirmed decision that changed nothing returned %v", err)
		}
	})
}
