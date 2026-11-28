package runtime

import (
	"context"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
)

// latePagedEvents are the alert_paged lifecycle events the controller recorded.
type latePagedEvents struct{ events []*contracts.LifecycleEvent }

func (s *latePagedEvents) sink() LifecycleSink {
	return lifecycleSinkFunc(func(event *contracts.LifecycleEvent) {
		if event.GetEvent() == PhaseAlertPaged {
			s.events = append(s.events, event)
		}
	})
}

// The sequence from a three-fault run: incident A opened on one journey, a
// second joined it inside the join window, and that joined journey keeps
// failing again, so A never stays clear. Past the join window, while A's
// responder still works, a third journey outside A's alert set starts
// failing. It pages at once on A, without joining A's alert set or
// re-prompting A's agent. When A's responder returns with its alert set still
// failing, A closes escalated and the next incident opens on the late journey.
func TestAUserTestFailingAfterTheJoinWindowPagesAtOnceWhileTheIncidentStaysOpen(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		now := time.Unix(1000, 0).UTC()
		search := &switchedProbe{id: memory.TrafficDetectorPrefix + "search", scenario: "search"}
		reserve := &switchedProbe{id: memory.TrafficDetectorPrefix + "reserve", scenario: "reserve"}
		login := &switchedProbe{id: memory.TrafficDetectorPrefix + "login", scenario: "login"}
		c, _ := alertSetController(t, now, search, reserve, login)
		c.config.VerificationTimeout = 3 * time.Second
		w := &lateWorld{t: t, c: c, now: now, late: login}
		c.now = func() time.Time { return w.now }
		c.OnIncidentClosed = func(closure IncidentClosure) { w.closed = append(w.closed, closure) }
		dispatcher := c.dispatcher.(*recordingDispatcher)
		paged := &latePagedEvents{}
		c.SetLifecycleSink(paged.sink())
		first := w.incidentID()
		// A's responder is still working, and A is past its join window.
		c.responderDone, c.dispatchState = false, contracts.DispatchState_DISPATCH_STATE_RUNNING
		c.responderCompletedAt, c.currentIncidentResult = time.Time{}, nil
		c.incidentDetectedAt = now.Add(-AlertJoinWindow - time.Minute)
		joined := reserve.finding()
		c.currentIncidentRequest.JoinedKeys = findingKeys([]sdk.Finding{joined})
		c.currentIncidentRequest.Findings = append(c.currentIncidentRequest.Findings, joined)

		// The joined journey re-fails and the new one fails in the same pass.
		reserve.failing.Store(true)
		login.failing.Store(true)
		pagedAt := w.now
		w.step()
		request := c.currentIncidentRequest
		if request == nil || request.IncidentID != first {
			t.Fatalf("incident A must stay open while a member of its alert set fails: %+v", request)
		}
		loginKey := findingKey(login.finding())
		if got := request.LatePages; len(got) != 1 || got[0].Key != loginKey || !got[0].PagedAt.Equal(pagedAt) {
			t.Fatalf("the late journey did not page at once: late pages %+v", got)
		}
		if len(paged.events) != 1 || !slices.Equal(paged.events[0].GetFindingKeys(), []string{loginKey}) || paged.events[0].GetIncidentId() != first {
			t.Fatalf("alert_paged events = %v", paged.events)
		}
		if slices.Contains(alertKeys(request), loginKey) {
			t.Fatalf("a late journey joined the alert set: %v", alertKeys(request))
		}

		// A stays open while its responder works; the page fires once, and the
		// agent is never prompted for it.
		w.runUntil(10, func() bool { return false })
		if got := c.currentIncidentRequest; got == nil || got.IncidentID != first || len(got.LatePages) != 1 || len(paged.events) != 1 {
			t.Fatalf("after more passes: request %+v, events %d", got, len(paged.events))
		}
		if calls := dispatcher.callCount(); calls != 0 {
			t.Fatalf("a late page dispatched the agent %d times", calls)
		}

		// A's responder returns while its alert set still fails: A stays open
		// and a fresh session starts on it (SDO never escalates); the late
		// journey, past A's window, keeps waiting for the next incident.
		result := completedResult(first)
		c.mu.Lock()
		c.responderDone, c.dispatchState, c.responderCompletedAt, c.currentIncidentResult = true, contracts.DispatchState_DISPATCH_STATE_COMPLETED, w.now, &result
		c.mu.Unlock()
		w.drive(60, func() bool { return c.currentIncidentRequest != nil && c.currentIncidentRequest.SessionGeneration > 0 })
		if len(w.closed) != 0 || w.incidentID() != first || c.currentIncidentRequest.SessionGeneration != 1 || len(c.currentIncidentRequest.LatePages) != 1 {
			t.Fatalf("incident A did not stay open for a fresh session: closed %+v, request %+v", w.closed, c.currentIncidentRequest)
		}
		if !slices.ContainsFunc(c.batcher.Snapshot().Findings, func(f sdk.Finding) bool { return findingKey(f) == loginKey }) {
			t.Fatalf("the late journey was lost: batch %v", c.batcher.Snapshot().Findings)
		}
	})
}

// Only a user test or failing user traffic pages late. A Kubernetes condition
// or a clue waits as before, and a user test inside the window, while the
// responder works, joins the alert set instead of paging.
func TestOnlyUserFacingFailuresPageLate(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	opening := &switchedProbe{id: memory.TrafficDetectorPrefix + "opening", scenario: "ssh"}
	c, _ := alertSetController(t, now, opening)
	c.responderDone = false
	paged := &latePagedEvents{}
	c.SetLifecycleSink(paged.sink())
	ctx := context.Background()
	inside := (&switchedProbe{id: memory.TrafficDetectorPrefix + "inside", scenario: "clone"}).finding()
	c.admitActivation(ctx, inside, c.incidentDetectedAt.Add(AlertJoinWindow), sdk.DetectorSpec{})
	if len(c.currentIncidentRequest.LatePages) != 0 {
		t.Fatalf("a user test inside the join window paged late: %+v", c.currentIncidentRequest.LatePages)
	}
	late := c.incidentDetectedAt.Add(AlertJoinWindow + time.Second)
	condition := sdk.Finding{DetectorID: memory.KubernetesConditionDetectorIDs()[0], RuleID: "stuck", Status: sdk.FindingActive, Severity: sdk.SeverityCritical,
		Summary: "stuck", Evidence: "synthetic", PrimaryResource: sdk.ObjectRef{Kind: "Pod", Namespace: "demo", Name: "web"}}
	clue := sdk.Finding{DetectorID: "resource-saturation", RuleID: "cpu", Status: sdk.FindingActive, Severity: sdk.SeverityWarning,
		Summary: "cpu", Evidence: "synthetic", PrimaryResource: sdk.ObjectRef{Kind: "Pod", Namespace: "demo", Name: "web"}}
	c.admitActivation(ctx, condition, late, sdk.DetectorSpec{})
	c.admitActivation(ctx, clue, late, sdk.DetectorSpec{})
	if len(c.currentIncidentRequest.LatePages) != 0 || len(paged.events) != 0 {
		t.Fatalf("a condition or clue paged late: %+v", c.currentIncidentRequest.LatePages)
	}
	userTest := (&switchedProbe{id: memory.TrafficDetectorPrefix + "late", scenario: "write"}).finding()
	c.admitActivation(ctx, userTest, late, sdk.DetectorSpec{})
	c.admitActivation(ctx, userTest, late.Add(time.Second), sdk.DetectorSpec{})
	if got := c.currentIncidentRequest.LatePages; len(got) != 1 || got[0].Key != findingKey(userTest) || len(paged.events) != 1 {
		t.Fatalf("late pages = %+v, events %d; want one page for the late user test", got, len(paged.events))
	}
}

// Late pages survive the persisted projection, so a restart neither drops nor
// repeats one.
func TestLatePagesRoundTripThroughTheContract(t *testing.T) {
	at := time.Unix(2000, 0).UTC()
	request := IncidentRequest{IncidentID: "synthetic", OpeningKeys: []string{"a"}, LatePages: []LatePage{{Key: "b", PagedAt: at}}}
	message, err := incidentRequestContract(request)
	if err != nil {
		t.Fatal(err)
	}
	if got := incidentRequestState(message).LatePages; len(got) != 1 || got[0].Key != "b" || !got[0].PagedAt.Equal(at) {
		t.Fatalf("late pages = %+v", got)
	}
}
