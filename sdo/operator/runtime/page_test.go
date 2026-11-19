package runtime

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
)

const syntheticPage = "Users report failures. Find and fix it."

// pageDispatcher is a responder that works until the test returns it.
type pageDispatcher struct {
	mu       sync.Mutex
	calls    int
	requests chan IncidentRequest
	returned chan struct{}
}

func (d *pageDispatcher) Dispatch(ctx context.Context, request IncidentRequest) (IncidentResult, error) {
	d.mu.Lock()
	d.calls++
	d.mu.Unlock()
	d.requests <- request
	select {
	case <-d.returned:
		return completedResult(request.IncidentID), nil
	case <-ctx.Done():
		return IncidentResult{}, ctx.Err()
	}
}

func (d *pageDispatcher) callCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.calls
}

// pageController is a detector-mode controller whose inbox holds one page.
func pageController(t *testing.T, now time.Time, probes ...*switchedProbe) (*Controller, *pageDispatcher, *int) {
	t.Helper()
	detectors := make([]sdk.Detector, 0, len(probes))
	for _, probe := range probes {
		detectors = append(detectors, probe)
	}
	cfg := testControllerConfig()
	cfg.ClosureStabilityWindow = time.Second
	dispatcher := &pageDispatcher{requests: make(chan IncidentRequest, 4), returned: make(chan struct{})}
	t.Cleanup(func() { close(dispatcher.returned) })
	c, err := NewController(cfg, detectors, staticProvider{}, dispatcher, now)
	if err != nil {
		t.Fatal(err)
	}
	c.now = func() time.Time { return now }
	reads := new(int)
	c.ExternalIncidents = func(context.Context) (*ExternalIncident, error) {
		*reads++
		return &ExternalIncident{ID: "page-1", Namespace: cfg.Namespace, Message: syntheticPage}, nil
	}
	return c, dispatcher, reads
}

// In detector mode a page opens an incident with no opening alert while the
// detectors keep running, its responder is dispatched once with the page
// text, and the page, left in the inbox, is accepted only once.
func TestAPageOpensAnIncidentInDetectorModeAndDispatchesOnce(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1000, 0).UTC()
	probe := &switchedProbe{id: memory.TrafficDetectorPrefix + "web", scenario: "browse"}
	c, dispatcher, reads := pageController(t, now, probe)
	if err := c.Step(ctx, now, nil); err != nil {
		t.Fatal(err)
	}
	if *reads == 0 {
		t.Fatal("a detector-mode controller never read the page inbox")
	}
	request := c.currentIncidentRequest
	if !c.incidentOpen || request == nil || request.Page != syntheticPage || request.ExternalMessage != "" {
		t.Fatalf("the page did not open a detector-mode incident: %+v", request)
	}
	if len(openingKeys(request)) != 0 || len(alertKeys(request)) != 0 {
		t.Fatalf("a page-opened incident has no detector alert: opening %v", openingKeys(request))
	}
	if len(c.history) == 0 {
		t.Fatal("detectors stopped running in detector mode")
	}
	executePendingEffect(t, c)
	dispatched := awaitRequest(t, dispatcher.requests)
	if dispatched.Page != syntheticPage {
		t.Fatalf("responder did not receive the page text: %q", dispatched.Page)
	}
	if err := validateIncidentRequest(dispatched); err != nil {
		t.Fatalf("page-opened request refused by the contract: %v", err)
	}
	opened := request.IncidentID
	for range 3 {
		now = now.Add(time.Second)
		if err := c.Step(ctx, now, nil); err != nil {
			t.Fatal(err)
		}
	}
	if c.currentIncidentRequest == nil || c.currentIncidentRequest.IncidentID != opened || dispatcher.callCount() != 1 {
		t.Fatalf("the same page opened or dispatched again: incident %v, dispatches %d", c.currentIncidentRequest, dispatcher.callCount())
	}
	state, err := c.ExportState()
	if err != nil {
		t.Fatal(err)
	}
	if state.GetLastPageId() != "page-1" || state.GetIncidentRequest().GetPage() != syntheticPage {
		t.Fatalf("page acceptance not persisted: %q / %q", state.GetLastPageId(), state.GetIncidentRequest().GetPage())
	}
}

// A page that arrives while a detected incident is open joins it: no second
// incident, no re-prompt, and the incident now waits for the responder's return.
func TestAPageJoinsTheOpenIncidentInsteadOfOpeningASecond(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1000, 0).UTC()
	opening := &switchedProbe{id: memory.TrafficDetectorPrefix + "opening", scenario: "ssh"}
	opening.failing.Store(true)
	c, _ := alertSetController(t, now, opening)
	c.responderDone = false
	c.dispatchState = contracts.DispatchState_DISPATCH_STATE_RUNNING
	c.ExternalIncidents = func(context.Context) (*ExternalIncident, error) {
		return &ExternalIncident{ID: "page-2", Namespace: c.config.Namespace, Message: syntheticPage}, nil
	}
	if err := c.Step(ctx, now, nil); err != nil {
		t.Fatal(err)
	}
	request := c.currentIncidentRequest
	if request == nil || request.IncidentID != "synthetic" || request.Page != syntheticPage || len(request.OpeningKeys) != 1 {
		t.Fatalf("the page did not join the open incident: %+v", request)
	}
	if c.lastPageID != "page-2" {
		t.Fatalf("joined page not accepted: %q", c.lastPageID)
	}
}

// A page-opened incident is Recovered only after the responder returned and
// every alert stays clear for the stability window; a user test that joined
// holds it. Its closure is a verified (learnable) one, not an escalation.
func TestAPageOpenedIncidentClosesOnlyAfterTheResponderReturns(t *testing.T) {
	for _, joined := range []bool{false, true} {
		t.Run(map[bool]string{false: "clear", true: "joined user test holds"}[joined], func(t *testing.T) {
			ctx := context.Background()
			now := time.Unix(1000, 0).UTC()
			probe := &switchedProbe{id: memory.TrafficDetectorPrefix + "web", scenario: "browse"}
			c, dispatcher, _ := pageController(t, now, probe)
			closed := new(IncidentClosure)
			c.OnIncidentClosed = func(closure IncidentClosure) { *closed = closure }
			if err := c.Step(ctx, now, nil); err != nil {
				t.Fatal(err)
			}
			executePendingEffect(t, c)
			awaitRequest(t, dispatcher.requests)
			step := func(count int) {
				for range count {
					now = now.Add(time.Second)
					c.now = func() time.Time { return now }
					if err := c.Step(ctx, now, nil); err != nil {
						t.Fatal(err)
					}
				}
			}
			step(5)
			if !c.incidentOpen || closed.Result != nil || c.pendingClosure != nil {
				t.Fatal("a page-opened incident closed while its responder was still working")
			}
			if joined {
				probe.failing.Store(true)
				step(2)
				if got := c.currentIncidentRequest.JoinedKeys; len(got) != 1 {
					t.Fatalf("a user test failing after the page did not join its alert set: %v", got)
				}
			}
			dispatcher.returned <- struct{}{}
			c.handleDispatchCompletion(<-c.results, now)
			if !c.responderDoneNow() {
				t.Fatal("the responder's return never reached the controller")
			}
			step(5)
			if joined {
				if !c.incidentOpen || closed.Result != nil {
					t.Fatal("closed while a joined user test still failed")
				}
				return
			}
			if c.incidentOpen || closed.Result == nil {
				t.Fatal("the page-opened incident did not close after the responder returned and alerts stayed clear")
			}
			if !c.escalatedAt.IsZero() || closed.UnconfirmedReason != contracts.ClosureUnconfirmedReason_CLOSURE_UNCONFIRMED_REASON_UNSPECIFIED || len(closed.FinalDetectorStates) == 0 {
				t.Fatalf("closure is not a verified one: escalated %v reason %v states %d", c.escalatedAt, closed.UnconfirmedReason, len(closed.FinalDetectorStates))
			}
			state, err := c.ExportState()
			if err != nil {
				t.Fatal(err)
			}
			restored, err := NewController(c.config, []sdk.Detector{probe}, staticProvider{}, dispatcher, now)
			if err != nil {
				t.Fatal(err)
			}
			if err := restored.RestoreState(state); err != nil {
				t.Fatal(err)
			}
			restored.ExternalIncidents = c.ExternalIncidents
			if err := restored.Step(ctx, now.Add(time.Second), nil); err != nil {
				t.Fatal(err)
			}
			if restored.incidentOpen || dispatcher.callCount() != 1 {
				t.Fatal("a page still in the inbox reopened after its incident closed and the controller restarted")
			}
		})
	}
}

// The contract admits a detector-mode request opened by a page without
// detector evidence, and never a page on an external-mode request.
func TestThePageContract(t *testing.T) {
	request := externalGoldenRequest(t)
	request.Findings, request.DetectorHistory = nil, nil
	request.Page = syntheticPage
	if err := validateIncidentRequest(request); err != nil {
		t.Fatal(err)
	}
	request.ExternalMessage = "external"
	if err := validateIncidentRequest(request); err == nil {
		t.Fatal("an external-mode request accepted a page")
	}
	message, err := incidentRequestContract(IncidentRequest{IncidentID: "synthetic", Page: syntheticPage})
	if err != nil {
		t.Fatal(err)
	}
	if incidentRequestState(message).Page != syntheticPage {
		t.Fatal("page lost in the contract projection")
	}
	request = IncidentRequest{IncidentID: "p", Page: syntheticPage}
	if !strings.Contains(buildDetectSummary(detectInput{Request: request, View: detectApp("create"), Now: detectNow()}).Text, "opened on: page") {
		t.Fatal("sdo detect does not say a page opened the incident")
	}
}

func (c *Controller) responderDoneNow() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.responderDone
}
