package traffic

import (
	"context"
	"math/rand/v2"
	"net/http"
	"strings"
	"testing"
	"time"
)

// readModelCheck is an agent-written check: a 404 means the event handler has
// not delivered yet; 200 must carry the exact name.
type readModelCheck struct{ notYet bool }

func (readModelCheck) Build(_ context.Context, _ *rand.Rand, _ State) (Request, error) {
	return Request{Method: "GET", Path: "/readmodel/customers/sdo-probe-1"}, nil
}

func (c readModelCheck) Check(r Response, state State) Verdict {
	switch {
	case r.Status == http.StatusNotFound && c.notYet:
		return NotYet("read model has no customer yet")
	case r.Status != http.StatusOK:
		return Fail("read model answered HTTP %d", r.Status)
	case !strings.Contains(string(r.Body), "Probe One"):
		return Fail("read model has the wrong customer: %s", r.Body)
	}
	state["seen"] = "yes"
	return Pass()
}

func registerScenario(check Endpoint) Scenario {
	return Scenario{
		ID: "register-customer", Target: Target{Service: "web", Port: 80}, SideEffect: SideEffectIdempotentWrite, Marker: "sdo-probe-1",
		Steps: []Step{
			{Name: "register", Endpoint: POST("/customers/sdo-probe-1", nil).Status(http.StatusCreated)},
			{Name: "read-model", Endpoint: check},
		},
	}
}

func readModelServer(deliverAfter time.Duration) *simServer {
	clock := newSimClock()
	var wrote time.Time
	return &simServer{clock: clock, handle: func(now time.Time, r *http.Request) (int, string) {
		if r.Method == http.MethodPost {
			wrote = now
			return http.StatusCreated, `{}`
		}
		if deliverAfter < 0 || now.Sub(wrote) < deliverAfter {
			return http.StatusNotFound, "no such customer"
		}
		return http.StatusOK, `{"name":"Probe One"}`
	}}
}

// The event is never delivered to the read model. Treating a 404 as a pass
// cannot see that; a NotYet check keeps asking and then fails.
func TestNotYetFailsWhenTheEventIsNeverDelivered(t *testing.T) {
	server := readModelServer(-1)
	engine := simEngine(t, probeWorkload("register-customer"), Catalog{registerScenario(readModelCheck{notYet: true})}, server)
	_, sample := engine.Iterate(context.Background(), 0)
	if sample.Outcome != OutcomeError || sample.Step != "read-model" {
		t.Fatalf("an undelivered event passed: %+v", sample)
	}
	if !strings.Contains(sample.Error, "still not there") || !strings.Contains(sample.Error, "no customer yet") {
		t.Fatalf("error does not say it kept waiting: %q", sample.Error)
	}
	got := sample.Convergence
	if len(got) != 1 || got[0].Converged || got[0].Attempts < 10 {
		t.Fatalf("convergence not recorded as a failed wait: %+v", got)
	}
	if took := got[0].Took; took < 9*time.Second || took > 11*time.Second {
		t.Fatalf("waited %s, want the 10 s iteration timeout", took)
	}
	if posts := strings.Count(strings.Join(server.requests(), "\n"), "POST"); posts != 1 {
		t.Fatalf("the write was repeated %d times", posts)
	}
}

func TestNotYetPassesOnArrivalAndRecordsHowLongItTook(t *testing.T) {
	server := readModelServer(2 * time.Second)
	engine := simEngine(t, probeWorkload("register-customer"), Catalog{registerScenario(readModelCheck{notYet: true})}, server)
	_, sample := engine.Iterate(context.Background(), 0)
	if sample.Outcome != OutcomeOK {
		t.Fatalf("a delivered event failed: %+v", sample)
	}
	got := sample.Convergence
	if len(got) != 1 || !got[0].Converged || got[0].Attempts < 5 || got[0].Took < 2*time.Second || got[0].Took > 3*time.Second {
		t.Fatalf("convergence = %+v, want a pass after about 2s", got)
	}
}

// A check that never answers NotYet is judged once, as before.
func TestFailIsNotRetried(t *testing.T) {
	server := readModelServer(-1)
	engine := simEngine(t, probeWorkload("register-customer"), Catalog{registerScenario(readModelCheck{})}, server)
	_, sample := engine.Iterate(context.Background(), 0)
	if sample.Outcome != OutcomeError || len(sample.Convergence) != 0 || sample.Requests != 2 {
		t.Fatalf("a failing check was retried: %+v", sample)
	}
}

// Only the passing attempt's State changes are kept.
func TestNotYetKeepsOnlyThePassingAttemptsState(t *testing.T) {
	server := readModelServer(time.Second)
	scenario := registerScenario(readModelCheck{notYet: true})
	scenario.Steps = append(scenario.Steps, Step{Name: "after", Endpoint: stateGate{}})
	engine := simEngine(t, probeWorkload("register-customer"), Catalog{scenario}, server)
	if _, sample := engine.Iterate(context.Background(), 0); sample.Outcome != OutcomeOK {
		t.Fatalf("State from the passing attempt was lost: %+v", sample)
	}
}

type stateGate struct{}

func (stateGate) Build(_ context.Context, _ *rand.Rand, state State) (Request, error) {
	if state["seen"] != "yes" {
		return Request{}, context.Canceled
	}
	return Request{Method: "GET", Path: "/readmodel/customers/sdo-probe-1"}, nil
}
func (stateGate) Check(Response, State) Verdict { return Pass() }
