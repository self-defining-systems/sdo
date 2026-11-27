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
	// It keeps sending while a whole request still fits before the 10 s
	// iteration deadline.
	if took := got[0].Took; took < DefaultIterationTimeout-DefaultTimeout-time.Second || took > DefaultIterationTimeout {
		t.Fatalf("waited %s, want until a request no longer fits before the 10 s iteration timeout", took)
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

// The wait for a NotYet step is Convergence, not latency: a read model that
// converges after seconds, each request fast, must not violate the latency SLO.
func TestNotYetWaitIsNotLatency(t *testing.T) {
	server := readModelServer(3 * time.Second)
	engine := simEngine(t, probeWorkload("register-customer"), Catalog{registerScenario(readModelCheck{notYet: true})}, server)
	_, sample := engine.Iterate(context.Background(), 0)
	if sample.Outcome != OutcomeOK || len(sample.Convergence) != 1 || sample.Convergence[0].Took < 3*time.Second {
		t.Fatalf("setup of the test: %+v", sample)
	}
	if sample.Latency > 200*time.Millisecond {
		t.Fatalf("latency %s counts the wait", sample.Latency)
	}
	slo := defaultSLO()
	verdict := Evaluate("register-customer", slo, []Sample{sample, sample, sample}, sample.At)
	if !verdict.Healthy {
		t.Fatalf("converging step violates the objective: %+v", verdict)
	}
}

func TestSetupNotYetRetriesWithinIterationAndKeepsProgress(t *testing.T) {
	for _, test := range []struct {
		name         string
		deliverAfter time.Duration
		outcome      Outcome
	}{
		{"eventually ready", 2 * time.Second, OutcomeOK},
		{"never ready", -1, OutcomeError},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := readModelServer(test.deliverAfter)
			scenario := registerScenario(readModelCheck{notYet: true})
			scenario.Setup = scenario.Steps
			scenario.Steps = []Step{{Name: "after", Endpoint: stateGate{}}}
			engine := simEngine(t, probeWorkload(scenario.ID), Catalog{scenario}, server)
			_, sample := engine.Iterate(context.Background(), 0)
			if sample.Outcome != test.outcome || len(sample.Convergence) != 1 || sample.Convergence[0].Attempts < 2 {
				t.Fatalf("setup did not retry: %+v", sample)
			}
			if test.outcome == OutcomeOK {
				if _, sample := engine.Iterate(context.Background(), 1); sample.Outcome != OutcomeOK {
					t.Fatalf("setup progress lost: %+v", sample)
				}
			} else if took := sample.Convergence[0].Took; took < DefaultIterationTimeout-DefaultTimeout-time.Second || took > DefaultIterationTimeout {
				t.Fatalf("setup waited %s, want the iteration's 10s budget less one request", took)
			}
			if posts := strings.Count(strings.Join(server.requests(), "\n"), "POST"); posts != 1 {
				t.Fatalf("setup repeated its write: %d POSTs", posts)
			}
		})
	}
}
