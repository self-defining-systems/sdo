package runtime

import (
	"context"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/runtime/prober"
	"sdo.dev/operator/sdk/traffic"
)

// fakeProber serves recorded windows and counts any call that would send traffic.
type fakeProber struct {
	windows map[string]traffic.Window
	calls   int
}

func (p *fakeProber) Windows(context.Context) (map[string]traffic.Window, error) {
	return p.windows, nil
}

func (p *fakeProber) Burst(context.Context, prober.BurstRequest) (prober.BurstResult, error) {
	p.calls++
	return prober.BurstResult{}, nil
}

func (p *fakeProber) Reset(context.Context) error { p.calls++; return nil }

func tryProber(now time.Time) *fakeProber {
	okSample := traffic.Sample{At: now.Add(-time.Minute), Outcome: traffic.OutcomeOK, Status: 200, Iteration: 1}
	failSample := traffic.Sample{At: now.Add(-time.Second), Outcome: traffic.OutcomeError, Status: 503, Iteration: 2, Step: "open",
		Request: "GET /items?token=abcd1234efgh5678", Error: "unexpected status 503", Observed: "Authorization: Bearer abcdefghijklmnop upstream down", Latency: 40 * time.Millisecond}
	return &fakeProber{windows: map[string]traffic.Window{"probe": {ObservedAt: now, Scenarios: []traffic.ScenarioObservations{
		{Scenario: traffic.Descriptor{ID: "browse", Description: "list items", Target: traffic.Target{Service: "web", Port: 80}, SideEffect: traffic.SideEffectRead, Steps: []string{"open"}}, Samples: []traffic.Sample{okSample, failSample}, Qualified: true},
		{Scenario: traffic.Descriptor{ID: "health", Target: traffic.Target{Service: "web", Port: 80}, SideEffect: traffic.SideEffectRead, Steps: []string{"ping"}}, Samples: []traffic.Sample{{At: now, Outcome: traffic.OutcomeOK, Status: 200}}, Qualified: true},
	}}}}
}

func tryLoop(p *fakeProber) *ResponderLoop {
	loop := NewResponderLoop(IncidentRequest{IncidentID: "incident"}, nil, nil, nil, PlanExecutor{}, 3)
	loop.Traffic = p
	loop.Now = func() time.Time { return time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC) }
	return loop
}

func TestTryListsUserTestsWithStatus(t *testing.T) {
	p := tryProber(time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC))
	response := tryLoop(p).Handle(context.Background(), ToolRequest{Command: "try"})
	if response.Error != "" {
		t.Fatal(response.Error)
	}
	for _, want := range []string{"browse", "FAIL", "health", "PASS"} {
		if !strings.Contains(response.Text, want) {
			t.Fatalf("listing lacks %q:\n%s", want, response.Text)
		}
	}
}

func TestTryShowsTheLastSampleRedactedAndIsReadOnly(t *testing.T) {
	p := tryProber(time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC))
	loop := tryLoop(p)
	response := loop.Handle(context.Background(), ToolRequest{Command: "try", Test: "browse"})
	if response.Error != "" {
		t.Fatal(response.Error)
	}
	for _, want := range []string{"passes now: no", "GET /items", "status: 503", "unexpected status 503", "step: open"} {
		if !strings.Contains(response.Text, want) {
			t.Fatalf("detail lacks %q:\n%s", want, response.Text)
		}
	}
	for _, secret := range []string{"abcd1234efgh5678", "abcdefghijklmnop"} {
		if strings.Contains(response.Text, secret) {
			t.Fatalf("secret %q leaked:\n%s", secret, response.Text)
		}
	}
	if p.calls != 0 || loop.batches != 0 || len(loop.applicationsLog) != 0 {
		t.Fatalf("try must not burst, reset, or write: calls=%d", p.calls)
	}
	if !concurrentRead("try") {
		t.Fatal("try is a concurrent read")
	}
}

func TestTryNamesTheValidTestsForAnUnknownOne(t *testing.T) {
	loop := tryLoop(tryProber(time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC)))
	response := loop.Handle(context.Background(), ToolRequest{Command: "try", Test: "nope"})
	if !strings.Contains(response.Error, `unknown user test "nope"`) || !strings.Contains(response.Error, "browse") {
		t.Fatalf("error = %q", response.Error)
	}
}
