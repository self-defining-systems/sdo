package runtime

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	kclock "k8s.io/utils/clock"
	clocktesting "k8s.io/utils/clock/testing"

	"sdo.dev/operator/apihistory"
	"sdo.dev/operator/runtime/prober"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/traffic"
)

// detectWorld is a synthetic application whose "create" user test fails
// until healed; reads count up so a wait sees time pass.
type detectWorld struct {
	mu     sync.Mutex
	healed bool
	// healAfter heals the world on this many snapshot reads (0: never).
	healAfter int
	reads     int
	bursts    []prober.BurstRequest
	sendOK    bool
}

func (w *detectWorld) view() detectFake {
	if w.healed {
		return detectApp()
	}
	return detectApp("create")
}

func (w *detectWorld) Snapshot(context.Context) (sdk.DetectionContext, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.reads++
	if w.healAfter > 0 && w.reads >= w.healAfter {
		w.healed = true
	}
	return w.view(), nil
}

func (w *detectWorld) Windows(context.Context) (map[string]traffic.Window, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	windows := w.view().Traffic
	// The prober's newest request of the create test fails until the world heals.
	for name, window := range windows {
		for i := range window.Scenarios {
			newest := traffic.Sample{At: detectNow().Add(time.Second), Outcome: traffic.OutcomeOK}
			if !w.healed && window.Scenarios[i].Scenario.ID == "create" {
				newest.Outcome, newest.Status = traffic.OutcomeError, 500
			}
			window.Scenarios[i].Samples = append(window.Scenarios[i].Samples, newest)
		}
		windows[name] = window
	}
	return windows, nil
}

func (w *detectWorld) Burst(_ context.Context, request prober.BurstRequest) (prober.BurstResult, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(request.Scenarios) == 0 {
		// The controller's internal verify burst.
		return prober.BurstResult{Workload: request.Workload, Healthy: true}, nil
	}
	w.bursts = append(w.bursts, request)
	sample := traffic.Sample{At: detectNow(), Outcome: traffic.OutcomeError, Status: 500, Step: "write-row", Request: "POST /items",
		Error: "token=abcd1234efgh5678ijkl9012mnop3456 refused", Latency: 120 * time.Millisecond}
	if w.sendOK {
		sample = traffic.Sample{At: detectNow(), Outcome: traffic.OutcomeOK, Latency: 40 * time.Millisecond}
	}
	return prober.BurstResult{Workload: request.Workload, Window: traffic.Window{Scenarios: []traffic.ScenarioObservations{
		{Scenario: traffic.Descriptor{ID: request.Scenarios[0]}, Samples: []traffic.Sample{sample}}}}}, nil
}

func (*detectWorld) Reset(context.Context) error { return nil }

// trafficDetector fires one finding per failing user test of the view.
type trafficDetector struct{}

func (trafficDetector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: "traffic.health", Interval: time.Second}
}

func (trafficDetector) Detect(_ context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	var findings []sdk.Finding
	for _, window := range view.(traffic.WindowSet).TrafficWindows() {
		for _, verdict := range traffic.Judge(window) {
			if verdict.Evaluated && !verdict.Healthy {
				findings = append(findings, sdk.Finding{RuleID: traffic.RuleIDPrefix + verdict.ScenarioID, Status: sdk.FindingActive, Summary: verdict.ScenarioID + " failing"})
			}
		}
	}
	return findings, nil
}

// autoClock is a fake clock whose timers fire at once by advancing it, so a
// wait runs without goroutines or real time.
type autoClock struct{ *clocktesting.FakeClock }

func (c autoClock) NewTimer(d time.Duration) kclock.Timer {
	timer := c.FakeClock.NewTimer(d)
	c.FakeClock.Step(d)
	return timer
}

func detectWorldLoop(world *detectWorld) (*ResponderLoop, autoClock) {
	clock := autoClock{clocktesting.NewFakeClock(detectNow())}
	loop := NewResponderLoop(detectRequest(), nil, world, []sdk.Detector{trafficDetector{}}, PlanExecutor{}, 10)
	loop.ReadView = func(v sdk.DetectionContext) program.ReadView { return program.ReadFrom(v) }
	loop.Traffic = world
	loop.Now = clock.Now
	loop.WaitClock = clock
	return loop, clock
}

func TestDetectATestSendsItsScheduledRequestNowAndShowsTheResult(t *testing.T) {
	world := &detectWorld{}
	loop, _ := detectWorldLoop(world)
	response := loop.Handle(context.Background(), ToolRequest{Command: "detect", Name: "create"})
	if response.Error != "" {
		t.Fatal(response.Error)
	}
	if len(world.bursts) != 1 || world.bursts[0].Workload != "health" || len(world.bursts[0].Scenarios) != 1 || world.bursts[0].Scenarios[0] != "create" {
		t.Fatalf("bursts = %+v, want the health probe's create test once", world.bursts)
	}
	for _, want := range []string{"sent now: create at 07:28:43Z: FAIL HTTP 500, latency 120ms", "  request: POST /items", "  status: 500", "last failing samples"} {
		if !strings.Contains(response.Text, want) {
			t.Fatalf("detect create lacks %q:\n%s", want, response.Text)
		}
	}
	if strings.Contains(response.Text, "abcd1234efgh5678") {
		t.Fatalf("a credential leaked:\n%s", response.Text)
	}
	world.sendOK = true
	response = loop.Handle(context.Background(), ToolRequest{Command: "detect", Name: "list"})
	if !strings.Contains(response.Text, "sent now: list at 07:28:43Z: PASS") || !strings.Contains(response.Text, "list: no failing samples in the window") {
		t.Fatalf("passing test:\n%s", response.Text)
	}
}

func TestDetectADetectorRunsItAlone(t *testing.T) {
	loop, _ := detectWorldLoop(&detectWorld{})
	response := loop.Handle(context.Background(), ToolRequest{Command: "detect", Name: "traffic.health"})
	if response.Error != "" {
		t.Fatal(response.Error)
	}
	for _, want := range []string{"detector traffic.health: firing, 1 finding (evaluated 07:28:43Z)", "user test create  create failing"} {
		if !strings.Contains(response.Text, want) {
			t.Fatalf("detector output lacks %q:\n%s", want, response.Text)
		}
	}
}

func TestDetectAnItemOrAnUnknownName(t *testing.T) {
	loop, _ := detectWorldLoop(&detectWorld{})
	if response := loop.Handle(context.Background(), ToolRequest{Command: "detect", Name: "tests"}); response.Error != "" || !strings.Contains(response.Text, "create") {
		t.Fatalf("item tests = %q, %q", response.Text, response.Error)
	}
	response := loop.Handle(context.Background(), ToolRequest{Command: "detect", Name: "nothing"})
	for _, want := range []string{`no user test, detector or summary item named "nothing"`, "user tests: create, list", "detectors: traffic.health"} {
		if !strings.Contains(response.Error, want) {
			t.Fatalf("error lacks %q: %q", want, response.Error)
		}
	}
}

func TestDetectSummarySaysItIsFresh(t *testing.T) {
	loop, _ := detectWorldLoop(&detectWorld{})
	response := loop.Handle(context.Background(), ToolRequest{Command: "detect"})
	if !strings.Contains(response.Text, "fresh: detectors ran on reads taken 07:28:43Z; newest user-test request 07:28:42Z (1s ago)") {
		t.Fatalf("no freshness line:\n%s", response.Text)
	}
}

func TestDetectWaitReturnsWhenAFailingTestPasses(t *testing.T) {
	world := &detectWorld{healAfter: 3}
	loop, clock := detectWorldLoop(world)
	started := clock.Now()
	response := loop.Handle(context.Background(), ToolRequest{Command: "detect", WaitSeconds: 60})
	if response.Error != "" {
		t.Fatal(response.Error)
	}
	first := strings.SplitN(response.Text, "\n", 2)[0]
	if first != "wait: after 4s, create newest request passed (was failing); cleared user test create" {
		t.Fatalf("first line = %q", first)
	}
	if got := clock.Since(started); got != 4*time.Second {
		t.Fatalf("waited %s, want the two polls (4s) until the change", got)
	}
	if calls := len(loop.calls); calls != 1 {
		t.Fatalf("a wait recorded %d tool calls, want one", calls)
	}
}

func TestDetectWaitReturnsAtTheTimeoutWithoutAChange(t *testing.T) {
	loop, clock := detectWorldLoop(&detectWorld{})
	started := clock.Now()
	response := loop.Handle(context.Background(), ToolRequest{Command: "detect", WaitSeconds: 5})
	if first := strings.SplitN(response.Text, "\n", 2)[0]; first != "wait: no change in 5s" {
		t.Fatalf("first line = %q", first)
	}
	if got := clock.Since(started); got != 5*time.Second {
		t.Fatalf("waited %s, want 5s", got)
	}
	if response := loop.Handle(context.Background(), ToolRequest{Command: "detect", WaitSeconds: 601}); response.Error == "" {
		t.Fatal("a wait above the bound must be refused")
	}
}

func TestDetectWaitOnATestSendsItOnceThenWatchesTheSchedule(t *testing.T) {
	world := &detectWorld{healAfter: 6}
	loop, _ := detectWorldLoop(world)
	response := loop.Handle(context.Background(), ToolRequest{Command: "detect", Name: "create", WaitSeconds: 30})
	if len(world.bursts) != 1 {
		t.Fatalf("sent %d times, want once", len(world.bursts))
	}
	if first := strings.SplitN(response.Text, "\n", 2)[0]; !strings.HasPrefix(first, "wait: after ") || !strings.Contains(first, "create newest request passed") {
		t.Fatalf("first line = %q", first)
	}
}

func TestDetectWaitReturnsOnceRecovered(t *testing.T) {
	loop, clock := detectWorldLoop(&detectWorld{})
	loop.IncidentStatus = func() *apihistory.IncidentVerified { return &apihistory.IncidentVerified{} }
	started := clock.Now()
	response := loop.Handle(context.Background(), ToolRequest{Command: "detect", WaitSeconds: 60})
	if first := strings.SplitN(response.Text, "\n", 2)[0]; first != "wait: after 0s, recovered: SDO is ending your incident work" {
		t.Fatalf("first line = %q", first)
	}
	if clock.Since(started) != 0 {
		t.Fatal("a recovered incident must not wait")
	}
}

// A harness-paged incident judges the page's operations through SDO's own
// test requests: detect, detect TEST and --wait work as for any incident.
func TestDetectWorksTheSameForAPagedIncident(t *testing.T) {
	world := &detectWorld{healAfter: 4}
	loop, _ := detectWorldLoop(world)
	loop.Request.ExternalMessage = "Page: users are seeing failures: item create"
	loop.ExternalClues = true
	summary := loop.Handle(context.Background(), ToolRequest{Command: "detect"})
	if !strings.Contains(summary.Text, "opened on: page") || !strings.Contains(summary.Text, "create  HTTP 500") {
		t.Fatalf("paged summary:\n%s", summary.Text)
	}
	if sent := loop.Handle(context.Background(), ToolRequest{Command: "detect", Name: "create"}); !strings.Contains(sent.Text, "sent now: create") {
		t.Fatalf("paged detect create:\n%s", sent.Text)
	}
	waited := loop.Handle(context.Background(), ToolRequest{Command: "detect", Name: "create", WaitSeconds: 30})
	if first := strings.SplitN(waited.Text, "\n", 2)[0]; !strings.Contains(first, "create newest request passed") {
		t.Fatalf("paged wait first line = %q", first)
	}
}
