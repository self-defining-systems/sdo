package prober

import (
	"context"
	"io"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"sdo.dev/operator/sdk/traffic"
)

// stepClock is a traffic.Clock the test moves: After fires only when advance
// passes its deadline, so the engine's pacing is observed exactly.
type stepClock struct {
	mu      sync.Mutex
	now     time.Time
	waiters []stepWaiter
}

type stepWaiter struct {
	at    time.Time
	fired chan time.Time
}

func (c *stepClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *stepClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	fired := make(chan time.Time, 1)
	c.waiters = append(c.waiters, stepWaiter{at: c.now.Add(d), fired: fired})
	return fired
}

func (c *stepClock) pending() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.waiters)
}

// advance moves time by d and reports how many waiters fired.
func (c *stepClock) advance(d time.Duration) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	kept, fired := c.waiters[:0], 0
	for _, waiter := range c.waiters {
		if waiter.at.After(c.now) {
			kept = append(kept, waiter)
			continue
		}
		waiter.fired <- c.now
		fired++
	}
	c.waiters = kept
	return fired
}

type okClient struct{}

func (okClient) Do(request *http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("ok")), Request: request}, nil
}

// slowProbe is a health probe whose designer chose the longest sample
// interval, to keep the steady probe load low.
func slowProbe(t *testing.T) (*Prober, *stepClock) {
	t.Helper()
	return slowProbeWith(t, traffic.SideEffectRead)
}

func slowProbeWith(t *testing.T, effect traffic.SideEffect) (*Prober, *stepClock) {
	t.Helper()
	var catalog traffic.Catalog
	var scenarios []traffic.WorkloadScenario
	for _, id := range []string{"catalog-read", "cart-read"} {
		marker := ""
		if effect != traffic.SideEffectRead {
			marker = "sdoprobe-" + id
		}
		catalog = append(catalog, traffic.Scenario{ID: id, Target: traffic.Target{Service: "web", Port: 80}, SideEffect: effect, Marker: marker,
			Steps: []traffic.Step{{Name: "get", Endpoint: traffic.GET("/"+id, nil)}}})
		scenarios = append(scenarios, traffic.WorkloadScenario{ID: id})
	}
	workload := traffic.Workload{APIVersion: traffic.APIVersion, Kind: traffic.WorkloadKind, Name: "probe-later",
		Purpose: traffic.PurposeHealthProbe, SampleInterval: traffic.Duration(60 * time.Second), Scenarios: scenarios}
	clock := &stepClock{now: time.Date(2026, 11, 16, 9, 30, 0, 0, time.UTC)}
	p, err := New(Config{Namespace: "app", Catalog: catalog, Workloads: []traffic.Workload{workload}, Client: okClient{},
		BaseURL: func(traffic.Target) string { return "http://web:80" }, Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	return p, clock
}

func sampleCount(p *Prober) int {
	count := 0
	for _, observed := range p.Windows()["probe-later"].Scenarios {
		count += len(observed.Samples)
	}
	return count
}

// settle yields to the engine's goroutines until every started iteration
// recorded its sample and the engine waits for its next slot (bounded by
// yields, not by the wall clock).
func settle(t *testing.T, p *Prober, clock *stepClock, started int) {
	t.Helper()
	for yields := 0; sampleCount(p) < started || clock.pending() == 0; yields++ {
		if yields > 5_000_000 {
			t.Fatalf("prober did not settle: %d of %d samples, %d waiters", sampleCount(p), started, clock.pending())
		}
		runtime.Gosched()
	}
}

// readyAfter runs the probe from now until its window holds every
// scenario's SLO minimum, and returns how long that took in probe time.
func readyAfter(t *testing.T, p *Prober, clock *stepClock, started *int, limit time.Duration) time.Duration {
	t.Helper()
	begin := clock.Now()
	for !p.Windows()["probe-later"].Ready() {
		if clock.Now().Sub(begin) > limit {
			t.Fatalf("window not ready %s after the start: %d samples", limit, sampleCount(p))
		}
		*started += clock.advance(100 * time.Millisecond)
		settle(t, p, clock, *started)
	}
	return clock.Now().Sub(begin)
}

// A started or reset prober has no samples, and every detector that reads
// it stays unconfirmed until each scenario holds its SLO's minimum. That
// wait must not be minimum-samples times the steady interval (which the
// designer lengthens to lower steady load, up to minutes): the prober samples
// at the floor interval until the window is ready, then at the steady
// interval again.
func TestProberWarmsUpAtTheFloorIntervalAfterStartAndReset(t *testing.T) {
	p, clock := slowProbe(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); p.Run(ctx) }()
	defer func() { cancel(); <-done }()
	started := 1
	settle(t, p, clock, started)

	// Minimum samples at the floor interval, plus one iteration's spread.
	floor := time.Duration(traffic.DefaultMinSamples)*traffic.MinSampleInterval + traffic.MinSampleInterval
	if took := readyAfter(t, p, clock, &started, 5*time.Minute); took > floor {
		t.Fatalf("start: window ready after %s, want at most %s (steady pacing needs %s)", took, floor, 3*time.Minute)
	}

	// Warm, the probe returns to its steady interval: one sample per
	// scenario per 60 s.
	before := sampleCount(p)
	for range 1200 {
		started += clock.advance(100 * time.Millisecond)
		settle(t, p, clock, started)
	}
	if sent := sampleCount(p) - before; sent > 2*2+1 {
		t.Fatalf("warm probe sent %d samples in 120 s, want the steady interval's 4", sent)
	}

	// A reset (a maintenance resume, a controller restart) forgets the
	// samples and warms up again.
	p.Reset()
	started = sampleCount(p)
	if took := readyAfter(t, p, clock, &started, 5*time.Minute); took > floor+traffic.MinSampleInterval {
		t.Fatalf("reset: window ready after %s, want at most %s", took, floor+traffic.MinSampleInterval)
	}
}

// A workload that writes keeps its designer's interval while warming up: its
// scenarios read back their own writes through shared synthetic users, and at
// the floor rate samples of different workloads interleave (recorded on EKS:
// social's compose-read read another sample's post and the install's
// onboarding was rejected).
func TestAWritingProbeWarmsUpAtItsOwnInterval(t *testing.T) {
	p, clock := slowProbeWith(t, traffic.SideEffectIdempotentWrite)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); p.Run(ctx) }()
	defer func() { cancel(); <-done }()
	started := 1
	settle(t, p, clock, started)
	floor := time.Duration(traffic.DefaultMinSamples)*traffic.MinSampleInterval + traffic.MinSampleInterval
	if took := readyAfter(t, p, clock, &started, 10*time.Minute); took <= floor {
		t.Fatalf("a writing probe warmed up at the floor interval: ready after %s", took)
	}
}
