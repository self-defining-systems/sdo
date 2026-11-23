package prober

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"sort"
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
	var catalog traffic.Catalog
	var scenarios []traffic.WorkloadScenario
	for _, id := range []string{"catalog-read", "cart-read"} {
		catalog = append(catalog, traffic.Scenario{ID: id, Target: traffic.Target{Service: "web", Port: 80}, SideEffect: traffic.SideEffectRead,
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
	for _, window := range p.Windows() {
		for _, observed := range window.Scenarios {
			count += len(observed.Samples)
		}
	}
	return count
}

// ready reports whether every workload's window holds each scenario's SLO
// minimum.
func ready(p *Prober) bool {
	for _, window := range p.Windows() {
		if !window.Ready() {
			return false
		}
	}
	return true
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
	for !ready(p) {
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

// sharedAuthorApp is a synthetic application with one write-read journey:
// POST /compose stores a user's newest post, GET /newest returns it. Like a
// fan-out write, a compose answers only once the next compose arrives or a
// short while has passed, so two samples in flight on the same user interleave
// their write and read-back.
type sharedAuthorApp struct {
	mu       sync.Mutex
	newest   map[string]string
	composes int
	// inFlight and overlapped count composes that were answering at once.
	inFlight, overlapped int
}

func (a *sharedAuthorApp) Do(request *http.Request) (*http.Response, error) {
	answer := func(body string) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
	}
	if request.Method == http.MethodGet {
		a.mu.Lock()
		defer a.mu.Unlock()
		return answer(`{"text":"` + a.newest[request.URL.Query().Get("user")] + `"}`)
	}
	if err := request.ParseForm(); err != nil {
		return nil, err
	}
	a.mu.Lock()
	a.newest[request.PostForm.Get("user")] = request.PostForm.Get("text")
	a.composes++
	mine := a.composes
	a.inFlight++
	a.overlapped = max(a.overlapped, a.inFlight)
	a.mu.Unlock()
	defer func() { a.mu.Lock(); a.inFlight--; a.mu.Unlock() }()
	for waited := time.Duration(0); waited < 20*time.Millisecond; waited += 200 * time.Microsecond {
		a.mu.Lock()
		next := a.composes > mine
		a.mu.Unlock()
		if next {
			break
		}
		time.Sleep(200 * time.Microsecond)
	}
	return answer("ok")
}

// sharedAuthorProbe runs the same compose-then-read-newest journey, as the
// same synthetic author, in two health-probe workloads with one schedule (a
// designer copying a write-read journey into its call probe), so their samples
// start together.
func sharedAuthorProbe(t *testing.T, interval time.Duration) (*Prober, *stepClock) {
	t.Helper()
	p, clock, _ := authorProbe(t, interval, "sdoprobe-author", "sdoprobe-author")
	return p, clock
}

// authorProbe runs the journey as writeAuthor in probe-write and as
// callAuthor in probe-call.
func authorProbe(t *testing.T, interval time.Duration, writeAuthor string, callAuthor string) (*Prober, *stepClock, *sharedAuthorApp) {
	t.Helper()
	text, _ := traffic.FromSeedPair("n", 1_000_000_000, func(i int) string { return fmt.Sprintf("sdoprobe-post-%d", i) }, func(i int) string { return fmt.Sprintf("sdoprobe-post-%d", i) })
	journey := func(id string, author string) traffic.Scenario {
		return traffic.Scenario{ID: id, Target: traffic.Target{Service: "web", Port: 80}, SideEffect: traffic.SideEffectIdempotentWrite, Marker: "sdoprobe",
			Steps: []traffic.Step{
				{Name: "compose", Endpoint: traffic.POST("/compose", nil).WithForm(traffic.Params{"user": traffic.Const(author), "text": text}).Contains("ok")},
				{Name: "read-newest", Endpoint: traffic.GET("/newest", traffic.Params{"user": traffic.Const(author)}).JSONEquals("text", "sdoprobe-post-{n}")},
			}}
	}
	catalog := traffic.Catalog{journey("compose-read", writeAuthor), journey("call-compose-read", callAuthor)}
	var workloads []traffic.Workload
	for name, id := range map[string]string{"probe-write": "compose-read", "probe-call": "call-compose-read"} {
		workloads = append(workloads, traffic.Workload{APIVersion: traffic.APIVersion, Kind: traffic.WorkloadKind, Name: name,
			Purpose: traffic.PurposeHealthProbe, SampleInterval: traffic.Duration(interval), Scenarios: []traffic.WorkloadScenario{{ID: id}}})
	}
	clock := &stepClock{now: time.Date(2026, 11, 16, 9, 30, 0, 0, time.UTC)}
	app := &sharedAuthorApp{newest: map[string]string{}}
	p, err := New(Config{Namespace: "app", Catalog: catalog, Workloads: workloads, Client: app,
		BaseURL: func(traffic.Target) string { return "http://web:80" }, Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	return p, clock, app
}

// failures lists every sample that did not pass.
func failures(p *Prober) []string {
	var failed []string
	for name, window := range p.Windows() {
		for _, observed := range window.Scenarios {
			for _, sample := range observed.Samples {
				if sample.Outcome != traffic.OutcomeOK {
					failed = append(failed, fmt.Sprintf("%s/%s iteration %d step %s: %s", name, observed.Scenario.ID, sample.Iteration, sample.Step, sample.Error))
				}
			}
		}
	}
	sort.Strings(failed)
	return failed
}

// Samples that write and read back one synthetic identity never overlap,
// whichever workload runs them and at whatever pace: a read-back sees its own
// sample's write (recorded on EKS: social's compose-read read the post its
// call-probe copy composed as the same author, the write probe fired and the
// install's onboarding was rejected).
func TestSamplesOfOneSyntheticIdentityNeverInterleave(t *testing.T) {
	p, clock := sharedAuthorProbe(t, traffic.MinSampleInterval)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); p.Run(ctx) }()
	defer func() { cancel(); <-done }()
	started := 2
	settle(t, p, clock, started)
	for range 600 {
		started += clock.advance(100 * time.Millisecond)
		settle(t, p, clock, started)
	}
	if count := sampleCount(p); count < 12 {
		t.Fatalf("%d samples in 60 s, want at least 12", count)
	}
	if failed := failures(p); len(failed) > 0 {
		t.Fatalf("a read-back saw another sample's write (%d of %d samples failed): %s", len(failed), sampleCount(p), strings.Join(failed, "; "))
	}
}

// A writing probe warms up at the floor interval like a reading one: its
// window is ready within minimum-samples floor intervals, and no sample fails.
func TestAWritingProbeWarmsUpAtTheFloorInterval(t *testing.T) {
	p, clock := sharedAuthorProbe(t, 60*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); p.Run(ctx) }()
	defer func() { cancel(); <-done }()
	started := 2
	settle(t, p, clock, started)
	floor := time.Duration(traffic.DefaultMinSamples)*traffic.MinSampleInterval + traffic.MinSampleInterval
	if took := readyAfter(t, p, clock, &started, 10*time.Minute); took > floor {
		t.Fatalf("a writing probe warmed up at its steady interval: ready after %s, want at most %s", took, floor)
	}
	if failed := failures(p); len(failed) > 0 {
		t.Fatalf("a read-back saw another sample's write while warming up: %s", strings.Join(failed, "; "))
	}
}

// Samples of different synthetic identities still run concurrently: only a
// shared identity waits.
func TestSamplesOfDistinctSyntheticIdentitiesStillOverlap(t *testing.T) {
	p, clock, app := authorProbe(t, traffic.MinSampleInterval, "sdoprobe-author-w", "sdoprobe-author-c")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); p.Run(ctx) }()
	defer func() { cancel(); <-done }()
	started := 2
	settle(t, p, clock, started)
	for range 300 {
		started += clock.advance(100 * time.Millisecond)
		settle(t, p, clock, started)
	}
	if failed := failures(p); len(failed) > 0 {
		t.Fatalf("distinct identities failed: %s", strings.Join(failed, "; "))
	}
	app.mu.Lock()
	defer app.mu.Unlock()
	if app.overlapped < 2 {
		t.Fatalf("samples of distinct identities never overlapped (%d composes): they were serialized", app.composes)
	}
}
