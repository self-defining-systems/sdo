package traffic

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// limitedAPI answers 429 with Retry-After while limited, 200 otherwise, and counts what it was sent.
type limitedAPI struct {
	mu         sync.Mutex
	limited    bool
	retryAfter string
	sent       int
}

func (a *limitedAPI) Do(request *http.Request) (*http.Response, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.sent++
	header := http.Header{}
	status := http.StatusOK
	body := `[{"name":"repo"}]`
	if a.limited {
		status, body = http.StatusTooManyRequests, `Retry later`
		if a.retryAfter != "" {
			header.Set("Retry-After", a.retryAfter)
		}
	}
	return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

func (a *limitedAPI) count() int { a.mu.Lock(); defer a.mu.Unlock(); return a.sent }

type manualClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *manualClock) Now() time.Time                       { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *manualClock) After(time.Duration) <-chan time.Time { return make(chan time.Time) }
func (c *manualClock) advance(d time.Duration)              { c.mu.Lock(); c.now = c.now.Add(d); c.mu.Unlock() }
func limitedEngine(t *testing.T, api Doer, clock Clock) *Engine {
	t.Helper()
	workload := Workload{
		APIVersion: APIVersion, Kind: WorkloadKind, Name: "probe-read-later", Purpose: PurposeHealthProbe,
		Scenarios: []WorkloadScenario{{ID: listScenario().ID}},
	}
	engine, err := NewEngine(workload, Catalog{listScenario()}, api, func(Target) string { return "http://web:80" }, clock)
	if err != nil {
		t.Fatal(err)
	}
	return engine
}

// The application rate-limiting the probe's own user (HTTP 429) is the probe's load, not an application failure:
// the sample is an invalid probe defect that names the wait, and the probe sends nothing to that target until the
// application's Retry-After has passed. A probe that kept sending (or that counted its own throttling as a failed
// user test) would hold an incident open on load SDO itself generates (an application that limits search per user).
func TestRateLimitedProbeBacksOffAndIsNotAFailure(t *testing.T) {
	clock := &manualClock{now: time.Date(2026, 11, 23, 6, 0, 0, 0, time.UTC)}
	api := &limitedAPI{limited: true, retryAfter: "30"}
	engine := limitedEngine(t, api, clock)

	_, sample := engine.Iterate(context.Background(), 0)
	if sample.Outcome != OutcomeInvalid || sample.Defect != DefectRateLimited || sample.Status != http.StatusTooManyRequests {
		t.Fatalf("a 429 to the probe must be an invalid rate-limit defect: %+v", sample)
	}
	if api.count() != 1 {
		t.Fatalf("sent %d requests, want 1", api.count())
	}
	clock.advance(10 * time.Second)
	_, sample = engine.Iterate(context.Background(), 1)
	if api.count() != 1 {
		t.Fatalf("the probe sent during the application's Retry-After: %d requests", api.count())
	}
	if sample.Outcome != OutcomeInvalid || sample.Defect != DefectRateLimited || sample.Requests != 0 {
		t.Fatalf("a sample inside the wait must be an invalid rate-limit defect that sent nothing: %+v", sample)
	}
	if !strings.Contains(sample.Error, "not an application failure") {
		t.Fatalf("the defect must say it is not an application failure: %q", sample.Error)
	}

	api.mu.Lock()
	api.limited = false
	api.mu.Unlock()
	clock.advance(21 * time.Second)
	if _, sample = engine.Iterate(context.Background(), 2); sample.Outcome != OutcomeOK || api.count() != 2 {
		t.Fatalf("after Retry-After the probe must send and judge again: %+v (sent %d)", sample, api.count())
	}
}

// Engines of one process that share setup (the continuous probe and its verify bursts) share the wait too: a burst
// started while the steady probe is told to wait sends nothing.
func TestSharedEnginesShareTheRateLimitWait(t *testing.T) {
	clock := &manualClock{now: time.Date(2026, 11, 23, 6, 0, 0, 0, time.UTC)}
	api := &limitedAPI{limited: true, retryAfter: "60"}
	steady := limitedEngine(t, api, clock)
	burst := limitedEngine(t, api, clock)
	burst.ShareSetup(steady)

	steady.Iterate(context.Background(), 0)
	clock.advance(time.Second)
	if _, sample := burst.Iterate(context.Background(), 0); sample.Defect != DefectRateLimited || api.count() != 1 {
		t.Fatalf("a shared engine sent during the wait: %+v (sent %d)", sample, api.count())
	}
}

// Without Retry-After the probe waits a default window, and a Retry-After past the cap is capped, so a hostile or
// broken header can neither make the probe hammer the application nor blind it for long.
func TestRateLimitWaitDefaultAndCap(t *testing.T) {
	for _, tc := range []struct {
		header string
		want   time.Duration
	}{{"", DefaultRateLimitWait}, {"86400", MaxRateLimitWait}, {"garbage", DefaultRateLimitWait}, {"5", 5 * time.Second}} {
		clock := &manualClock{now: time.Date(2026, 11, 23, 6, 0, 0, 0, time.UTC)}
		api := &limitedAPI{limited: true, retryAfter: tc.header}
		engine := limitedEngine(t, api, clock)
		engine.Iterate(context.Background(), 0)
		clock.advance(tc.want - time.Second)
		engine.Iterate(context.Background(), 1)
		if api.count() != 1 {
			t.Fatalf("Retry-After %q: sent before %s", tc.header, tc.want)
		}
		clock.advance(2 * time.Second)
		engine.Iterate(context.Background(), 2)
		if api.count() != 2 {
			t.Fatalf("Retry-After %q: did not send after %s", tc.header, tc.want)
		}
	}
}

// A scenario whose check accepts 429 (a probe of the rate limiter itself) is judged by its check as before.
func TestA429TheCheckAcceptsIsNotADefect(t *testing.T) {
	scenario := listScenario()
	scenario.Steps = []Step{{Name: "list", Endpoint: GET("/api/repos", nil).Status(http.StatusTooManyRequests)}}
	clock := &manualClock{now: time.Date(2026, 11, 23, 6, 0, 0, 0, time.UTC)}
	api := &limitedAPI{limited: true, retryAfter: "30"}
	workload := Workload{APIVersion: APIVersion, Kind: WorkloadKind, Name: "probe-read-later", Purpose: PurposeHealthProbe, Scenarios: []WorkloadScenario{{ID: scenario.ID}}}
	engine, err := NewEngine(workload, Catalog{scenario}, api, func(Target) string { return "http://web:80" }, clock)
	if err != nil {
		t.Fatal(err)
	}
	if _, sample := engine.Iterate(context.Background(), 0); sample.Outcome != OutcomeOK || sample.Defect != "" {
		t.Fatalf("sample = %+v", sample)
	}
}

// A window of rate-limit defects is never a failing user test, and its reason says why.
func TestRateLimitDefectsNeverFailAScenario(t *testing.T) {
	clock := &manualClock{now: time.Date(2026, 11, 23, 6, 0, 0, 0, time.UTC)}
	engine := limitedEngine(t, &limitedAPI{limited: true, retryAfter: "30"}, clock)
	var samples []Sample
	for i := uint64(0); i < 10; i++ {
		_, sample := engine.Iterate(context.Background(), i)
		samples = append(samples, sample)
		clock.advance(time.Second)
	}
	last := samples[len(samples)-1]
	if verdict := Evaluate("list-repos", SLO{Window: 10, MinSamples: 1, MaxAge: Duration(time.Hour)}, samples, last.At); !verdict.Healthy || verdict.Errors != 0 {
		t.Fatalf("verdict = %+v", verdict)
	}
	reason := latestFailure(ScenarioObservations{Samples: samples, Invalid: len(samples), LastInvalid: last.Error})
	for _, want := range []string{"probe defect rate_limited", "not an application failure"} {
		if !strings.Contains(reason, want) {
			t.Fatalf("reason %q lacks %q", reason, want)
		}
	}
	if strings.Contains(reason, "page size") {
		t.Fatalf("a rate-limit defect must not give the truncation advice: %q", reason)
	}
}
