package traffic_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"sdo.dev/operator/sdk/traffic"
)

// recorder is a stand-in application that records what it received.
type recorder struct {
	mu       sync.Mutex
	requests []*http.Request
	bodies   []string
	handle   func(w http.ResponseWriter, r *http.Request)
}

func (r *recorder) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	buffer := new(strings.Builder)
	if request.Body != nil {
		_, _ = fmt.Fprint(buffer, readAll(request))
	}
	r.mu.Lock()
	r.requests = append(r.requests, request)
	r.bodies = append(r.bodies, buffer.String())
	r.mu.Unlock()
	if r.handle != nil {
		r.handle(w, request)
		return
	}
	_, _ = w.Write([]byte("ok"))
}

func readAll(request *http.Request) string {
	data := new(strings.Builder)
	buffer := make([]byte, 512)
	for {
		n, err := request.Body.Read(buffer)
		data.Write(buffer[:n])
		if err != nil {
			return data.String()
		}
	}
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.requests)
}

// handlerDoer executes the application handler in memory.
type handlerDoer struct{ handler http.Handler }

func (d handlerDoer) Do(r *http.Request) (*http.Response, error) {
	w := httptest.NewRecorder()
	d.handler.ServeHTTP(w, r)
	return w.Result(), nil
}

var applications sync.Map
var applicationID atomic.Uint64

func serve(t *testing.T, handler http.Handler) func(traffic.Target) string {
	t.Helper()
	address := fmt.Sprintf("http://app-%d.invalid", applicationID.Add(1))
	applications.Store(address, handlerDoer{handler})
	t.Cleanup(func() { applications.Delete(address) })
	return func(traffic.Target) string { return address }
}

type routedDoer struct{}

func (routedDoer) Do(r *http.Request) (*http.Response, error) {
	d, ok := applications.Load(r.URL.Scheme + "://" + r.URL.Host)
	if !ok {
		return nil, &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
	}
	return d.(traffic.Doer).Do(r)
}

type stepClock struct {
	now       time.Time
	deadlines []fakeDeadline
}
type fakeDeadline struct {
	at     time.Time
	cancel context.CancelCauseFunc
}
type deadlineContext struct {
	context.Context
	at time.Time
}

func (c deadlineContext) Deadline() (time.Time, bool) { return c.at, true }
func (c deadlineContext) Err() error {
	if context.Cause(c.Context) == context.DeadlineExceeded {
		return context.DeadlineExceeded
	}
	return c.Context.Err()
}
func (c *stepClock) WithTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	child, cancel := context.WithCancelCause(ctx)
	at := c.now.Add(d)
	if parent, ok := ctx.Deadline(); ok && parent.Before(at) {
		at = parent
	}
	c.deadlines = append(c.deadlines, fakeDeadline{at, cancel})
	return deadlineContext{child, at}, func() { cancel(context.Canceled) }
}
func (c *stepClock) advance(d time.Duration) {
	c.now = c.now.Add(d)
	for _, timer := range c.deadlines {
		if !c.now.Before(timer.at) {
			timer.cancel(context.DeadlineExceeded)
		}
	}
}

func (c *stepClock) Now() time.Time { c.advance(time.Millisecond); return c.now }
func (c *stepClock) After(d time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	c.advance(d)
	ch <- c.now
	return ch
}

func search() traffic.Scenario {
	return traffic.Scenario{
		ID: "search", Target: traffic.Target{Service: "frontend", Port: 5000}, DependsOn: []string{"search", "geo"},
		SideEffect: traffic.SideEffectRead,
		Steps: []traffic.Step{{Name: "search", Endpoint: traffic.GET("/hotels", traffic.Params{
			"inDate":  traffic.DateRange("2015-04-09", "2015-04-20"),
			"outDate": traffic.DaysAfter("inDate", 1, 3),
			"lat":     traffic.FloatBetween(37.7, 37.8, 4),
			"user":    traffic.FromSeed(500, func(index int) string { return fmt.Sprintf("user-%d", index) }),
		})}},
	}
}

func workload(t *testing.T, name string, purpose traffic.Purpose, scenarios ...traffic.WorkloadScenario) traffic.Workload {
	t.Helper()
	w := traffic.Workload{APIVersion: traffic.APIVersion, Kind: traffic.WorkloadKind, Name: name, Purpose: purpose, Scenarios: scenarios}
	if purpose != traffic.PurposeHealthProbe {
		w.Duration = traffic.Duration(time.Second)
	}
	return w.WithDefaults()
}

func engine(t *testing.T, w traffic.Workload, catalog traffic.Catalog, base func(traffic.Target) string) *traffic.Engine {
	t.Helper()
	e, err := traffic.NewEngine(w, catalog, routedDoer{}, base, &stepClock{now: time.Unix(0, 0)})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	return e
}

func TestGeneratedRequestsReplayExactlyFromTheIterationNumber(t *testing.T) {
	w := workload(t, "health", traffic.PurposeHealthProbe, traffic.WorkloadScenario{ID: "search"})
	first := engine(t, w, traffic.Catalog{search()}, func(traffic.Target) string { return "http://x" })
	second := engine(t, w, traffic.Catalog{search()}, func(traffic.Target) string { return "http://x" })
	a, err := first.Plan(context.Background(), 7)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	b, _ := second.Plan(context.Background(), 7)
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("same seed and iteration must build identical requests: %v vs %v", a, b)
	}
	other, _ := first.Plan(context.Background(), 8)
	if reflect.DeepEqual(a, other) {
		t.Fatalf("different iterations should vary generated parameters: %v", a)
	}
	query := a[0].Query
	in, _ := time.Parse(traffic.DateLayout, query["inDate"])
	out, _ := time.Parse(traffic.DateLayout, query["outDate"])
	nights := out.Sub(in).Hours() / 24
	if in.Before(time.Date(2015, 4, 9, 0, 0, 0, 0, time.UTC)) || nights < 1 || nights > 3 || !strings.HasPrefix(query["user"], "user-") {
		t.Fatalf("parameters must respect their generators: %v", query)
	}
}

func TestIterationSendsMarkedRequestsAndMeasuresThem(t *testing.T) {
	app := &recorder{}
	e := engine(t, workload(t, "health", traffic.PurposeHealthProbe, traffic.WorkloadScenario{ID: "search"}),
		traffic.Catalog{search()}, serve(t, app))
	id, sample := e.Iterate(context.Background(), 3)
	if id != "search" || sample.Outcome != traffic.OutcomeOK || sample.Status != 200 || sample.Requests != 1 || sample.Latency != time.Millisecond {
		t.Fatalf("healthy iteration: %s %+v", id, sample)
	}
	if app.requests[0].Header.Get(traffic.SyntheticHeader) != "1" || app.requests[0].URL.Path != "/hotels" {
		t.Fatalf("request must be marked synthetic: %+v", app.requests[0])
	}
}

func TestChainedStepsShareStateAndCleanupRunsAfterAFailure(t *testing.T) {
	app := &recorder{handle: func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost:
			_, _ = w.Write([]byte(`{"order": {"id": "o-17"}}`))
		case r.Method == http.MethodGet:
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte("upstream order service unavailable"))
		default:
			_, _ = w.Write([]byte("deleted"))
		}
	}}
	order := traffic.Scenario{
		ID: "order", Target: traffic.Target{Service: "shop", Port: 80}, SideEffect: traffic.SideEffectWriteWithCleanup,
		Marker: "sdo-synthetic",
		Steps: []traffic.Step{
			{Name: "create", Endpoint: traffic.POST("/orders", traffic.Params{"customer": traffic.Const("sdo-synthetic")}).SaveJSON("order.id", "order")},
			{Name: "read", Endpoint: traffic.GET("/orders/{order}", nil).Contains("o-17")},
		},
		Cleanup: []traffic.Step{{Name: "delete", Endpoint: traffic.DELETE("/orders/{order}", traffic.Params{
			"customer": traffic.Const("sdo-synthetic"),
		})}},
	}
	e := engine(t, workload(t, "journey", traffic.PurposeJourney, traffic.WorkloadScenario{ID: "order"}), traffic.Catalog{order}, serve(t, app))
	_, sample := e.Iterate(context.Background(), 0)
	if sample.Outcome != traffic.OutcomeError || sample.Step != "read" || sample.Status != 502 ||
		!strings.Contains(sample.Error, "upstream order service unavailable") || sample.Request != "GET /orders/o-17" {
		t.Fatalf("failing step must be named with its request and body: %+v", sample)
	}
	if app.count() != 3 || app.requests[2].Method != http.MethodDelete || app.requests[2].URL.Path != "/orders/o-17" || sample.CleanupError != "" {
		t.Fatalf("cleanup must run with the saved state after a failed step: %d requests, %+v", app.count(), sample)
	}
}

func TestEngineRefusesUnsafeRequestsWithoutSendingThem(t *testing.T) {
	app := &recorder{}
	sneaky := traffic.Scenario{ID: "sneaky", Target: traffic.Target{Service: "shop", Port: 80}, SideEffect: traffic.SideEffectRead,
		Steps: []traffic.Step{{Name: "write", Endpoint: traffic.POST("/orders", nil)}}}
	unmarked := traffic.Scenario{ID: "unmarked", Target: traffic.Target{Service: "shop", Port: 80},
		SideEffect: traffic.SideEffectIdempotentWrite, Marker: "sdo-synthetic",
		Steps: []traffic.Step{{Name: "write", Endpoint: traffic.PUT("/users/alice", nil)}}}
	for _, scenario := range []traffic.Scenario{sneaky, unmarked} {
		e := engine(t, workload(t, "journey", traffic.PurposeJourney, traffic.WorkloadScenario{ID: scenario.ID}),
			traffic.Catalog{scenario}, serve(t, app))
		_, sample := e.Iterate(context.Background(), 0)
		if sample.Outcome != traffic.OutcomeInvalid || sample.Requests != 0 {
			t.Fatalf("%s: unsafe request must be refused, got %+v", scenario.ID, sample)
		}
	}
	if app.count() != 0 {
		t.Fatalf("refused requests must never reach the application, got %d", app.count())
	}
}

func TestEngineClassifiesTimeoutsAndConnectionRefused(t *testing.T) {
	// A timed-out response is returned by the injected transport, without
	// waiting for an operating-system deadline.
	base := serve(t, http.NotFoundHandler())
	clock := &stepClock{now: time.Unix(0, 0)}
	applications.Store(base(traffic.Target{}), deadlineDoer{t: t, clock: clock})
	w := workload(t, "health", traffic.PurposeHealthProbe, traffic.WorkloadScenario{ID: "search"})
	w.Timeout = traffic.Duration(50 * time.Millisecond)
	e, err := traffic.NewEngine(w, traffic.Catalog{search()}, routedDoer{}, base, clock)
	if err != nil {
		t.Fatal(err)
	}
	_, sample := e.Iterate(context.Background(), 0)
	if sample.Outcome != traffic.OutcomeTimeout || sample.DialFailed {
		t.Fatalf("a hung response must time out without being classified as a dial failure, got %+v", sample)
	}
	_, sample = engine(t, w, traffic.Catalog{search()}, func(traffic.Target) string { return "http://closed.invalid" }).Iterate(context.Background(), 0)
	if sample.Outcome != traffic.OutcomeError || !strings.Contains(sample.Error, "refused") || !sample.DialFailed {
		t.Fatalf("a closed port must fail with a classified dial failure, got %+v", sample)
	}
}

type deadlineDoer struct {
	t     *testing.T
	clock *stepClock
}

func (d deadlineDoer) Do(r *http.Request) (*http.Response, error) {
	if _, ok := r.Context().Deadline(); !ok {
		d.t.Fatal("request has no timeout")
	}
	d.clock.advance(50 * time.Millisecond)
	select {
	case <-r.Context().Done():
	default:
		d.t.Fatal("request deadline did not cancel the hung response")
	}
	return nil, r.Context().Err()
}

// stubDoer lets a test hand the engine a synthetic transport error without
// touching a real network, so a dial timeout (which a closed listener alone
// cannot reproduce portably: the OS still completes the handshake unless
// something drops the packets) can still be classified deterministically.
type stubDoer struct{ err error }

func (s stubDoer) Do(*http.Request) (*http.Response, error) { return nil, s.err }

// stubTimeout satisfies the unexported `interface{ Timeout() bool }` that
// net/http and this package's isTimeout check for, purely by structure.
type stubTimeout struct{ msg string }

func (e stubTimeout) Error() string { return e.msg }
func (e stubTimeout) Timeout() bool { return true }

func TestEngineClassifiesDialFailuresSeparatelyFromOtherTransportErrors(t *testing.T) {
	cases := []struct {
		name           string
		err            error
		wantOutcome    traffic.Outcome
		wantDialFailed bool
	}{
		{
			name:           "a dial that hangs until its own timeout",
			err:            &net.OpError{Op: "dial", Net: "tcp", Err: stubTimeout{msg: "i/o timeout"}},
			wantOutcome:    traffic.OutcomeTimeout,
			wantDialFailed: true,
		},
		{
			name:           "a dial refused outright",
			err:            &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connect: connection refused")},
			wantOutcome:    traffic.OutcomeError,
			wantDialFailed: true,
		},
		{
			name:           "a slow response after a successful dial",
			err:            &net.OpError{Op: "read", Net: "tcp", Err: stubTimeout{msg: "i/o timeout"}},
			wantOutcome:    traffic.OutcomeTimeout,
			wantDialFailed: false,
		},
		{
			name:           "a generic transport error with no dial phase",
			err:            errors.New("unexpected EOF"),
			wantOutcome:    traffic.OutcomeError,
			wantDialFailed: false,
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			w := workload(t, "health", traffic.PurposeHealthProbe, traffic.WorkloadScenario{ID: "search"})
			e, err := traffic.NewEngine(w, traffic.Catalog{search()}, stubDoer{err: testCase.err},
				func(traffic.Target) string { return "http://x" }, &stepClock{now: time.Unix(0, 0)})
			if err != nil {
				t.Fatalf("new engine: %v", err)
			}
			_, sample := e.Iterate(context.Background(), 0)
			if sample.Outcome != testCase.wantOutcome || sample.DialFailed != testCase.wantDialFailed {
				t.Fatalf("got outcome=%s dialFailed=%v, want outcome=%s dialFailed=%v: %+v",
					sample.Outcome, sample.DialFailed, testCase.wantOutcome, testCase.wantDialFailed, sample)
			}
		})
	}
}

func TestScheduleFollowsWeightsDeterministically(t *testing.T) {
	login := traffic.Scenario{ID: "login", Target: traffic.Target{Service: "frontend", Port: 5000}, SideEffect: traffic.SideEffectRead,
		Steps: []traffic.Step{{Name: "login", Endpoint: traffic.GET("/user", nil)}}}
	e := engine(t, workload(t, "health", traffic.PurposeHealthProbe,
		traffic.WorkloadScenario{ID: "search", Weight: 3}, traffic.WorkloadScenario{ID: "login", Weight: 1}),
		traffic.Catalog{search(), login}, func(traffic.Target) string { return "http://x" })
	got := []string{}
	for iteration := uint64(0); iteration < 8; iteration++ {
		got = append(got, e.ScenarioAt(iteration).ID)
	}
	want := []string{"search", "search", "login", "search", "search", "search", "login", "search"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("smooth weighted round-robin: got %v want %v", got, want)
	}
}

type immediateDoer struct{}

func (immediateDoer) Do(*http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("ok"))}, nil
}

// rateClock exposes timer registration so the test drives each arrival only
// after Run has armed its next timer. No OS clock or network scheduling is used.
type rateTimer struct {
	delay time.Duration
	fire  chan time.Time
}
type rateClock struct {
	mu     sync.Mutex
	now    time.Time
	timers chan rateTimer
}

func (c *rateClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *rateClock) After(d time.Duration) <-chan time.Time {
	fire := make(chan time.Time, 1)
	c.timers <- rateTimer{d, fire}
	return fire
}
func (c *rateClock) advance(d time.Duration) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	return c.now
}

func receiveRate[T any](t *testing.T, ctx context.Context, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-ctx.Done():
		t.Fatal("rate test did not reach its next synchronization point")
	}
	var zero T
	return zero
}

func TestRunHonoursTheRateAndBoundedDuration(t *testing.T) {
	w := workload(t, "verify", traffic.PurposeVerifyBurst, traffic.WorkloadScenario{ID: "search"})
	w.RatePerSecond, w.Duration = 20, traffic.Duration(250*time.Millisecond)
	clock := &rateClock{now: time.Unix(0, 0), timers: make(chan rateTimer, 16)}
	e, err := traffic.NewEngine(w, traffic.Catalog{search()}, immediateDoer{}, func(traffic.Target) string { return "http://app" }, clock)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	emitted := make(chan traffic.Sample, 16)
	done := make(chan struct{})
	var next uint64
	var skipped int
	go func() {
		defer close(done)
		next, skipped = e.Run(ctx, 0, func(_ string, sample traffic.Sample) { emitted <- sample })
	}()
	deadline := receiveRate(t, ctx, clock.timers)
	if deadline.delay != 250*time.Millisecond {
		t.Fatalf("duration: %s", deadline.delay)
	}
	for iteration := 0; iteration < 5; iteration++ {
		gap := receiveRate(t, ctx, clock.timers)
		if gap.delay != 50*time.Millisecond {
			t.Fatalf("arrival gap: %s", gap.delay)
		}
		sample := receiveRate(t, ctx, emitted)
		if sample.Outcome != traffic.OutcomeOK {
			t.Fatalf("sample: %+v", sample)
		}
		at := clock.advance(gap.delay)
		if iteration == 4 {
			deadline.fire <- at
		} else {
			gap.fire <- at
		}
	}
	receiveRate(t, ctx, done)
	if next != 5 || skipped != 0 {
		t.Fatalf("20/s for 250ms: next=%d skipped=%d", next, skipped)
	}
}

func TestWorkloadValidationRejectsUnsafeProfiles(t *testing.T) {
	cases := map[string]string{
		"rate above cap":        `"ratePerSecond": 50`,
		"health probe duration": `"duration": "5s"`,
		"timeout above cap":     `"timeout": "20s"`,
		"unknown field":         `"burst": true`,
	}
	for name, field := range cases {
		document := `{"apiVersion": "sdo.dev/v1alpha1", "kind": "TrafficWorkload", "name": "health", "purpose": "health-probe", ` +
			field + `, "scenarios": [{"id": "search"}]}`
		if _, err := traffic.ParseWorkloadJSON([]byte(document)); err == nil {
			t.Fatalf("%s must be rejected", name)
		}
	}
	w := workload(t, "health", traffic.PurposeHealthProbe, traffic.WorkloadScenario{ID: "missing"})
	if err := w.ValidateAgainst(traffic.Catalog{search()}); err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("a workload naming an unknown scenario must be rejected, got %v", err)
	}
}

func TestScenarioValidationRequiresMarkedWritesAndCleanup(t *testing.T) {
	write := traffic.Scenario{ID: "reserve", Target: traffic.Target{Service: "frontend", Port: 5000},
		SideEffect: traffic.SideEffectWriteWithCleanup, Marker: "sdo-synthetic",
		Steps: []traffic.Step{{Name: "reserve", Endpoint: traffic.POST("/reservation", nil)}}}
	if err := write.Validate(); err == nil || !strings.Contains(err.Error(), "cleanup") {
		t.Fatalf("write-with-cleanup without cleanup must be rejected, got %v", err)
	}
	write.SideEffect, write.Marker = traffic.SideEffectIdempotentWrite, ""
	if err := write.Validate(); err == nil || !strings.Contains(err.Error(), "Marker") {
		t.Fatalf("a write without a marker must be rejected, got %v", err)
	}
	if err := (traffic.Catalog{search(), search()}).Validate(); err == nil {
		t.Fatalf("duplicate scenario IDs must be rejected")
	}
}

func TestConformanceRequiresScenariosToDetectTheirFaultClasses(t *testing.T) {
	if failures := traffic.CheckCatalog(context.Background(), traffic.Catalog{search()}); len(failures) != 0 {
		t.Fatalf("a plain GET detects unreachable and error-status targets: %v", failures)
	}
	weak := search()
	weak.Detects = []traffic.FaultClass{traffic.FaultWrongBody}
	failures := traffic.CheckCatalog(context.Background(), traffic.Catalog{weak})
	if len(failures) != 1 || !strings.Contains(failures[0].Error(), "wrong-body") {
		t.Fatalf("declaring wrong-body without a body check must fail conformance, got %v", failures)
	}
	strong := search()
	strong.Detects = []traffic.FaultClass{traffic.FaultWrongBody, traffic.FaultSlow}
	strong.Steps[0].Endpoint = traffic.GET("/hotels", nil).Contains("features")
	if failures := traffic.CheckCatalog(context.Background(), traffic.Catalog{strong}); len(failures) != 0 {
		t.Fatalf("a body check detects wrong bodies and the engine detects slowness: %v", failures)
	}
	broken := search()
	broken.Steps[0].Endpoint = traffic.GET("/orders/{order}", nil)
	failures = traffic.CheckCatalog(context.Background(), traffic.Catalog{broken})
	if len(failures) == 0 || !strings.Contains(failures[0].Error(), "cannot build") {
		t.Fatalf("a scenario that cannot build its first request must fail conformance, got %v", failures)
	}
}
