package traffic

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	// MaxErrorExcerpt bounds the response body or transport error kept per sample.
	MaxErrorExcerpt = 240
	// MaxResponseBody bounds how much of a response the engine reads.
	MaxResponseBody = 64 * 1024
)

// Outcome classifies one iteration of a scenario.
type Outcome string

const (
	OutcomeOK      Outcome = "ok"
	OutcomeError   Outcome = "error"
	OutcomeTimeout Outcome = "timeout"
	// OutcomeInvalid marks a generator bug: a request could not be built or
	// broke a safety rule, so nothing was sent. It is not a health signal.
	OutcomeInvalid Outcome = "invalid"
)

// Sample is the observed result of one scenario iteration.
type Sample struct {
	At        time.Time     `json:"at"`
	Latency   time.Duration `json:"latency"`
	Iteration uint64        `json:"iteration"`
	// Status is the last HTTP status received, or 0 when none arrived.
	Status  int     `json:"status,omitempty"`
	Outcome Outcome `json:"outcome"`
	// Step names the step that failed; Request is its method and URL path
	// with query, so the failure can be reproduced by hand.
	Step    string `json:"step,omitempty"`
	Request string `json:"request,omitempty"`
	// Error explains a failed sample: the transport error, the check's
	// reason, or a bounded excerpt of the unexpected response body.
	Error string `json:"error,omitempty"`
	// DialFailed marks a failure that happened while establishing the
	// connection (DNS or TCP dial), rather than while sending the request,
	// reading the response, or checking it. A NetworkPolicy or a stopped
	// listener that refuses new connections shows up here, distinctly from
	// an application-level error or a slow response, whether the dial
	// failed outright (Outcome error) or hung until the dial timeout
	// (Outcome timeout).
	DialFailed   bool   `json:"dialFailed,omitempty"`
	Requests     int    `json:"requests"`
	CleanupError string `json:"cleanupError,omitempty"`
}

// Doer sends HTTP requests; *http.Client satisfies it.
type Doer interface {
	Do(*http.Request) (*http.Response, error)
}

// Clock is the engine's only source of time.
type Clock interface {
	Now() time.Time
	After(time.Duration) <-chan time.Time
}

type realClock struct{}

func (realClock) Now() time.Time                         { return time.Now() }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// RealClock is the wall clock.
var RealClock Clock = realClock{}

// Engine runs one workload's scenarios. It alone decides which scenario an
// iteration runs, seeds its random source, bounds its time, and measures it.
type Engine struct {
	workload  Workload
	scenarios map[string]Scenario
	schedule  []string
	client    Doer
	baseURL   func(Target) string
	clock     Clock
}

// NewEngine validates workload against catalog and prepares its schedule.
// baseURL maps a scenario target to scheme://host:port.
func NewEngine(workload Workload, catalog Catalog, client Doer, baseURL func(Target) string, clock Clock) (*Engine, error) {
	if err := catalog.Validate(); err != nil {
		return nil, err
	}
	workload = workload.WithDefaults()
	if err := workload.Validate(); err != nil {
		return nil, err
	}
	if err := workload.ValidateAgainst(catalog); err != nil {
		return nil, err
	}
	if client == nil || baseURL == nil {
		return nil, fmt.Errorf("traffic engine needs an HTTP client and a base URL function")
	}
	if clock == nil {
		clock = RealClock
	}
	scenarios := make(map[string]Scenario, len(workload.Scenarios))
	for _, selected := range workload.Scenarios {
		scenario, _ := catalog.Scenario(selected.ID)
		scenarios[selected.ID] = scenario
	}
	return &Engine{
		workload: workload, scenarios: scenarios, schedule: weightedSchedule(workload.Scenarios),
		client: client, baseURL: baseURL, clock: clock,
	}, nil
}

// Workload returns the engine's workload with defaults applied.
func (e *Engine) Workload() Workload { return e.workload }

// Descriptors describe the workload's scenarios in workload order.
func (e *Engine) Descriptors() []Descriptor {
	descriptors := make([]Descriptor, 0, len(e.workload.Scenarios))
	for _, selected := range e.workload.Scenarios {
		descriptors = append(descriptors, e.scenarios[selected.ID].Describe())
	}
	return descriptors
}

// ScenarioAt is the scenario iteration runs. The schedule is a smooth
// weighted round-robin cycle, so every scenario runs at its weight's share
// and the choice depends only on the iteration number.
func (e *Engine) ScenarioAt(iteration uint64) Scenario {
	return e.scenarios[e.schedule[iteration%uint64(len(e.schedule))]]
}

// Rand is the random source of iteration: the same workload seed and
// iteration always yield the same values, so a failing probe replays exactly.
func (e *Engine) Rand(iteration uint64) *rand.Rand {
	return rand.New(rand.NewPCG(e.workload.Seed, iteration))
}

// Plan builds, without sending, the requests of iteration assuming every
// step succeeds without saving state. It is meant for replay and review.
func (e *Engine) Plan(ctx context.Context, iteration uint64) ([]Request, error) {
	scenario := e.ScenarioAt(iteration)
	rng := e.Rand(iteration)
	state := State{}
	requests := make([]Request, 0, len(scenario.Steps))
	for _, step := range scenario.Steps {
		request, err := step.Endpoint.Build(ctx, rng, state)
		if err != nil {
			return requests, fmt.Errorf("step %s: %w", step.Name, err)
		}
		requests = append(requests, request)
	}
	return requests, nil
}

// Iterate runs iteration once and never returns an error: every failure is
// a sample. Cleanup steps of a write-with-cleanup scenario run after any
// request was sent, even when a step failed; their failures are reported in
// CleanupError and do not change the outcome.
func (e *Engine) Iterate(ctx context.Context, iteration uint64) (string, Sample) {
	scenario := e.ScenarioAt(iteration)
	rng := e.Rand(iteration)
	state := State{}
	started := e.clock.Now()
	sample := Sample{At: started.UTC(), Iteration: iteration, Outcome: OutcomeOK}
	iterationCtx, cancel := context.WithTimeout(ctx, e.workload.IterationTimeout.Duration())
	defer cancel()
	base := e.baseURL(scenario.normalizedTarget())
	for _, step := range scenario.Steps {
		result := e.step(iterationCtx, scenario, step, rng, state, base, false)
		sample.Requests += result.sent
		if result.status != 0 {
			sample.Status = result.status
		}
		if result.outcome != OutcomeOK {
			sample.Outcome, sample.Step, sample.Request, sample.Error = result.outcome, step.Name, result.request, result.err
			sample.DialFailed = result.dial
			break
		}
	}
	sample.Latency = e.since(started)
	if scenario.SideEffect == SideEffectWriteWithCleanup && sample.Requests > 0 {
		cleanupCtx, cancelCleanup := context.WithTimeout(context.WithoutCancel(ctx), e.workload.IterationTimeout.Duration())
		defer cancelCleanup()
		for _, step := range scenario.Cleanup {
			result := e.step(cleanupCtx, scenario, step, rng, state, base, true)
			if result.outcome != OutcomeOK {
				sample.CleanupError = bounded(step.Name + ": " + result.err)
				break
			}
		}
	}
	return scenario.ID, sample
}

type stepResult struct {
	outcome Outcome
	status  int
	sent    int
	request string
	err     string
	dial    bool
}

func (e *Engine) step(ctx context.Context, scenario Scenario, step Step, rng *rand.Rand, state State, base string, cleanup bool) stepResult {
	request, err := step.Endpoint.Build(ctx, rng, state)
	if err != nil {
		return stepResult{outcome: OutcomeInvalid, err: bounded("build: " + err.Error())}
	}
	line := requestLine(request)
	if err := checkRequest(scenario, request, cleanup); err != nil {
		return stepResult{outcome: OutcomeInvalid, request: line, err: bounded(err.Error())}
	}
	requestCtx, cancel := context.WithTimeout(ctx, e.workload.Timeout.Duration())
	defer cancel()
	built, err := BuildHTTPRequest(requestCtx, base, request)
	if err != nil {
		return stepResult{outcome: OutcomeInvalid, request: line, err: bounded(err.Error())}
	}
	response, err := e.client.Do(built)
	if err != nil {
		dial := isDialError(err)
		if isTimeout(requestCtx, err) {
			return stepResult{outcome: OutcomeTimeout, sent: 1, request: line, err: bounded(err.Error()), dial: dial}
		}
		return stepResult{outcome: OutcomeError, sent: 1, request: line, err: bounded(err.Error()), dial: dial}
	}
	defer response.Body.Close()
	body, readErr := io.ReadAll(io.LimitReader(response.Body, MaxResponseBody))
	if readErr != nil {
		outcome := OutcomeError
		if isTimeout(requestCtx, readErr) {
			outcome = OutcomeTimeout
		}
		return stepResult{outcome: outcome, status: response.StatusCode, sent: 1, request: line, err: bounded(readErr.Error())}
	}
	verdict := step.Endpoint.Check(Response{Status: response.StatusCode, Header: response.Header.Clone(), Body: body}, state)
	if !verdict.OK {
		reason := verdict.Reason
		if strings.TrimSpace(reason) == "" {
			reason = fmt.Sprintf("HTTP %d rejected by check", response.StatusCode)
		}
		return stepResult{outcome: OutcomeError, status: response.StatusCode, sent: 1, request: line, err: bounded(reason)}
	}
	return stepResult{outcome: OutcomeOK, status: response.StatusCode, sent: 1, request: line}
}

// Run starts iterations at the workload's arrival pattern and rate, from
// iteration first, until ctx ends or, for bounded workloads, the duration
// elapses. At most MaxInFlight iterations run at once; an arrival that finds
// them all busy is skipped. It waits for running iterations and returns the
// next iteration number and the number of skipped arrivals.
func (e *Engine) Run(ctx context.Context, first uint64, emit func(scenario string, sample Sample)) (uint64, int) {
	var deadline <-chan time.Time
	if e.workload.Duration > 0 {
		deadline = e.clock.After(e.workload.Duration.Duration())
	}
	gaps := rand.New(rand.NewPCG(e.workload.Seed, math.MaxUint64))
	slots := make(chan struct{}, MaxInFlight)
	var running sync.WaitGroup
	defer running.Wait()
	iteration, skipped := first, 0
	for {
		select {
		case slots <- struct{}{}:
			running.Add(1)
			go func(iteration uint64) {
				defer running.Done()
				defer func() { <-slots }()
				scenario, sample := e.Iterate(ctx, iteration)
				if ctx.Err() == nil || sample.Outcome == OutcomeOK {
					emit(scenario, sample)
				}
			}(iteration)
		default:
			skipped++
		}
		iteration++
		select {
		case <-ctx.Done():
			return iteration, skipped
		case <-deadline:
			return iteration, skipped
		case <-e.clock.After(e.gap(gaps)):
		}
	}
}

func (e *Engine) gap(rng *rand.Rand) time.Duration {
	mean := float64(time.Second) / e.workload.RatePerSecond
	if e.workload.Arrival == ArrivalPoisson {
		// Bound the tail so one long gap cannot stall detection.
		return time.Duration(math.Min(rng.ExpFloat64()*mean, 4*mean))
	}
	return time.Duration(mean)
}

func (e *Engine) since(started time.Time) time.Duration {
	elapsed := e.clock.Now().Sub(started)
	if elapsed <= 0 {
		return time.Nanosecond
	}
	return elapsed
}

// BuildHTTPRequest renders request against baseURL (scheme://host:port) with
// a canonical, sorted query string and the synthetic marker header.
func BuildHTTPRequest(ctx context.Context, baseURL string, request Request) (*http.Request, error) {
	base, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("parse base URL: %w", err)
	}
	target := *base
	target.Path = request.Path
	query := url.Values{}
	for key, value := range request.Query {
		query.Set(key, value)
	}
	target.RawQuery = query.Encode()
	var body io.Reader
	if request.Body != "" {
		body = strings.NewReader(request.Body)
	}
	built, err := http.NewRequestWithContext(ctx, request.Method, target.String(), body)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(request.Headers))
	for name := range request.Headers {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		built.Header.Set(name, request.Headers[name])
	}
	built.Header.Set(SyntheticHeader, "1")
	return built, nil
}

func requestLine(request Request) string {
	query := url.Values{}
	for key, value := range request.Query {
		query.Set(key, value)
	}
	line := request.Method + " " + request.Path
	if encoded := query.Encode(); encoded != "" {
		line += "?" + encoded
	}
	return bounded(line)
}

// weightedSchedule is one cycle of smooth weighted round-robin.
func weightedSchedule(scenarios []WorkloadScenario) []string {
	total := 0
	for _, scenario := range scenarios {
		total += scenario.Weight
	}
	current := make([]int, len(scenarios))
	schedule := make([]string, 0, total)
	for len(schedule) < total {
		best := 0
		for index, scenario := range scenarios {
			current[index] += scenario.Weight
			if current[index] > current[best] {
				best = index
			}
		}
		current[best] -= total
		schedule = append(schedule, scenarios[best].ID)
	}
	return schedule
}

func isTimeout(ctx context.Context, err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return true
	}
	var timeout interface{ Timeout() bool }
	return errors.As(err, &timeout) && timeout.Timeout()
}

// isDialError reports whether err happened while establishing the
// connection (DNS lookup or TCP dial), rather than while writing the
// request, reading the response, or checking it. It covers both an
// outright refusal and a dial that hung until its own timeout, so a
// connection-level fault (for example a NetworkPolicy denying new
// connections) is classified the same way whether the failure was fast or
// slow.
func isDialError(err error) bool {
	var opErr *net.OpError
	return errors.As(err, &opErr) && opErr.Op == "dial"
}

func bounded(text string) string {
	text = strings.Join(strings.Fields(text), " ")
	if len(text) <= MaxErrorExcerpt {
		return text
	}
	return text[:MaxErrorExcerpt] + "…"
}
