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
	"sdo.dev/operator/sdk/internal/clockio"
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
	// Status is the last HTTP status received, or 0 when none arrived or
	// the last answer was an exit status.
	Status int `json:"status,omitempty"`
	// Exit is the exit status of the last EXEC, when the last answer was
	// one.
	Exit    *int    `json:"exit,omitempty"`
	Outcome Outcome `json:"outcome"`
	// Step names the step that failed; Request is its method and URL path
	// with query, so the failure can be reproduced by hand.
	Step    string `json:"step,omitempty"`
	Request string `json:"request,omitempty"`
	// Error explains a failed sample: the transport error, the check's
	// reason, or a bounded excerpt of the unexpected response body.
	Error string `json:"error,omitempty"`
	// Observed is what the other side answered, apart from the probe's own
	// Error: a bounded, redacted excerpt of the server's response body, or the
	// transport error cause without the client's method and URL prefix.
	Observed string `json:"observed,omitempty"`
	// DialFailed marks a failure that happened while establishing the
	// connection (DNS or TCP dial), rather than while sending the request,
	// reading the response, or checking it. A NetworkPolicy or a stopped
	// listener that refuses new connections shows up here, distinctly from
	// an application-level error or a slow response, whether the dial
	// failed outright (Outcome error) or hung until the dial timeout
	// (Outcome timeout).
	DialFailed bool `json:"dialFailed,omitempty"`
	// IdentityWait is set on an invalid sample whose probe user was still
	// being provisioned: how long one provisioning run may take. Such a sample
	// is an environment wait, not a finding about the application.
	IdentityWait time.Duration `json:"identityWait,omitempty"`
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

type realClock struct{ clockio.Clock }

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

	// setups records, per scenario, the setup steps that succeeded; engines
	// that ShareSetup use one record.
	setups *setupRecord
	// passwords returns the passwords of the probe users scenarios declare;
	// nil provisions none.
	passwords Passwords
	// execer runs EXEC steps on ssh targets; nil runs none.
	execer Execer
}

// UsePasswords lets scenarios that declare Users sign in with the passwords
// source returns. A prober gives every engine the same source.
func (e *Engine) UsePasswords(source Passwords) { e.passwords = source }

// setupRecord holds, per scenario, how far its setup steps got.
type setupRecord struct {
	mu       sync.Mutex
	progress map[string]setupProgress
}

// setupProgress is how many of a scenario's setup steps succeeded, in order,
// and the State they saved, without probe-user passwords.
type setupProgress struct {
	done  int
	saved State
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
		client: client, baseURL: baseURL, clock: clock, setups: &setupRecord{progress: map[string]setupProgress{}},
	}, nil
}

// ShareSetup makes e use source's record of completed setup steps, so a
// setup step that succeeded in either engine is never run again by the other. A
// process that runs the same catalog in several engines (a continuous probe
// and its verify bursts) shares one record: data a setup step wrote and that
// was lost afterwards then fails every engine instead of being written again.
// Both engines must run the same catalog.
func (e *Engine) ShareSetup(source *Engine) { e.setups = source.setups }

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
	for _, u := range scenario.Users {
		state[u.As] = passwordPlaceholder
		if u.SSHKey != "" {
			state[u.SSHKey] = publicKeyPlaceholder
		}
	}
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
// a sample. A scenario's setup steps run first until each has succeeded once in
// this engine (or an engine it shares setup with); a retry resumes at the
// first step that has not succeeded. A failed setup step fails the sample, and
// setup time is not part of its latency. Cleanup steps of a write-with-cleanup scenario run after any
// request was sent, even when a step failed; their failures are reported in
// CleanupError and do not change the outcome.
func (e *Engine) Iterate(ctx context.Context, iteration uint64) (_ string, result Sample) {
	if ctx == nil {
		// Agent-authored tests sometimes pass nil; treat it as "no deadline"
		// instead of panicking inside the clock.
		ctx = context.Background()
	}
	scenario := e.ScenarioAt(iteration)
	rng := e.Rand(iteration)
	state := State{}
	sample := Sample{At: e.clock.Now().UTC(), Iteration: iteration, Outcome: OutcomeOK}
	iterationCtx, cancel := e.withTimeout(ctx, e.workload.IterationTimeout.Duration())
	defer cancel()
	passwords, keys, err := e.userState(iterationCtx, scenario)
	if err != nil {
		// The probe cannot measure; that says nothing about the application.
		sample.Outcome, sample.Step, sample.Error = OutcomeInvalid, "probe-users", bounded(err.Error())
		sample.IdentityWait = identityWait(err)
		return scenario.ID, sample
	}
	// Texts are kept whole until the passwords are replaced, then bounded, so
	// a cut never leaves part of a password behind.
	defer func() { result.redact(passwords); result.bound() }()
	for _, values := range []State{passwords, keys} {
		for key, value := range values {
			state[key] = value
		}
	}
	// Setup draws from its own copy of the iteration's source, so the steps
	// build the same requests whether or not setup ran first.
	if failed := e.setup(iterationCtx, scenario, e.Rand(iteration), state, passwords, &sample); failed {
		return scenario.ID, sample
	}
	for _, values := range []State{passwords, keys} {
		for key, value := range values {
			state[key] = value
		}
	}
	started := e.clock.Now()
	for _, step := range scenario.Steps {
		result := e.step(iterationCtx, scenario, step, rng, state, passwords, phaseStep)
		sample.record(result)
		if result.outcome != OutcomeOK {
			sample.Outcome, sample.Step, sample.Request, sample.Error = result.outcome, step.Name, result.request, result.err
			sample.Observed, sample.DialFailed = result.observed, result.dial
			break
		}
	}
	sample.Latency = e.since(started)
	if scenario.SideEffect == SideEffectWriteWithCleanup && sample.Requests > 0 {
		cleanupCtx, cancelCleanup := e.withTimeout(context.WithoutCancel(ctx), e.workload.IterationTimeout.Duration())
		defer cancelCleanup()
		for _, step := range scenario.Cleanup {
			result := e.step(cleanupCtx, scenario, step, rng, state, passwords, phaseCleanup)
			if result.outcome != OutcomeOK {
				sample.CleanupError = step.Name + ": " + result.err
				break
			}
		}
	}
	return scenario.ID, sample
}

// setup runs scenario's setup steps that have not yet succeeded in this
// engine or one it shares setup with, starting from the State the earlier
// ones saved, and copies the State of all of them into state. A step that
// succeeded never runs again, so a write it made (a credential, a fixture) is
// not repeated when a later step fails and the setup is retried. It reports
// whether a setup step failed; the failure is then recorded in sample.
func (e *Engine) setup(ctx context.Context, scenario Scenario, rng *rand.Rand, state State, passwords State, sample *Sample) bool {
	if len(scenario.Setup) == 0 {
		return false
	}
	e.setups.mu.Lock()
	progress := e.setups.progress[scenario.ID]
	e.setups.mu.Unlock()
	if progress.done < len(scenario.Setup) {
		started := e.clock.Now()
		// Setup steps see the iteration's passwords; the record keeps only
		// what the steps saved.
		saved := progress.saved.clone()
		for key, value := range state {
			saved[key] = value
		}
		for index := progress.done; index < len(scenario.Setup); index++ {
			step := scenario.Setup[index]
			// A step may change State before it fails; only a passing step's
			// changes are kept.
			attempt := saved.clone()
			result := e.step(ctx, scenario, step, rng, attempt, passwords, phaseSetup)
			sample.record(result)
			if result.outcome != OutcomeOK {
				sample.Outcome, sample.Step, sample.Request, sample.Error = result.outcome, step.Name, result.request, result.err
				sample.Observed, sample.DialFailed = result.observed, result.dial
				sample.Latency = e.since(started)
				return true
			}
			saved = attempt
			e.setups.advance(scenario, index+1, saved)
		}
		progress = setupProgress{done: len(scenario.Setup), saved: saved}
	}
	for key, value := range progress.saved {
		state[key] = value
	}
	return false
}

// advance records that scenario's first done setup steps succeeded with
// saved, unless a concurrent iteration already recorded at least as many.
func (r *setupRecord) advance(scenario Scenario, done int, saved State) {
	kept := saved.clone()
	for _, u := range scenario.Users {
		delete(kept, u.As)
		delete(kept, u.SSHKey)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.progress[scenario.ID].done < done {
		r.progress[scenario.ID] = setupProgress{done: done, saved: kept}
	}
}

type stepResult struct {
	outcome Outcome
	status  int
	exit    *int
	sent    int
	request string
	err     string
	// observed is the server's body excerpt or the bare transport cause.
	observed string
	dial     bool
}

// record adds a step's request count and last answer to the sample.
func (sample *Sample) record(result stepResult) {
	sample.Requests += result.sent
	switch {
	case result.exit != nil:
		exit := *result.exit
		sample.Exit, sample.Status = &exit, 0
	case result.status != 0:
		sample.Exit, sample.Status = nil, result.status
	}
}

func (e *Engine) step(ctx context.Context, scenario Scenario, step Step, rng *rand.Rand, state State, passwords State, phase stepPhase) stepResult {
	request, err := step.Endpoint.Build(ctx, rng, state)
	if err != nil {
		return stepResult{outcome: OutcomeInvalid, err: "build: " + err.Error()}
	}
	target := scenario.stepTarget(step)
	lineTarget := step.Target
	if target.Scheme == "ssh" {
		lineTarget = &target
	}
	line := requestLine(request, lineTarget)
	if err := checkRequest(scenario, target, request, phase); err != nil {
		return stepResult{outcome: OutcomeInvalid, request: line, err: err.Error()}
	}
	if target.Scheme == "ssh" {
		if carriesPassword(request, passwords) {
			return stepResult{outcome: OutcomeInvalid, request: line, err: "an EXEC request may not send a probe-user password"}
		}
		return e.execStep(ctx, scenario, step, target, request, state, line)
	}
	requestCtx, cancel := e.withTimeout(ctx, e.workload.Timeout.Duration())
	defer cancel()
	built, err := BuildHTTPRequest(requestCtx, e.baseURL(target), request)
	if err != nil {
		return stepResult{outcome: OutcomeInvalid, request: line, err: err.Error()}
	}
	response, err := e.client.Do(built)
	if err != nil {
		dial := isDialError(err)
		cause := transportCause(err)
		if isTimeout(requestCtx, err) {
			return stepResult{outcome: OutcomeTimeout, sent: 1, request: line, err: err.Error(), observed: cause, dial: dial}
		}
		return stepResult{outcome: OutcomeError, sent: 1, request: line, err: err.Error(), observed: cause, dial: dial}
	}
	defer response.Body.Close()
	body, readErr := io.ReadAll(io.LimitReader(response.Body, MaxResponseBody))
	if readErr != nil {
		outcome := OutcomeError
		if isTimeout(requestCtx, readErr) {
			outcome = OutcomeTimeout
		}
		return stepResult{outcome: outcome, status: response.StatusCode, sent: 1, request: line, err: readErr.Error()}
	}
	verdict := step.Endpoint.Check(Response{Status: response.StatusCode, Header: response.Header.Clone(), Body: body}, state)
	if !verdict.OK {
		reason := verdict.Reason
		if strings.TrimSpace(reason) == "" {
			reason = fmt.Sprintf("HTTP %d rejected by check", response.StatusCode)
		}
		return stepResult{outcome: OutcomeError, status: response.StatusCode, sent: 1, request: line, err: reason, observed: observedExcerpt(body, response.StatusCode, "")}
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

// requestLine is the method and path of request; a step with its own
// target names it, and an EXEC always names its ssh target.
func requestLine(request Request, target *Target) string {
	prefix := ""
	if target != nil {
		normalized := target.normalized()
		user := ""
		if normalized.User != "" {
			user = normalized.User + "@"
		}
		prefix = fmt.Sprintf("%s://%s%s:%d", normalized.Scheme, user, normalized.Service, normalized.Port)
	}
	if request.Method == MethodExec {
		return strings.TrimSpace(request.Method + " " + prefix + " " + request.Path)
	}
	query := url.Values{}
	for key, value := range request.Query {
		query.Set(key, value)
	}
	line := request.Method + " " + prefix + request.Path
	if encoded := query.Encode(); encoded != "" {
		line += "?" + encoded
	}
	return line
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

// bound limits the sample's texts once its passwords are replaced.
func (sample *Sample) bound() {
	if sample.Error != "" {
		sample.Error = bounded(sample.Error)
	}
	if sample.Observed != "" {
		sample.Observed = bounded(sample.Observed)
	}
	if sample.CleanupError != "" {
		sample.CleanupError = bounded(sample.CleanupError)
	}
	if sample.Request != "" {
		sample.Request = bounded(sample.Request)
	}
}

// transportCause is err without the client's `Method "URL":` prefix.
func transportCause(err error) string {
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return urlErr.Err.Error()
	}
	return err.Error()
}

func bounded(text string) string {
	text = redactSecrets(text)
	text = strings.Join(strings.Fields(text), " ")
	if len(text) <= MaxErrorExcerpt {
		return text
	}
	return text[:MaxErrorExcerpt] + "…"
}

// withTimeout lets an injected clock drive request, cleanup and iteration
// deadlines as well as rate timers. Existing Clock implementations retain
// the wall-clock deadline default.
func (e *Engine) withTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if c, ok := e.clock.(interface {
		WithTimeout(context.Context, time.Duration) (context.Context, context.CancelFunc)
	}); ok {
		return c.WithTimeout(ctx, d)
	}
	return (clockio.Clock{}).WithTimeout(ctx, d)
}
