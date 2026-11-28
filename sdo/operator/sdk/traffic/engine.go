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
	"strconv"
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

// DefectResponseTruncated marks an invalid sample whose successful (2xx) step
// response was longer than MaxResponseBody and failed its check on the cut
// body. A non-2xx answer stays an application failure however large its body. The probe
// cannot judge such a response, so it says nothing about the application: the
// step must request less data (a smaller page).
const DefectResponseTruncated = "response_truncated"

// DefectRateLimited marks an invalid sample the application refused with HTTP
// 429 (a step whose check does not expect it), or one the engine did not send
// because that refusal's wait has not passed. The application is limiting the
// probe's own user: that is the probe's load, not an application failure, and
// the probe honours Retry-After (DefaultRateLimitWait without one, at most
// MaxRateLimitWait) before it sends to that target again. Real users the
// application limits show up in its request metrics, not here.
const DefectRateLimited = "rate_limited"

// DefaultRateLimitWait is how long the probe waits after a 429 without a
// usable Retry-After: the per-minute windows rate limiters use.
const DefaultRateLimitWait = time.Minute

// MaxRateLimitWait caps the wait a Retry-After can ask for, so a broken header
// cannot blind the probe for long.
const MaxRateLimitWait = 5 * time.Minute

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
	// Defect is the machine-readable probe-authoring defect behind an invalid
	// sample (DefectResponseTruncated), and ByteCap the response cap it names.
	// A defect is never an application failure.
	Defect  string `json:"defect,omitempty"`
	ByteCap int    `json:"byteCap,omitempty"`
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
	Requests     int           `json:"requests"`
	// ExistingRecords says what the scenario's ReadsExistingRecords step saw
	// on an iteration that passed: exercised, own-only or empty. It is an
	// operability note, never a failure: a fresh install has no user data.
	ExistingRecords string `json:"existingRecords,omitempty"`
	// ExistingMarked is how many records that step listed carry the
	// scenario's Marker: probe data users see. A count that keeps rising over
	// a run means the probes leave data behind.
	ExistingMarked int `json:"existingMarked,omitempty"`
	// Convergence says, for each step that answered NotYet, how many attempts
	// it took and for how long, whether it passed or ran out of time.
	Convergence  []Convergence `json:"convergence,omitempty"`
	CleanupError string        `json:"cleanupError,omitempty"`
	// Done is when the sample's steps finished (cleanup excluded): when its
	// outcome was observed. See ObservedAt.
	Done time.Time `json:"done,omitempty"`
}

// ObservedAt is when the sample's outcome was known: Done, or, for a sample
// recorded without it, its start plus its latency. Freshness (maxAge) is
// judged from it, so a step that waited for an effect until near the
// iteration deadline still counts; ordering, onsets and "since" rules keep At.
func (sample Sample) ObservedAt() time.Time {
	if !sample.Done.IsZero() {
		return sample.Done
	}
	return sample.At.Add(sample.Latency)
}

// Convergence is how a step that answered NotYet went in a sample.
type Convergence struct {
	Step      string        `json:"step"`
	Attempts  int           `json:"attempts"`
	Took      time.Duration `json:"took"`
	Converged bool          `json:"converged"`
}

// pendingPause is how long the engine waits before sending a pending step again.
const pendingPause = 250 * time.Millisecond

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
	plan      SamplingPlan
	client    Doer
	baseURL   func(Target) string
	clock     Clock
	// nonce is this engine's run nonce, mixed into every iteration's random
	// source so a restarted prober never replays the previous run's values.
	nonce uint64

	// setups records, per scenario, the setup steps that succeeded; engines
	// that ShareSetup use one record.
	setups *setupRecord
	// passwords returns the passwords of the probe users scenarios declare;
	// nil provisions none.
	passwords Passwords
	// execer runs EXEC steps on ssh targets; nil runs none.
	execer Execer
	// limits holds, per target, until when the application asked the probe to
	// wait (HTTP 429); engines that ShareSetup use one record.
	limits *rateLimits
	// warming reports whether the probe is warming up (UseWarmup); nil never.
	warming func() bool
	// startsFailing says whether a scheduled sample began a failure
	// (UseConfirm); nil samples nothing again.
	startsFailing func(scenario string) bool
	// confirmed receives the confirming samples, apart from the scheduled ones.
	confirmed func(scenario string, sample Sample)
	// identities serializes the samples that act on one synthetic identity;
	// engines that ShareSetup use one record. identityKeys are each writing
	// scenario's identities (writeIdentity).
	identities   *identityLocks
	identityKeys map[string][]string
	// stepLog receives every sent request and its answer (UseStepLog).
	stepLog func(StepLog)
}

// StepLog is one request a scenario sent and what came back, with probe-user
// credentials redacted: the line a prober's log would print.
type StepLog struct {
	Phase   string
	Step    string
	Request string
	Status  int
	Took    time.Duration
	Answer  string
	Error   string
}

func (l StepLog) String() string {
	line := fmt.Sprintf("%s %s: %s", l.Phase, l.Step, l.Request)
	if l.Status != 0 {
		line += fmt.Sprintf(" -> %d", l.Status)
	}
	line += fmt.Sprintf(" in %s", l.Took.Round(time.Millisecond))
	if l.Answer != "" {
		line += " " + l.Answer
	}
	if l.Error != "" {
		line += "; " + l.Error
	}
	return line
}

// UseStepLog sends every request the engine sends, and its answer, to log.
// It is for a person or agent trying one scenario; a prober leaves it unset.
func (e *Engine) UseStepLog(log func(StepLog)) { e.stepLog = log }

// rateLimits is, per target base URL, the end of the wait the application
// last asked for.
type rateLimits struct {
	mu    sync.Mutex
	until map[string]time.Time
}

// waiting is the end of the wait for base, if it has not passed at now.
func (r *rateLimits) waiting(base string, now time.Time) (time.Time, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	until, ok := r.until[base]
	return until, ok && now.Before(until)
}

// wait records that the application asked the probe to wait for base until until.
func (r *rateLimits) wait(base string, until time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.until == nil {
		r.until = map[string]time.Time{}
	}
	if until.After(r.until[base]) {
		r.until[base] = until
	}
}

// retryAfter is the wait a 429 asks for: its Retry-After in seconds or as an
// HTTP date, DefaultRateLimitWait when absent or unreadable, never more than
// MaxRateLimitWait.
func retryAfter(header http.Header, now time.Time) time.Duration {
	wait := DefaultRateLimitWait
	value := strings.TrimSpace(header.Get("Retry-After"))
	if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
		wait = time.Duration(seconds) * time.Second
	} else if at, err := http.ParseTime(value); err == nil {
		wait = at.Sub(now)
	}
	if wait <= 0 {
		wait = time.Second
	}
	return min(wait, MaxRateLimitWait)
}

// UsePasswords lets scenarios that declare Users sign in with the passwords
// source returns. A prober gives every engine the same source.
func (e *Engine) UsePasswords(source Passwords) { e.passwords = source }

// setupRecord holds, per scenario, how far its setup steps got.
type setupRecord struct {
	mu       sync.Mutex
	progress map[string]setupProgress
	// running serializes a scenario's setup: iterations run concurrently, and
	// two that both saw the setup unfinished would each write its fixture.
	running map[string]*sync.Mutex
	// fixtures holds, per setup request, the State its first success saved.
	// Scenarios that share a fixture (same request) send it once.
	fixtures map[string]State
	// locker serializes one setup request across processes; nil is none.
	locker SetupLocker
}

// SetupLocker serializes a setup request across probe processes (a hot-swapped
// memory host overlaps its predecessor, a restart replays setup). key names the
// request, not the scenario, so every scenario and process that would write the
// same fixture waits for the others. The in-process lock is always held too.
type SetupLocker interface {
	Lock(ctx context.Context, key string) (release func(), err error)
}

// UseSetupLock makes e serialize each setup request across processes with l.
func (e *Engine) UseSetupLock(l SetupLocker) {
	e.setups.mu.Lock()
	e.setups.locker = l
	e.setups.mu.Unlock()
}

// fixtureKey identifies the write a setup request makes: its target and the
// request itself, never the scenario that declares it or the credentials it
// signs in with.
func fixtureKey(target Target, request Request) string {
	query := make([]string, 0, len(request.Query))
	for k, v := range request.Query {
		query = append(query, k+"="+v)
	}
	sort.Strings(query)
	return fmt.Sprintf("%s:%d|%s %s?%s|%s", target.Service, target.Port, request.Method, request.Path, strings.Join(query, "&"), request.Body)
}

// lockFixture waits for every other setup of the same request, in this process
// and through the locker in others.
func (r *setupRecord) lockFixture(ctx context.Context, key string) (func(), error) {
	release := r.lock("fixture:" + key)
	r.mu.Lock()
	locker := r.locker
	r.mu.Unlock()
	if locker == nil {
		return release, nil
	}
	remote, err := locker.Lock(ctx, key)
	if err != nil {
		release()
		return nil, err
	}
	return func() { remote(); release() }, nil
}

// lock waits for any setup of the scenario in progress and returns the
// release function.
func (r *setupRecord) lock(id string) func() {
	r.mu.Lock()
	m := r.running[id]
	if m == nil {
		if r.running == nil {
			r.running = map[string]*sync.Mutex{}
		}
		m = &sync.Mutex{}
		r.running[id] = m
	}
	r.mu.Unlock()
	m.Lock()
	return m.Unlock
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
	identityKeys := map[string][]string{}
	for _, selected := range workload.Scenarios {
		scenario, _ := catalog.Scenario(selected.ID)
		scenarios[selected.ID] = scenario
		if keys := writeIdentity(scenario); len(keys) > 0 {
			identityKeys[selected.ID] = keys
		}
	}
	return &Engine{
		nonce:    newRunNonce(clock.Now()),
		workload: workload, scenarios: scenarios, plan: workload.SamplingPlan(),
		client: client, baseURL: baseURL, clock: clock, setups: &setupRecord{progress: map[string]setupProgress{}},
		limits: &rateLimits{}, identities: &identityLocks{}, identityKeys: identityKeys,
	}, nil
}

// ShareSetup makes e use source's record of completed setup steps, so a
// setup step that succeeded in either engine is never run again by the other,
// source's rate-limit waits, so neither sends while the application asked
// the other to wait, and source's identity locks, so a writing sample of
// either engine never overlaps one of the other that acts on the same
// synthetic identity (Iterate). A
// process that runs the same catalog in several engines (a continuous probe
// and its verify bursts) shares one record: data a setup step wrote and that
// was lost afterwards then fails every engine instead of being written again.
// Both engines must run the same catalog.
func (e *Engine) ShareSetup(source *Engine) {
	locker := e.setups.locker
	e.setups = source.setups
	e.limits = source.limits
	e.identities = source.identities
	if e.setups.locker == nil {
		e.setups.mu.Lock()
		e.setups.locker = locker
		e.setups.mu.Unlock()
	}
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

// ScenarioAt is the scenario iteration runs. The choice depends only on the
// iteration number: it follows the workload's SamplingPlan, which gives every
// scenario of a health probe its floor and shares the rest by weight.
func (e *Engine) ScenarioAt(iteration uint64) Scenario {
	return e.scenarios[e.plan.Schedule[iteration%uint64(len(e.plan.Schedule))]]
}

// IterationOf is the first iteration at or after from that runs scenario: the
// request the schedule would send for it, so running it now repeats a
// scheduled request. ok is false for a scenario the workload does not run.
func (e *Engine) IterationOf(scenario string, from uint64) (uint64, bool) {
	cycle := uint64(len(e.plan.Schedule))
	for offset := uint64(0); offset < cycle; offset++ {
		if e.plan.Schedule[(from+offset)%cycle] == scenario {
			return from + offset, true
		}
	}
	return 0, false
}

// Rate is the iterations per second the engine starts: the workload's rate,
// raised when its scenarios' sampling floor needs more.
func (e *Engine) Rate() float64 { return e.plan.Rate }

// UseWarmup makes Run pace iterations at the workload's WarmupRate while
// warming reports true, and at Rate otherwise. The caller owns the
// predicate: a prober warms up from its start or reset until every scenario
// holds its SLO's minimum samples.
func (e *Engine) UseWarmup(warming func() bool) { e.warming = warming }

// Rand is the random source of iteration: within one engine the same
// iteration always yields the same values, so a failing probe replays, and
// distinct iterations yield distinct ones. The engine's run nonce is part of
// the source, so a restarted prober (whose iterations count from 0 again)
// draws different values than the run before it.
func (e *Engine) Rand(iteration uint64) *rand.Rand {
	return rand.New(rand.NewPCG(e.workload.Seed^e.nonce, iteration))
}

// UseNonce fixes the run nonce, for tests that need values reproducible
// across engines. Production engines keep the random nonce NewEngine draws.
func (e *Engine) UseNonce(nonce uint64) { e.nonce = nonce }

// newRunNonce draws a nonce from the start time and random bits; it is
// persisted nowhere, so every engine start gets a new one.
func newRunNonce(start time.Time) uint64 {
	return uint64(start.UnixNano())*0x9e3779b97f4a7c15 ^ clockio.RandomBits()
}

// Plan builds, without sending, the requests of iteration assuming every
// step succeeds without saving state, as if it started now. It is meant for
// review; replay uses PlanAt with the failing sample's At.
func (e *Engine) Plan(ctx context.Context, iteration uint64) ([]Request, error) {
	return e.PlanAt(ctx, iteration, e.clock.Now())
}

// PlanAt is Plan for an iteration that started at at (a sample's At), so the
// requests that depend on the iteration's time are the ones it sent.
func (e *Engine) PlanAt(ctx context.Context, iteration uint64, at time.Time) ([]Request, error) {
	scenario := e.ScenarioAt(iteration)
	rng := e.Rand(iteration)
	state := State{StateNow: at.UTC().Format(time.RFC3339)}
	for _, u := range scenario.Users {
		if u.As != "" {
			state[u.As] = passwordPlaceholder
		}
		if u.Token != "" {
			state[u.Token] = "[token]"
		}
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
//
// A writing scenario's iteration first waits until no other iteration of this
// engine, or of one it shares setup with, is acting on the same synthetic
// identity (the same requests, or the same probe user): its read-back must
// see its own write, not one a concurrent sample made in between. The wait is
// before the sample starts, so it is not part of the sample's time or latency.
func (e *Engine) Iterate(ctx context.Context, iteration uint64) (string, Sample) {
	return e.IterateWithTrace(ctx, iteration, nil)
}

// IterateWithTrace runs the same iteration as Iterate. When trace is set it
// receives the name of every attempted setup, journey, and cleanup step,
// in order. It receives no request, response, or state values.
func (e *Engine) IterateWithTrace(ctx context.Context, iteration uint64, trace func(step string)) (_ string, result Sample) {
	if ctx == nil {
		// Agent-authored tests sometimes pass nil; treat it as "no deadline"
		// instead of panicking inside the clock.
		ctx = context.Background()
	}
	scenario := e.ScenarioAt(iteration)
	release, err := e.identities.lock(ctx, e.identityKeys[scenario.ID])
	if err != nil {
		// Nothing was sent; that says nothing about the application.
		return scenario.ID, Sample{At: e.clock.Now().UTC(), Iteration: iteration, Outcome: OutcomeInvalid, Step: "identity", Error: "waiting for the scenario's synthetic identity: " + err.Error()}
	}
	defer release()
	rng := e.Rand(iteration)
	state := State{}
	sample := Sample{At: e.clock.Now().UTC(), Iteration: iteration, Outcome: OutcomeOK}
	// The iteration's time is State, recorded as sample.At, so Build never
	// reads a clock and a replay with that time builds the same requests.
	state[StateNow] = sample.At.Format(time.RFC3339)
	iterationCtx, cancel := e.withTimeout(ctx, e.workload.IterationTimeout.Duration())
	defer cancel()
	passwords, keys, err := e.userState(iterationCtx, scenario)
	if err != nil {
		// The probe cannot measure; that says nothing about the application.
		sample.Outcome, sample.Step, sample.Error = OutcomeInvalid, "probe-users", bounded(err.Error())
		sample.IdentityWait = identityWait(err)
		return scenario.ID, sample
	}
	// Texts are kept whole until credentials are replaced, then bounded, so
	// a cut never leaves part of a credential behind.
	defer func() { result.redact(passwords); result.bound() }()
	for _, values := range []State{passwords, keys} {
		for key, value := range values {
			state[key] = value
		}
	}
	deadline := sample.At.Add(e.workload.IterationTimeout.Duration())
	// Setup draws from its own copy of the iteration's source, so the steps
	// build the same requests whether or not setup ran first.
	if failed := e.setup(iterationCtx, scenario, e.Rand(iteration), state, passwords, deadline, &sample, trace); failed {
		return scenario.ID, sample
	}
	for _, values := range []State{passwords, keys} {
		for key, value := range values {
			state[key] = value
		}
	}
	state[StateNow] = sample.At.Format(time.RFC3339)
	started := e.clock.Now()
	var waited time.Duration
	for _, step := range scenario.Steps {
		if trace != nil {
			trace(step.Name)
		}
		result := e.untilNotPending(iterationCtx, scenario, step, rng, state, passwords, phaseStep, deadline)
		waited += result.waited
		sample.record(result)
		if result.outcome != OutcomeOK {
			sample.Outcome, sample.Step, sample.Request, sample.Error = result.outcome, step.Name, result.request, result.err
			sample.Observed, sample.DialFailed = result.observed, result.dial
			sample.Defect, sample.ByteCap = result.defect, result.byteCap
			break
		}
		if endpoint, ok := declaredHTTP(step.Endpoint); ok && endpoint.MarksExistingRecords() {
			sample.ExistingRecords = existingRecordsOutcome(state[ExistingMembersKey], scenario.Marker)
			sample.ExistingMarked = existingRecordsMarked(state[ExistingMembersKey], scenario.Marker)
		}
	}
	// Waiting for a NotYet step to pass is in Convergence, not in latency.
	sample.Latency = max(e.since(started)-waited, time.Nanosecond)
	sample.Done = e.clock.Now().UTC()
	if scenario.SideEffect == SideEffectWriteWithCleanup && sample.Requests > 0 {
		cleanupCtx, cancelCleanup := e.withTimeout(context.WithoutCancel(ctx), e.workload.IterationTimeout.Duration())
		defer cancelCleanup()
		for _, step := range scenario.Cleanup {
			if trace != nil {
				trace(step.Name)
			}
			result := e.step(cleanupCtx, scenario, step, rng, state, passwords, phaseCleanup)
			if result.outcome != OutcomeOK {
				sample.CleanupError = step.Name + ": " + result.err
				break
			}
		}
		if gone, ok := scenario.DeleteReadBack(); ok && sample.CleanupError == "" {
			if trace != nil {
				trace(gone.Name)
			}
			if result := e.step(cleanupCtx, scenario, gone, rng, state, passwords, phaseCleanup); result.outcome != OutcomeOK {
				sample.CleanupError = gone.Name + ": still there after DELETE (SDO reads a deleted path back and expects 404 or 410; if the application answers it otherwise, end Cleanup with your own read): " + result.err
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
func (e *Engine) setup(ctx context.Context, scenario Scenario, rng *rand.Rand, state State, passwords State, deadline time.Time, sample *Sample, trace func(string)) bool {
	if len(scenario.Setup) == 0 {
		return false
	}
	defer e.setups.lock(scenario.ID)()
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
			if trace != nil {
				trace(step.Name)
			}
			result := e.untilNotPending(ctx, scenario, step, rng, saved, passwords, phaseSetup, deadline)
			sample.record(result)
			if result.outcome != OutcomeOK {
				sample.Outcome, sample.Step, sample.Request, sample.Error = result.outcome, step.Name, result.request, result.err
				sample.Observed, sample.DialFailed = result.observed, result.dial
				sample.Defect, sample.ByteCap = result.defect, result.byteCap
				sample.Latency = e.since(started)
				return true
			}
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
	delete(kept, StateNow)
	for _, u := range scenario.Users {
		delete(kept, u.As)
		delete(kept, u.Token)
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
	// answer is the body excerpt of a passing answer, kept only for the step log.
	answer  string
	dial    bool
	defect  string
	byteCap int
	// pending is set when the step's check answered NotYet.
	pending bool
	// converge is set for a step that answered NotYet at least once.
	converge *Convergence
	// waited is the time spent on attempts before the last one and the pauses
	// between them; it is not part of the sample's latency.
	waited time.Duration
}

// record adds a step's request count and last answer to the sample.
func (sample *Sample) record(result stepResult) {
	sample.Requests += result.sent
	if result.converge != nil {
		sample.Convergence = append(sample.Convergence, *result.converge)
	}
	switch {
	case result.exit != nil:
		exit := *result.exit
		sample.Exit, sample.Status = &exit, 0
	case result.status != 0:
		sample.Exit, sample.Status = nil, result.status
	}
}

func (e *Engine) step(ctx context.Context, scenario Scenario, step Step, rng *rand.Rand, state State, passwords State, phase stepPhase) stepResult {
	if e.stepLog == nil {
		return e.sendStep(ctx, scenario, step, rng, state, passwords, phase)
	}
	started := e.clock.Now()
	result := e.sendStep(ctx, scenario, step, rng, state, passwords, phase)
	answer := result.answer
	if answer == "" {
		answer = result.observed
	}
	e.stepLog(StepLog{
		Phase: [...]string{phaseStep: "step", phaseSetup: "setup", phaseCleanup: "cleanup"}[phase], Step: step.Name,
		Request: redactCredentials(result.request, passwords), Status: result.status, Took: e.since(started),
		Answer: redactCredentials(answer, passwords), Error: redactCredentials(result.err, passwords),
	})
	return result
}

func (e *Engine) sendStep(ctx context.Context, scenario Scenario, step Step, rng *rand.Rand, state State, passwords State, phase stepPhase) stepResult {
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
	if err := checkRequest(scenario, target, request, phase, state); err != nil {
		return stepResult{outcome: OutcomeInvalid, request: line, err: err.Error()}
	}
	if phase == phaseSetup && target.Scheme != "ssh" {
		key := fixtureKey(target, request)
		release, err := e.setups.lockFixture(ctx, key)
		if err != nil {
			return stepResult{outcome: OutcomeError, request: line, err: "setup lock: " + err.Error()}
		}
		defer release()
		e.setups.mu.Lock()
		delta, written := e.setups.fixtures[key]
		e.setups.mu.Unlock()
		if written {
			for k, v := range delta {
				state[k] = v
			}
			return stepResult{outcome: OutcomeOK, request: line}
		}
		before := state.clone()
		result := e.send(ctx, scenario, step, target, request, state, passwords, line)
		if result.outcome == OutcomeOK {
			creditOwn(step, request, scenario, state)
			e.setups.mu.Lock()
			if e.setups.fixtures == nil {
				e.setups.fixtures = map[string]State{}
			}
			saved := State{}
			for k, v := range state {
				if before[k] != v {
					saved[k] = v
				}
			}
			delete(saved, StateNow)
			for _, u := range scenario.Users {
				delete(saved, u.As)
				delete(saved, u.Token)
				delete(saved, u.SSHKey)
			}
			e.setups.fixtures[key] = saved
			e.setups.mu.Unlock()
		}
		return result
	}
	result := e.send(ctx, scenario, step, target, request, state, passwords, line)
	if result.outcome == OutcomeOK {
		creditOwn(step, request, scenario, state)
	}
	return result
}

// untilNotPending runs a Setup or Steps step; while its check answers NotYet it
// builds and sends the step again, after a short pause, on a copy of state
// (only the last attempt's changes are kept), until the step passes or fails or
// the iteration deadline passes, which fails it with the last reason.
func (e *Engine) untilNotPending(ctx context.Context, scenario Scenario, step Step, rng *rand.Rand, state State, passwords State, phase stepPhase, deadline time.Time) stepResult {
	started := e.clock.Now()
	attempts, sent := 0, 0
	var result stepResult
	var lastStart time.Time
	for {
		attempts++
		lastStart = e.clock.Now()
		attempt := state.clone()
		result = e.step(ctx, scenario, step, rng, attempt, passwords, phase)
		sent += result.sent
		if !result.pending {
			if result.outcome == OutcomeOK {
				for key, value := range attempt {
					state[key] = value
				}
			}
			break
		}
		remaining := deadline.Sub(e.clock.Now())
		// Send again only when a whole request still fits before the
		// deadline, so running out of time is the step's last answer, never
		// a request cut off by the iteration timeout.
		if remaining <= pendingPause+e.workload.Timeout.Duration() || ctx.Err() != nil {
			break
		}
		select {
		case <-ctx.Done():
		case <-e.clock.After(pendingPause):
		}
		if ctx.Err() != nil || !e.clock.Now().Before(deadline) {
			break
		}
	}
	result.sent = sent
	result.waited = lastStart.Sub(started)
	if attempts > 1 || result.pending {
		result.converge = &Convergence{Step: step.Name, Attempts: attempts, Took: e.since(started), Converged: result.outcome == OutcomeOK}
	}
	if result.pending {
		result.err = fmt.Sprintf("%s (still not there after %d attempts over %s)", result.err, attempts, result.converge.Took.Round(time.Millisecond))
	}
	return result
}

// send runs one built request and judges its answer.
func (e *Engine) send(ctx context.Context, scenario Scenario, step Step, target Target, request Request, state State, passwords State, line string) stepResult {
	if target.Scheme == "ssh" {
		if carriesPassword(request, passwords) {
			return stepResult{outcome: OutcomeInvalid, request: line, err: "an EXEC request may not send a probe-user password or token"}
		}
		return e.execStep(ctx, scenario, step, target, request, state, line)
	}
	base := e.baseURL(target)
	if until, waiting := e.limits.waiting(base, e.clock.Now()); waiting {
		return stepResult{
			outcome: OutcomeInvalid, request: line, defect: DefectRateLimited,
			err: fmt.Sprintf("%s: not sent; the application asked the probe to wait (HTTP 429) until %s: the probe's load, not an application failure", DefectRateLimited, until.UTC().Format(time.RFC3339)),
		}
	}
	requestCtx, cancel := e.withTimeout(ctx, e.workload.Timeout.Duration())
	defer cancel()
	built, err := BuildHTTPRequest(requestCtx, base, request)
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
	body, readErr := io.ReadAll(io.LimitReader(response.Body, MaxResponseBody+1))
	if readErr != nil {
		outcome := OutcomeError
		if isTimeout(requestCtx, readErr) {
			outcome = OutcomeTimeout
		}
		return stepResult{outcome: outcome, status: response.StatusCode, sent: 1, request: line, err: readErr.Error()}
	}
	truncated := len(body) > MaxResponseBody
	if truncated {
		body = body[:MaxResponseBody]
	}
	observed := Response{Status: response.StatusCode, Header: response.Header.Clone(), Body: body, credentials: passwords}
	verdict := step.Endpoint.Check(observed, state)
	if !verdict.OK && truncated && response.StatusCode >= 200 && response.StatusCode < 300 {
		return stepResult{
			outcome: OutcomeInvalid, status: response.StatusCode, sent: 1, request: line, defect: DefectResponseTruncated, byteCap: MaxResponseBody,
			err: fmt.Sprintf("%s: step %q response exceeded the %d-byte cap, so its check could not be judged; probe defect, not an application failure: reduce page size or request less data (for a list, at most 20 records)", DefectResponseTruncated, step.Name, MaxResponseBody),
		}
	}
	if !verdict.OK && response.StatusCode == http.StatusTooManyRequests {
		wait := retryAfter(response.Header, e.clock.Now())
		e.limits.wait(base, e.clock.Now().Add(wait))
		return stepResult{
			outcome: OutcomeInvalid, status: response.StatusCode, sent: 1, request: line, defect: DefectRateLimited,
			observed: observed.excerpt(""),
			err:      fmt.Sprintf("%s: step %q was refused with HTTP 429; the probe waits %s before it sends to this target again: the probe's load, not an application failure (lower the probe's rate if it recurs)", DefectRateLimited, step.Name, wait),
		}
	}
	if !verdict.OK {
		reason := verdict.Reason
		if strings.TrimSpace(reason) == "" {
			reason = fmt.Sprintf("HTTP %d rejected by check", response.StatusCode)
		}
		return stepResult{outcome: OutcomeError, status: response.StatusCode, sent: 1, request: line, err: reason, observed: observed.excerpt(""), pending: verdict.Pending}
	}
	result := stepResult{outcome: OutcomeOK, status: response.StatusCode, sent: 1, request: line}
	if e.stepLog != nil {
		result.answer = observed.excerpt("")
	}
	return result
}

// Run starts iterations at the workload's arrival pattern and rate, from
// iteration first, until ctx ends or, for bounded workloads, the duration
// elapses. At most MaxInFlight iterations run at once; an arrival that finds
// them all busy is skipped. A scheduled sample that begins a failure is
// confirmed by at most two more (UseConfirm). It waits for running iterations and returns the
// next iteration number and the number of skipped arrivals.
func (e *Engine) Run(ctx context.Context, first uint64, emit func(scenario string, sample Sample)) (uint64, int) {
	var deadline <-chan time.Time
	if e.workload.Duration > 0 {
		deadline = e.clock.After(e.workload.Duration.Duration())
	}
	gaps := rand.New(rand.NewPCG(e.workload.Seed, math.MaxUint64))
	slots := make(chan struct{}, MaxInFlight)
	// Confirming samples draw from their own iteration range, so each stays
	// replayable and none repeats a scheduled one.
	confirm := &confirmer{engine: e, slots: slots, emit: e.confirmed, active: map[string]bool{}, next: first + 1<<31}
	var running sync.WaitGroup
	defer running.Wait()
	iteration, skipped := first, 0
	for {
		select {
		case slots <- struct{}{}:
			running.Add(1)
			go func(iteration uint64) {
				defer running.Done()
				scenario, sample := e.Iterate(ctx, iteration)
				if ctx.Err() == nil || sample.Outcome == OutcomeOK {
					emit(scenario, sample)
				}
				<-slots
				if e.startsFailing != nil {
					slo := e.workload.ScenarioSLO(scenario)
					if sample.failed(slo) && e.startsFailing(scenario) && confirm.start(scenario) {
						confirm.run(ctx, scenario, sample, slo)
					}
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
	rate := e.plan.Rate
	if e.warming != nil && e.warming() {
		rate = e.workload.WarmupRate()
	}
	mean := float64(time.Second) / rate
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
