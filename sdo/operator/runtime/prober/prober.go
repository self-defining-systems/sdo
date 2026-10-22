// Package prober runs an application's synthetic-traffic generators in a
// process of their own. The controller starts it as an isolated pod (no
// Kubernetes credentials, egress only to the application namespace, resource
// limits) and reads its observations over HTTP; generator code never runs in
// the controller. The package has no Kubernetes dependency.
package prober

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"sdo.dev/operator/sdk/traffic"
)

// DefaultCapacity bounds the samples kept per scenario.
const DefaultCapacity = 64

// DialTimeout bounds how long a probe waits to establish a connection. It
// is intentionally shorter than the workload's default per-request timeout
// (traffic.DefaultTimeout, 2s) so a blocked dial, for example a
// NetworkPolicy that denies new connections, is classified as its own
// failure (Sample.DialFailed) and counted well before the general request
// deadline would otherwise cancel it with a generic "context deadline
// exceeded".
const DialTimeout = 1 * time.Second

// Config configures a Prober.
type Config struct {
	// Namespace is the application namespace whose Services scenarios call.
	Namespace string
	// ClusterDomain is the cluster DNS suffix, "cluster.local" by default.
	ClusterDomain string
	Catalog       traffic.Catalog
	Workloads     []traffic.Workload
	// Client sends requests; by default a client without keep-alives, so a
	// changed Service selector is seen by the very next request.
	Client traffic.Doer
	// BaseURL overrides Service DNS addressing, for tests.
	BaseURL  func(traffic.Target) string
	Clock    traffic.Clock
	Capacity int
	// DialContext opens the fresh connections of link-probe workloads; by
	// default a plain net.Dialer.
	DialContext func(ctx context.Context, network string, address string) (net.Conn, error)
	// LinkAddress overrides Service DNS addressing of links, for tests.
	LinkAddress func(traffic.Link) string
}

// NewHTTPClient is the prober's default client: no keep-alives, so every
// request dials a fresh connection instead of reusing one that a fault may
// have opened before the fault started (a changed Service selector, or a
// NetworkPolicy that only blocks new connections and lets established ones
// keep working); no redirects; a dial time bounded well below the default
// per-request timeout so a blocked dial fails, and is classified, as its
// own kind of failure rather than racing the request's own deadline.
func NewHTTPClient() *http.Client {
	transport := &http.Transport{
		DisableKeepAlives:     true,
		DialContext:           (&net.Dialer{Timeout: DialTimeout}).DialContext,
		ResponseHeaderTimeout: traffic.MaxTimeout,
		MaxIdleConns:          0,
	}
	return &http.Client{
		Transport:     transport,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

// ServiceBaseURL addresses target in namespace through cluster DNS.
func ServiceBaseURL(target traffic.Target, namespace string, clusterDomain string) string {
	scheme := target.Scheme
	if scheme == "" {
		scheme = "http"
	}
	return fmt.Sprintf("%s://%s.%s.svc.%s:%d", scheme, target.Service, namespace, clusterDomain, target.Port)
}

type scenarioState struct {
	descriptor  traffic.Descriptor
	samples     []traffic.Sample
	qualified   bool
	invalid     int
	lastInvalid string
}

type probe struct {
	engine    *traffic.Engine
	scenarios map[string]*scenarioState
	order     []string
	skipped   int
}

// Prober continuously runs the health-probe workloads and runs verify bursts
// on demand.
type Prober struct {
	config Config
	mu     sync.Mutex
	probes map[string]*probe
	links  map[string]*linkProbe
	bursts map[string]traffic.Workload
	next   uint64
}

// New validates the catalog and workloads and prepares their engines.
func New(config Config) (*Prober, error) {
	if config.Namespace == "" {
		return nil, fmt.Errorf("prober needs the application namespace")
	}
	if config.ClusterDomain == "" {
		config.ClusterDomain = "cluster.local"
	}
	if config.Client == nil {
		config.Client = NewHTTPClient()
	}
	if config.Clock == nil {
		config.Clock = traffic.RealClock
	}
	if config.Capacity <= 0 {
		config.Capacity = DefaultCapacity
	}
	if config.BaseURL == nil {
		namespace, domain := config.Namespace, config.ClusterDomain
		config.BaseURL = func(target traffic.Target) string { return ServiceBaseURL(target, namespace, domain) }
	}
	if config.DialContext == nil {
		config.DialContext = (&net.Dialer{}).DialContext
	}
	if config.LinkAddress == nil {
		namespace, domain := config.Namespace, config.ClusterDomain
		config.LinkAddress = func(link traffic.Link) string { return LinkAddress(link, namespace, domain) }
	}
	// A primitive-only prober has neither scenarios nor workloads. Workload
	// configurations still require the full catalog validation.
	if len(config.Catalog) > 0 || len(config.Workloads) > 0 {
		if err := config.Catalog.Validate(); err != nil {
			return nil, err
		}
	}
	prober := &Prober{config: config, probes: map[string]*probe{}, links: map[string]*linkProbe{}, bursts: map[string]traffic.Workload{}}
	for _, workload := range config.Workloads {
		workload = workload.WithDefaults()
		if err := workload.Validate(); err != nil {
			return nil, err
		}
		if err := workload.ValidateAgainst(config.Catalog); err != nil {
			return nil, err
		}
		if _, exists := prober.probes[workload.Name]; exists {
			return nil, fmt.Errorf("duplicate traffic workload %q", workload.Name)
		}
		if _, exists := prober.bursts[workload.Name]; exists {
			return nil, fmt.Errorf("duplicate traffic workload %q", workload.Name)
		}
		if _, exists := prober.links[workload.Name]; exists {
			return nil, fmt.Errorf("duplicate traffic workload %q", workload.Name)
		}
		if workload.Purpose == traffic.PurposeLinkProbe {
			prober.links[workload.Name] = newLinkProbe(workload)
			continue
		}
		if workload.Purpose != traffic.PurposeHealthProbe {
			prober.bursts[workload.Name] = workload
			continue
		}
		engine, err := traffic.NewEngine(workload, config.Catalog, config.Client, config.BaseURL, config.Clock)
		if err != nil {
			return nil, err
		}
		prober.probes[workload.Name] = newProbe(engine)
	}
	return prober, nil
}

func newProbe(engine *traffic.Engine) *probe {
	state := &probe{engine: engine, scenarios: map[string]*scenarioState{}}
	for _, descriptor := range engine.Descriptors() {
		state.scenarios[descriptor.ID] = &scenarioState{descriptor: descriptor}
		state.order = append(state.order, descriptor.ID)
	}
	return state
}

// Run probes every health-probe workload until ctx ends.
func (p *Prober) Run(ctx context.Context) {
	var running sync.WaitGroup
	for name := range p.probes {
		running.Add(1)
		go func(name string) {
			defer running.Done()
			p.runProbe(ctx, name)
		}(name)
	}
	for _, probe := range p.links {
		running.Add(1)
		go func(probe *linkProbe) {
			defer running.Done()
			p.runLinks(ctx, probe)
		}(probe)
	}
	running.Wait()
}

func (p *Prober) runProbe(ctx context.Context, name string) {
	p.mu.Lock()
	state := p.probes[name]
	first := p.next
	p.mu.Unlock()
	_, skipped := state.engine.Run(ctx, first, func(scenario string, sample traffic.Sample) {
		p.record(name, scenario, sample)
	})
	p.mu.Lock()
	state.skipped += skipped
	p.mu.Unlock()
}

func (p *Prober) record(workload string, scenario string, sample traffic.Sample) {
	p.mu.Lock()
	defer p.mu.Unlock()
	state := p.probes[workload].scenarios[scenario]
	if sample.Outcome == traffic.OutcomeInvalid {
		state.invalid++
		state.lastInvalid = sample.Error
	}
	state.samples = append(state.samples, sample)
	if len(state.samples) > p.config.Capacity {
		state.samples = append([]traffic.Sample(nil), state.samples[len(state.samples)-p.config.Capacity:]...)
	}
	if sample.Outcome == traffic.OutcomeOK {
		state.qualified = true
	}
}

// Windows returns the current observations of every health-probe workload.
func (p *Prober) Windows() map[string]traffic.Window {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.config.Clock.Now().UTC()
	windows := make(map[string]traffic.Window, len(p.probes))
	for name, state := range p.probes {
		scenarios := make([]traffic.ScenarioObservations, 0, len(state.order))
		for _, id := range state.order {
			scenario := state.scenarios[id]
			scenarios = append(scenarios, traffic.ScenarioObservations{
				Scenario: scenario.descriptor, Samples: append([]traffic.Sample(nil), scenario.samples...),
				Qualified: scenario.qualified, Invalid: scenario.invalid, LastInvalid: scenario.lastInvalid,
			})
		}
		windows[name] = traffic.Window{
			Workload: state.engine.Workload(), ObservedAt: now, Scenarios: scenarios, Skipped: state.skipped,
		}
	}
	for name, probe := range p.links {
		windows[name] = p.linkWindowLocked(probe, now)
	}
	return windows
}

// Reset forgets all observations, for example after a maintenance pause.
// Iteration numbers keep increasing so every iteration stays replayable.
func (p *Prober) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, state := range p.probes {
		for _, scenario := range state.scenarios {
			scenario.samples, scenario.qualified, scenario.invalid, scenario.lastInvalid = nil, false, 0, ""
		}
		state.skipped = 0
	}
	// Link samples go, but not which links have ever connected: that is a
	// fact about the topology, and forgetting it would blind the detector to
	// a link that is already broken when the observations restart.
	for _, probe := range p.links {
		for _, state := range probe.links {
			state.samples = nil
		}
	}
}

// BurstRequest asks for a verify burst.
type BurstRequest struct {
	// Workload names a verify-burst or journey workload; empty selects the
	// first verify-burst workload by name.
	Workload string `json:"workload,omitempty"`
	// Scenarios restricts the burst, for example to an incident's failing
	// scenarios; empty runs all of the workload's scenarios.
	Scenarios []string `json:"scenarios,omitempty"`
}

// ScenarioVerdict is one scenario's judgement after a burst.
type ScenarioVerdict struct {
	Scenario string `json:"scenario"`
	Healthy  bool   `json:"healthy"`
	// Qualified is false when the steady probe has observed the scenario
	// but never seen it pass since the controller started: its generator
	// does not fit this application, its health detector cannot fire, and
	// so it does not block the burst either.
	Qualified  bool           `json:"qualified"`
	Evaluated  bool           `json:"evaluated"`
	Samples    int            `json:"samples"`
	ErrorRate  float64        `json:"errorRate"`
	LatencyMS  int64          `json:"latencyMs"`
	Violations []string       `json:"violations,omitempty"`
	Statuses   map[string]int `json:"statusCounts,omitempty"`
	Failures   []string       `json:"recentFailures,omitempty"`
}

// BurstResult is the outcome of a verify burst. Healthy requires every
// qualified scenario to have been evaluated and to meet its SLO.
type BurstResult struct {
	Workload  string            `json:"workload"`
	Healthy   bool              `json:"healthy"`
	StartedAt time.Time         `json:"startedAt"`
	Duration  time.Duration     `json:"durationNs"`
	Verdicts  []ScenarioVerdict `json:"verdicts"`
	Window    traffic.Window    `json:"window"`
}

// Burst runs a bounded verify workload now and judges every scenario from
// the burst's own samples.
func (p *Prober) Burst(ctx context.Context, request BurstRequest) (BurstResult, error) {
	workload, err := p.burstWorkload(request)
	if err != nil {
		return BurstResult{}, err
	}
	engine, err := traffic.NewEngine(workload, p.config.Catalog, p.config.Client, p.config.BaseURL, p.config.Clock)
	if err != nil {
		return BurstResult{}, err
	}
	state := newProbe(engine)
	var mu sync.Mutex
	p.mu.Lock()
	first := p.next
	p.next += 1 << 32
	p.mu.Unlock()
	started := p.config.Clock.Now()
	engine.Run(ctx, first, func(scenario string, sample traffic.Sample) {
		mu.Lock()
		defer mu.Unlock()
		observed := state.scenarios[scenario]
		observed.samples = append(observed.samples, sample)
		if sample.Outcome == traffic.OutcomeInvalid {
			observed.invalid++
			observed.lastInvalid = sample.Error
		}
	})
	if ctx.Err() != nil {
		return BurstResult{}, ctx.Err()
	}
	unqualified := p.unqualifiedScenarios()
	result := BurstResult{Workload: workload.Name, Healthy: true, StartedAt: started.UTC(), Duration: p.config.Clock.Now().Sub(started)}
	window := traffic.Window{Workload: workload, ObservedAt: p.config.Clock.Now().UTC()}
	for _, id := range state.order {
		observed := state.scenarios[id]
		// A burst judges the present: every scenario counts, qualified or not.
		window.Scenarios = append(window.Scenarios, traffic.ScenarioObservations{
			Scenario: observed.descriptor, Samples: observed.samples, Qualified: true,
			Invalid: observed.invalid, LastInvalid: observed.lastInvalid,
		})
		verdict := traffic.Evaluate(id, workload.ScenarioSLO(id), observed.samples, window.ObservedAt)
		failures := make([]string, 0, len(verdict.RecentFailures))
		for _, sample := range verdict.RecentFailures {
			failures = append(failures, fmt.Sprintf("iteration %d step %s %s: %s", sample.Iteration, sample.Step, sample.Request, sample.Error))
		}
		qualified := !unqualified[id]
		result.Verdicts = append(result.Verdicts, ScenarioVerdict{
			Scenario: id, Healthy: verdict.Evaluated && verdict.Healthy, Qualified: qualified, Evaluated: verdict.Evaluated,
			Samples: verdict.Samples, ErrorRate: verdict.ErrorRate, LatencyMS: verdict.Latency.Milliseconds(),
			Violations: verdict.Violations, Statuses: verdict.StatusCounts, Failures: failures,
		})
		if qualified && (!verdict.Evaluated || !verdict.Healthy) {
			result.Healthy = false
		}
	}
	result.Window = window
	return result, nil
}

// unqualifiedScenarios are the scenarios the steady probe has observed but
// never seen pass. A scenario the probe has not observed at all, for example
// before it starts or in a burst-only scenario, stays qualified.
func (p *Prober) unqualifiedScenarios() map[string]bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	observed := map[string]bool{}
	passed := map[string]bool{}
	for _, state := range p.probes {
		for id, scenario := range state.scenarios {
			if len(scenario.samples) > 0 {
				observed[id] = true
			}
			if scenario.qualified {
				passed[id] = true
			}
		}
	}
	unqualified := map[string]bool{}
	for id := range observed {
		if !passed[id] {
			unqualified[id] = true
		}
	}
	return unqualified
}

func (p *Prober) burstWorkload(request BurstRequest) (traffic.Workload, error) {
	name := request.Workload
	if name == "" {
		names := make([]string, 0, len(p.bursts))
		for candidate, workload := range p.bursts {
			if workload.Purpose == traffic.PurposeVerifyBurst {
				names = append(names, candidate)
			}
		}
		sort.Strings(names)
		if len(names) == 0 {
			return traffic.Workload{}, fmt.Errorf("no verify-burst workload is defined")
		}
		name = names[0]
	}
	workload, ok := p.bursts[name]
	if !ok {
		return traffic.Workload{}, fmt.Errorf("no verify-burst or journey workload named %q", name)
	}
	if len(request.Scenarios) == 0 {
		return workload, nil
	}
	wanted := make(map[string]bool, len(request.Scenarios))
	for _, id := range request.Scenarios {
		wanted[id] = true
	}
	restricted := workload
	restricted.Scenarios = nil
	for _, scenario := range workload.Scenarios {
		if wanted[scenario.ID] {
			restricted.Scenarios = append(restricted.Scenarios, scenario)
			delete(wanted, scenario.ID)
		}
	}
	if len(wanted) > 0 {
		missing := make([]string, 0, len(wanted))
		for id := range wanted {
			missing = append(missing, id)
		}
		sort.Strings(missing)
		return traffic.Workload{}, fmt.Errorf("workload %s does not run scenarios %v", name, missing)
	}
	// Keep the burst's total rate while it covers fewer scenarios, and give
	// the restricted burst its own replayable seed.
	restricted.Seed = workload.Seed ^ hashScenarios(request.Scenarios)
	return restricted, nil
}

func hashScenarios(ids []string) uint64 {
	sorted := append([]string(nil), ids...)
	sort.Strings(sorted)
	var hash uint64 = 1469598103934665603
	for _, id := range sorted {
		for _, character := range []byte(id + "\x00") {
			hash ^= uint64(character)
			hash *= 1099511628211
		}
	}
	return hash
}

// Summary is a compact per-scenario view for logs.
func (p *Prober) Summary() map[string]map[string]string {
	summary := map[string]map[string]string{}
	for name, window := range p.Windows() {
		scenarios := map[string]string{}
		for _, observed := range window.Scenarios {
			ok := 0
			for _, sample := range observed.Samples {
				if sample.Outcome == traffic.OutcomeOK {
					ok++
				}
			}
			scenarios[observed.Scenario.ID] = strconv.Itoa(ok) + "/" + strconv.Itoa(len(observed.Samples)) + " ok"
		}
		summary[name] = scenarios
	}
	return summary
}
