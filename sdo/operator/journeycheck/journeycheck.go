// Package journeycheck judges an application's user-journey probes against the
// healthy application before SDO relies on them: the request budget, each
// scenario passing twice and alongside another prober, its cleanup, and
// whether it notices when an answer it receives is wrong. A wrong answer is
// made from the application's own response on its way back, so nothing in the
// cluster is broken. Transport, identities and time are injected.
package journeycheck

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"sdo.dev/operator/sdk/traffic"
)

// Items, in the order they are reported: an earlier one is fixed first,
// because a later one is not meaningful while it is red.
const (
	ItemBudget     = "budget"
	ItemScheduled  = "scheduled"
	ItemPasses     = "passes"
	ItemCleanup    = "cleanup"
	ItemConcurrent = "concurrent"
	ItemLatency    = "latency-bound"
	ItemAltered    = "altered-answers"
)

var itemOrder = map[string]int{ItemBudget: 0, ItemScheduled: 1, ItemPasses: 2, ItemCleanup: 3, ItemConcurrent: 4, ItemLatency: 5, ItemAltered: 6}

// Result statuses.
const (
	Green = "green"
	Red   = "red"
)

// Result is one item's verdict on one scenario (or on the whole set).
type Result struct {
	Item    string `json:"item"`
	Subject string `json:"subject"`
	Status  string `json:"status"`
	Detail  string `json:"detail"`
}

// Report is one run, sorted by item then subject.
type Report struct {
	Results []Result `json:"results"`
	// PlannedRate and Cap are requests per second of all probes together.
	PlannedRate float64 `json:"planned_rate"`
	Cap         float64 `json:"cap"`
}

// Reds is the number of red results.
func (r Report) Reds() int {
	count := 0
	for _, result := range r.Results {
		if result.Status == Red {
			count++
		}
	}
	return count
}

// Config is one run.
type Config struct {
	Catalog   traffic.Catalog
	Workloads []traffic.Workload
	// Only keeps the scenarios whose ID contains one of these; empty keeps all.
	Only []string
	// Alter runs the altered-answers item.
	Alter     bool
	Client    traffic.Doer
	BaseURL   func(traffic.Target) string
	Passwords traffic.Passwords
	Exec      traffic.Execer
	Clock     traffic.Clock
	// Wait pauses between attempts while a probe user is provisioned.
	Wait func(ctx context.Context, d time.Duration) error
	// Progress, when set, receives a line per finished scenario.
	Progress func(string)
}

// maxIdentityWait bounds the wait for one probe user.
const (
	maxIdentityWait = 5 * time.Minute
	identityPoll    = 2 * time.Second
	parallel        = 8
	// slowInterval is the sample interval above which a workload should be
	// sampled faster when the budget allows.
	slowInterval = 30 * time.Second
)

// Run judges the scenarios the workloads schedule.
func Run(ctx context.Context, c Config) Report {
	report := Report{Cap: traffic.ProbeRateCap}
	load := budget(c, &report)
	report.PlannedRate = load
	scheduled := map[string]traffic.Workload{}
	for _, w := range c.Workloads {
		if w.Purpose != traffic.PurposeHealthProbe {
			continue
		}
		for _, s := range w.Scenarios {
			if prior, ok := scheduled[s.ID]; !ok || w.EffectiveSampleInterval() < prior.EffectiveSampleInterval() {
				scheduled[s.ID] = w
			}
		}
	}
	results := make([][]Result, len(c.Catalog))
	slots := make(chan struct{}, parallel)
	shared := &sharedSetup{}
	var running sync.WaitGroup
	for i, scenario := range c.Catalog {
		if !selected(scenario.ID, c.Only) {
			continue
		}
		workload, ok := scheduled[scenario.ID]
		if !ok {
			results[i] = []Result{{ItemScheduled, scenario.ID, Red, "no health-probe workload schedules it, so SDO never runs it: add it to a workload or remove it"}}
			continue
		}
		running.Add(1)
		go func() {
			defer running.Done()
			slots <- struct{}{}
			defer func() { <-slots }()
			j := judge{c: c, scenario: scenario, workload: workload, shared: shared}
			results[i] = j.run(ctx)
			if c.Progress != nil {
				c.Progress(scenario.ID + " judged")
			}
		}()
	}
	running.Wait()
	for _, r := range results {
		report.Results = append(report.Results, r...)
	}
	sort.SliceStable(report.Results, func(i, j int) bool {
		a, b := report.Results[i], report.Results[j]
		if a.Item != b.Item {
			return itemOrder[a.Item] < itemOrder[b.Item]
		}
		return a.Subject < b.Subject
	})
	return report
}

func selected(id string, only []string) bool {
	for _, o := range only {
		if strings.Contains(id, o) {
			return true
		}
	}
	return len(only) == 0
}

// budget reports what the schedule sends and what is left under the cap, so
// the author knows the room before writing the next probe.
func budget(c Config, report *Report) float64 {
	requests := map[string]int{}
	for _, s := range c.Catalog {
		requests[s.ID] = len(s.Steps)
	}
	total, longestLoad := 0.0, 0.0
	var longest traffic.Workload
	var parts []string
	for _, w := range c.Workloads {
		if w.Purpose != traffic.PurposeHealthProbe || len(w.Scenarios) == 0 {
			continue
		}
		load := traffic.MeasureProbeLoad([]traffic.Workload{w}, requests).RequestsPerSecond
		total += load
		if interval := w.EffectiveSampleInterval(); load > 0 && interval > slowInterval && interval > longest.EffectiveSampleInterval() {
			longest, longestLoad = w, load
		}
		parts = append(parts, fmt.Sprintf("%s %.2f (%d scenarios, %.2f iterations/s)", w.Name, load, len(w.Scenarios), w.SamplingPlan().Rate))
	}
	status, detail := Green, fmt.Sprintf("%.2f of %.1f requests/s planned, %.2f left: %s", total, traffic.ProbeRateCap, traffic.ProbeRateCap-total, strings.Join(parts, ", "))
	if total > traffic.ProbeRateCap {
		status = Red
		detail += "; merge steps or scenarios, lower ratePerSecond (unset is 4 iterations/s), or lengthen sampleInterval of the journeys that matter least"
	}
	// Unspent budget is slower notice: the slowest workload can take it. One
	// tenth stays free for confirming samples and cleanup.
	if left := 0.9*traffic.ProbeRateCap - total; len(c.Only) == 0 && longest.Name != "" && left > 0.1*traffic.ProbeRateCap {
		status = Red
		faster := time.Duration(float64(longest.EffectiveSampleInterval()) * longestLoad / (longestLoad + left))
		detail += fmt.Sprintf("; unspent: sample %s about every %s instead of %s (sampleInterval and ratePerSecond), or run its core journeys from a fast workload, so a broken journey is noticed sooner", longest.Name, max(faster, traffic.MinSampleInterval).Round(5*time.Second), longest.EffectiveSampleInterval())
	}
	report.Results = append(report.Results, Result{ItemBudget, "all", status, detail})
	return total
}

// sharedSetup makes one prober's engines share completed setup, as SDO's
// prober does.
type sharedSetup struct {
	mu    sync.Mutex
	first *traffic.Engine
}

type judge struct {
	c        Config
	scenario traffic.Scenario
	workload traffic.Workload
	shared   *sharedSetup
	next     uint64
}

func (j *judge) engine(client traffic.Doer, shared *sharedSetup, iterationTimeout time.Duration) (*traffic.Engine, error) {
	w := traffic.Workload{APIVersion: traffic.APIVersion, Kind: traffic.WorkloadKind, Name: "journey-check", Purpose: traffic.PurposeHealthProbe,
		Timeout: j.workload.Timeout, IterationTimeout: j.workload.IterationTimeout, Scenarios: []traffic.WorkloadScenario{{ID: j.scenario.ID}}}
	if iterationTimeout > 0 {
		w.IterationTimeout = traffic.Duration(iterationTimeout)
	}
	e, err := traffic.NewEngine(w, j.c.Catalog, client, j.c.BaseURL, j.c.Clock)
	if err != nil {
		return nil, err
	}
	e.UsePasswords(j.c.Passwords)
	e.UseExec(j.c.Exec)
	shared.mu.Lock()
	defer shared.mu.Unlock()
	if shared.first == nil {
		shared.first = e
	} else {
		e.ShareSetup(shared.first)
	}
	return e, nil
}

func (j *judge) iterate(ctx context.Context, e *traffic.Engine) traffic.Sample {
	waited := time.Duration(0)
	for {
		j.next++
		_, sample := e.Iterate(ctx, j.next)
		if sample.IdentityWait == 0 || waited >= maxIdentityWait || j.c.Wait(ctx, identityPoll) != nil {
			return sample
		}
		waited += identityPoll
	}
}

func (j *judge) run(ctx context.Context) []Result {
	id := j.scenario.ID
	one := func(item, status, detail string) Result { return Result{item, id, status, detail} }
	e, err := j.engine(j.c.Client, j.shared, 0)
	if err != nil {
		return []Result{one(ItemPasses, Red, err.Error())}
	}
	var slowest time.Duration
	var slow traffic.Sample
	requests, cleanup := 0, ""
	for round := range 3 {
		s := j.iterate(ctx, e)
		if s.Outcome != traffic.OutcomeOK {
			return []Result{one(ItemPasses, Red, fmt.Sprintf("run %d on the healthy application: %s", round+1, describe(s)))}
		}
		if s.Latency > slowest {
			slowest, slow = s.Latency, s
		}
		requests = max(requests, s.Requests)
		if s.CleanupError != "" {
			cleanup = s.CleanupError
		}
	}
	out := []Result{one(ItemPasses, Green, fmt.Sprintf("3 runs passed, slowest %s, %d requests%s", slowest.Round(time.Millisecond), requests, pauses(slow)))}
	if cleanup != "" {
		out = append(out, one(ItemCleanup, Red, "cleanup failed, so the probe leaves data behind: "+cleanup))
	}
	out = append(out, j.concurrent(ctx))
	bound := j.workload.ScenarioSLO(id).MaxLatency.Duration()
	if want := max(3*slowest, time.Second); bound < 2*slowest {
		out = append(out, one(ItemLatency, Red, fmt.Sprintf("maxLatency %s is under twice the slowest healthy run %s and would page on an ordinary slow moment: set it to about %s", bound, slowest.Round(time.Millisecond), want.Round(100*time.Millisecond))))
	}
	interval := j.workload.ScenarioArrival(id)
	out = append(out, one(ItemScheduled, Green, fmt.Sprintf("workload %s samples it every %s, so a full failure is confirmed in about %s", j.workload.Name, interval.Round(time.Second), (interval+slowest+5*time.Second).Round(time.Second))))
	if j.c.Alter {
		out = append(out, j.altered(ctx, requests, slowest))
	}
	return out
}

// concurrent runs the scenario from two probers at once: SDO starts a second
// one beside the first during a restart or an evaluation, and neither may fail
// the other.
func (j *judge) concurrent(ctx context.Context) Result {
	samples := make([]traffic.Sample, 2)
	var running sync.WaitGroup
	for i := range samples {
		other := *j
		other.next += uint64(1000 * (i + 1))
		shared := j.shared
		if i == 1 {
			shared = &sharedSetup{}
		}
		e, err := other.engine(j.c.Client, shared, 0)
		if err != nil {
			return Result{ItemConcurrent, j.scenario.ID, Red, err.Error()}
		}
		running.Add(1)
		go func() {
			defer running.Done()
			for range 2 {
				if samples[i] = other.iterate(ctx, e); samples[i].Outcome != traffic.OutcomeOK {
					return
				}
			}
		}()
	}
	running.Wait()
	for _, s := range samples {
		if s.Outcome != traffic.OutcomeOK {
			return Result{ItemConcurrent, j.scenario.ID, Red, "fails while a second prober runs it (shared object, constant name or exact list): " + describe(s)}
		}
	}
	return Result{ItemConcurrent, j.scenario.ID, Green, "two probers at once passed"}
}

// altered runs the scenario once per request and kind of wrong answer. A
// request none of whose wrong answers is noticed checks nothing: red.
func (j *judge) altered(ctx context.Context, requests int, slowest time.Duration) Result {
	id := j.scenario.ID
	bound := min(max(3*slowest, 5*time.Second), j.workload.IterationTimeout.Duration())
	a := &alterer{client: j.c.Client, marker: j.scenario.Marker, previous: map[int][]byte{}, write: map[int]bool{}}
	e, err := j.engine(a, j.shared, bound)
	if err != nil {
		return Result{ItemAltered, id, Red, err.Error()}
	}
	// Two clean runs: the second gives every "earlier answer" to replay.
	for range 2 {
		a.start(-1, "")
		if s := j.iterate(ctx, e); s.Outcome != traffic.OutcomeOK {
			return Result{ItemAltered, id, Red, "a clean run failed: " + describe(s)}
		}
	}
	tried, caught := 0, 0
	var blind, missed []string
	// Cleanup answers follow the steps and judge nothing.
	for step := range min(requests, len(j.scenario.Steps)) {
		stepTried, stepCaught, line := 0, 0, ""
		var lost []string
		for _, kind := range alterations {
			if a.write[step] && kind != "empty" {
				continue // what a write answers is judged by the read after it
			}
			a.start(step, kind)
			s := j.iterate(ctx, e)
			if !a.applied || s.Outcome == traffic.OutcomeInvalid || s.Requests <= step {
				continue
			}
			line = a.line
			stepTried++
			if s.Outcome == traffic.OutcomeOK {
				lost = append(lost, kind)
			} else {
				stepCaught++
			}
		}
		tried, caught = tried+stepTried, caught+stepCaught
		content := contains(lost, "stale") || contains(lost, "text")
		switch {
		case stepTried > 0 && stepCaught == 0:
			if !a.write[step] {
				blind = append(blind, line+" (no wrong answer noticed)")
			}
		case content:
			missed = append(missed, fmt.Sprintf("%s (%s)", line, strings.Join(lost, ", ")))
		}
	}
	detail := fmt.Sprintf("noticed %d of %d wrong answers", caught, tried)
	if len(missed) > 0 {
		detail += "; unnoticed wrong content, check it unless a later step reads it back: " + strings.Join(missed, "; ")
	}
	if len(blind) > 0 || caught == 0 {
		return Result{ItemAltered, id, Red, detail + "; checks nothing: " + strings.Join(blind, "; ")}
	}
	return Result{ItemAltered, id, Green, detail}
}

func contains(values []string, want string) bool {
	for _, v := range values {
		if v == want {
			return true
		}
	}
	return false
}

func describe(s traffic.Sample) string {
	text := fmt.Sprintf("%s at step %s: %s: %s", s.Outcome, s.Step, s.Request, s.Error)
	if s.Observed != "" && !strings.Contains(s.Error, s.Observed) {
		text += "; answered: " + s.Observed
	}
	return text
}

func pauses(s traffic.Sample) string {
	text := ""
	for _, c := range s.Convergence {
		text += fmt.Sprintf(", %s waited %s", c.Step, c.Took.Round(time.Millisecond))
	}
	return text
}

// Text is the report for people and agents: the red lines, then the rest.
func (r Report) Text() string {
	var b strings.Builder
	for _, status := range []string{Red, Green} {
		for _, x := range r.Results {
			if x.Status == status {
				fmt.Fprintf(&b, "%s %s %s: %s\n", x.Status, x.Item, x.Subject, x.Detail)
			}
		}
	}
	fmt.Fprintf(&b, "%d red; %.2f of %.1f requests/s planned\n", r.Reds(), r.PlannedRate, r.Cap)
	return b.String()
}
