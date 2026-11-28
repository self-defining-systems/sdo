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
	ItemSinglePass = "single-pass"
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
	// Plan says the run only read the schedule: nothing was sent or judged.
	Plan bool `json:"plan,omitempty"`
	Once bool `json:"once,omitempty"`
	// Log is, for a single pass, every request the scenario sent and the
	// start of each answer, in order, credentials redacted.
	Log []string `json:"log,omitempty"`
	// Schedules are every scheduled scenario's cost and expected detection,
	// selected by Only or not.
	Schedules []Schedule `json:"schedules,omitempty"`
	// Journeys is the goal's journeys in its order (AddChecklist).
	Journeys []JourneyItem `json:"journeys,omitempty"`
	// Unlisted says the checklist found no journeys in the goal.
	Unlisted bool `json:"unlisted,omitempty"`
	// Memory is the shared memory check's non-blocking result; its blocking
	// errors are red Results of item ItemMemoryCheck.
	Memory *MemoryCheck `json:"memory_check,omitempty"`
}

// ItemMemoryCheck names the red results that are the memory check's errors.
const ItemMemoryCheck = "memory-check"

// MemoryCheck is what the memory check beside the probes left out of Results:
// why it did not run (Skipped), or its notes, which never reject.
type MemoryCheck struct {
	Skipped string   `json:"skipped,omitempty"`
	Notes   []string `json:"notes,omitempty"`
}

// memoryText is the memory check's lines after its red results: why it was
// skipped, or its notes counted (listed in full when verbose).
// maxNoteLine bounds one memory-check note in a verbose journeys report.
const maxNoteLine = 160

func (r Report) memoryText(verbose bool) string {
	if r.Memory == nil {
		return ""
	}
	var b strings.Builder
	if r.Memory.Skipped != "" {
		fmt.Fprintf(&b, "memory check skipped: %s\n", r.Memory.Skipped)
	}
	switch {
	case len(r.Memory.Notes) == 0:
	case verbose:
		// Notes never block; one short line each keeps a verbose run readable.
		for _, note := range r.Memory.Notes {
			if line, _, cut := strings.Cut(note, "\n"); cut || len(note) > maxNoteLine {
				note = strings.TrimSpace(line[:min(len(line), maxNoteLine)]) + " …"
			}
			fmt.Fprintf(&b, "note %s: %s\n", ItemMemoryCheck, note)
		}
		fmt.Fprintf(&b, "memory check: notes cut to one line; sdo-operator memory check prints them whole\n")
	default:
		noun := "notes"
		if len(r.Memory.Notes) == 1 {
			noun = "note"
		}
		fmt.Fprintf(&b, "memory check: %d %s (--verbose lists them)\n", len(r.Memory.Notes), noun)
	}
	return b.String()
}

// Schedule is how the health probes sample one scenario: what it costs and how
// soon they notice it failing completely (traffic.Workload.ExpectedDetection
// of the workload that notices it soonest, with its slowest healthy run when
// it was judged). Times are seconds.
type Schedule struct {
	Scenario  string   `json:"scenario"`
	Workloads []string `json:"workloads"`
	// Requests is one iteration's requests; Cost is requests per second over
	// every workload that schedules it.
	Requests int     `json:"requests"`
	Cost     float64 `json:"requests_per_second"`
	Every    float64 `json:"every_s"`
	Typical  float64 `json:"typical_s"`
	Worst    float64 `json:"worst_s"`
	// KnownBroken says a workload marks it known broken: it notices nothing.
	KnownBroken bool `json:"known_broken,omitempty"`
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
	// Only selects exact IDs or, for unmatched filters, ID substrings. Empty keeps all.
	Only []string
	// Alter runs the altered-answers item.
	Alter bool
	// Once runs one iteration of the exact scenario named by Only.
	Once bool
	// DetectionTarget is the human objective's bound, when specified.
	DetectionTarget time.Duration
	// Plan only reads the schedule: budget and schedules, no request sent.
	Plan      bool
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
	if c.Once {
		return singlePass(ctx, c)
	}
	report := Report{Cap: traffic.ProbeRateCap, Plan: c.Plan}
	load := budget(c, &report)
	report.PlannedRate = load
	scheduled := scenarioWorkloads(c.Workloads)
	results := make([][]Result, len(c.Catalog))
	slowest := make([]time.Duration, len(c.Catalog))
	slots := make(chan struct{}, parallel)
	shared := &sharedSetup{}
	ids := make(map[string]bool, len(c.Catalog))
	for _, scenario := range c.Catalog {
		ids[scenario.ID] = true
	}
	var running sync.WaitGroup
	for i, scenario := range c.Catalog {
		if !selected(scenario.ID, c.Only, ids) {
			continue
		}
		if gap, ok := cleanupResult(scenario); ok {
			results[i] = append(results[i], gap)
		}
		if c.Plan {
			continue
		}
		workload, ok := scheduled[scenario.ID]
		if !ok {
			results[i] = append(results[i], Result{ItemScheduled, scenario.ID, Red, "no health-probe workload schedules it, so SDO never runs it: add it to a workload or remove it"})
			continue
		}
		running.Add(1)
		go func() {
			defer running.Done()
			slots <- struct{}{}
			defer func() { <-slots }()
			j := judge{c: c, scenario: scenario, workload: workload, shared: shared}
			results[i] = append(results[i], j.run(ctx)...)
			slowest[i] = j.slowest
			if c.Progress != nil {
				c.Progress(scenario.ID + " judged")
			}
		}()
	}
	running.Wait()
	for _, r := range results {
		report.Results = append(report.Results, r...)
	}
	for i, scenario := range c.Catalog {
		if s, ok := schedule(c.Workloads, scenario, slowest[i]); ok {
			report.Schedules = append(report.Schedules, s)
		}
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

func scenarioWorkloads(workloads []traffic.Workload) map[string]traffic.Workload {
	scheduled := map[string]traffic.Workload{}
	for _, w := range workloads {
		if w.Purpose != traffic.PurposeHealthProbe {
			continue
		}
		for _, s := range w.Scenarios {
			if prior, ok := scheduled[s.ID]; !ok || w.EffectiveSampleInterval() < prior.EffectiveSampleInterval() {
				scheduled[s.ID] = w
			}
		}
	}
	return scheduled
}

func singlePass(ctx context.Context, c Config) Report {
	report := Report{Once: true}
	if len(c.Only) != 1 {
		report.Results = []Result{{ItemSinglePass, "all", Red, "select one exact scenario ID"}}
		return report
	}
	id := c.Only[0]
	result := Result{Item: ItemSinglePass, Subject: id, Status: Red}
	scenario, found := c.Catalog.Scenario(id)
	if !found {
		result.Detail = "no scenario with that exact ID"
	} else {
		// Use the same workload selection as the full runner for timeouts.
		workload, found := scenarioWorkloads(c.Workloads)[id]
		if !found {
			// Not scheduled yet: try it with the default timeouts.
			workload = traffic.Workload{APIVersion: traffic.APIVersion, Kind: traffic.WorkloadKind, Name: "journey-check",
				Purpose: traffic.PurposeHealthProbe, Scenarios: []traffic.WorkloadScenario{{ID: id}}}.WithDefaults()
		}
		j := judge{c: c, scenario: scenario, workload: workload, shared: &sharedSetup{}}
		result, report.Log = j.singlePass(ctx)
	}
	report.Results = []Result{result}
	return report
}

func selected(id string, only []string, ids map[string]bool) bool {
	for _, o := range only {
		if id == o || (!ids[o] && strings.Contains(id, o)) {
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
		if interval := w.EffectiveSampleInterval(); load > 0 && interval > slowInterval && interval > longest.EffectiveSampleInterval() && !meetsTarget(w, c.DetectionTarget) {
			longest, longestLoad = w, load
		}
		parts = append(parts, fmt.Sprintf("%s %.2f (%d scenarios, %.2f iterations/s)", w.Name, load, len(w.Scenarios), w.SamplingPlan().Rate))
	}
	status, detail := Green, fmt.Sprintf("%.2f of %.1f requests/s planned, %.2f left: %s", total, traffic.ProbeRateCap, traffic.ProbeRateCap-total, strings.Join(parts, ", "))
	if total > traffic.ProbeRateCap {
		status = Red
		detail += "; merge steps or scenarios, lower ratePerSecond if it exceeds the sampling floor, or lengthen sampleInterval for less used journeys"
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

func meetsTarget(w traffic.Workload, target time.Duration) bool {
	if target == 0 {
		return false
	}
	for _, scenario := range w.Scenarios {
		if w.ExpectedDetection(scenario.ID, 0).Worst > target {
			return false
		}
	}
	return true
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
	// slowest is the slowest of the healthy runs, once they all passed.
	slowest time.Duration
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

func (j *judge) singlePass(ctx context.Context) (Result, []string) {
	result := Result{Item: ItemSinglePass, Subject: j.scenario.ID, Status: Red}
	e, err := j.engine(j.c.Client, j.shared, 0)
	if err != nil {
		result.Detail = err.Error()
		return result, nil
	}
	var log []string
	e.UseStepLog(func(entry traffic.StepLog) { log = append(log, entry.String()) })
	sample := j.iterate(ctx, e)
	if sample.Outcome == traffic.OutcomeOK {
		result.Status = Green
		result.Detail = fmt.Sprintf("%s, %d requests%s", sample.Latency.Round(time.Millisecond), sample.Requests, pauses(sample))
	} else {
		result.Detail = describe(sample)
	}
	if sample.CleanupError != "" {
		result.Status = Red
		result.Detail += "; cleanup failed: " + sample.CleanupError
	}
	return result, log
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
	j.slowest = slowest
	out := []Result{one(ItemPasses, Green, fmt.Sprintf("3 runs passed, slowest %s, %d requests%s", slowest.Round(time.Millisecond), requests, pauses(slow)))}
	if cleanup != "" {
		out = append(out, one(ItemCleanup, Red, "cleanup failed, so the probe leaves data behind: "+cleanup))
	}
	out = append(out, j.concurrent(ctx))
	bound := j.workload.ScenarioSLO(id).MaxLatency.Duration()
	if want := max(3*slowest, time.Second); bound < 2*slowest {
		out = append(out, one(ItemLatency, Red, fmt.Sprintf("maxLatency %s is under twice the slowest healthy run %s and would page on an ordinary slow moment: set it to about %s", bound, slowest.Round(time.Millisecond), want.Round(100*time.Millisecond))))
	}
	if s, ok := schedule(j.c.Workloads, j.scenario, slowest); ok {
		out = append(out, one(ItemScheduled, Green, fmt.Sprintf("sampled every %s by %s; a full failure is noticed in about %s, at worst %s", seconds(s.Every), strings.Join(s.Workloads, ", "), seconds(s.Typical), seconds(s.Worst))))
	}
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
	// Never below the request timeout: a workload whose iteration bound undercuts its own request timeout is invalid.
	bound := max(min(max(3*slowest, 5*time.Second), j.workload.IterationTimeout.Duration()), j.workload.Timeout.Duration())
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

// VerboseText includes every result in addition to the checklist.
func (r Report) VerboseText() string {
	var b strings.Builder
	b.WriteString(r.checklistText())
	for _, line := range r.Log {
		b.WriteString(line + "\n")
	}
	for _, status := range []string{Red, Green} {
		for _, x := range r.Results {
			if x.Status == status {
				fmt.Fprintf(&b, "%s %s %s: %s\n", x.Status, x.Item, x.Subject, x.Detail)
			}
		}
	}
	b.WriteString(r.memoryText(true))
	if !r.Once {
		b.WriteString(r.summary())
	}
	return b.String()
}

func (r Report) summary() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%d red", r.Reds())
	if len(r.Journeys) > 0 {
		counts := r.JourneyCounts()
		fmt.Fprintf(&b, ", %d journeys missing or failing, %d of %d passed", counts[JourneyMissingMap]+counts[JourneyMissingProbe]+counts[JourneyFailing], counts[JourneyPassed], len(r.Journeys))
	}
	fmt.Fprintf(&b, "; %.2f of %.1f requests/s planned\n", r.PlannedRate, r.Cap)
	if r.Plan {
		b.WriteString("plan only: the schedule was read, no request was sent and no probe judged\n")
	}
	return b.String()
}

// schedule is the scenario's Schedule over the health-probe workloads that
// schedule it; false when none does.
func schedule(workloads []traffic.Workload, scenario traffic.Scenario, slowest time.Duration) (Schedule, bool) {
	s := Schedule{Scenario: scenario.ID, Requests: len(scenario.Steps)}
	var best traffic.DetectionEstimate
	for _, w := range workloads {
		if w.Purpose != traffic.PurposeHealthProbe {
			continue
		}
		every := w.ScenarioArrival(scenario.ID)
		if every <= 0 {
			continue
		}
		s.Workloads = append(s.Workloads, w.Name)
		s.Cost += float64(s.Requests) / every.Seconds()
		if e := w.ExpectedDetection(scenario.ID, slowest); best.Typical == 0 || e.Typical < best.Typical {
			best = e
		}
	}
	if len(s.Workloads) == 0 {
		return Schedule{}, false
	}
	s.KnownBroken = best.Typical == 0
	s.Every, s.Typical, s.Worst = best.Every.Seconds(), best.Typical.Seconds(), best.Worst.Seconds()
	return s, true
}

// seconds renders a time in seconds for the report.
func seconds(s float64) string {
	return (time.Duration(s * float64(time.Second))).Round(time.Second).String()
}
