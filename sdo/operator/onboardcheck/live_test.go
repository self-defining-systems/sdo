package onboardcheck

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"sdo.dev/operator/memory/appmap"
)

// world is a deterministic fake application: a clock that only moves when
// something waits or evaluates, the set of broken components, and the raises
// the detectors would produce for them.
type world struct {
	mu      sync.Mutex
	now     time.Time
	evalDur time.Duration
	broken  map[string]bool
	// healthyRaise is raised on every evaluation, broken or not.
	healthyRaise []Raise
	// raiseFor maps a broken component to what the detectors raise.
	raiseFor map[string][]Raise
	// ignoredBy lists components whose breakage no detector notices.
	log        []string
	evalErr    error
	restoreErr map[string]error
	breakErr   map[string]error
	stuck      map[string]bool // restore succeeds but the component stays broken
	unavail    bool
	pending    []string
	evals      int
	late       map[string]lateRaise
	sim        []detectorSim
	simStart   time.Time
	brokenAt   map[string]time.Time
}

// lateRaise is a raise that appears only some time after its component broke.
type lateRaise struct {
	after time.Duration
	raise []Raise
}

func newWorld() *world {
	return &world{now: time.Unix(1000, 0), evalDur: 2 * time.Second, broken: map[string]bool{}, raiseFor: map[string][]Raise{}, restoreErr: map[string]error{}, breakErr: map[string]error{}, stuck: map[string]bool{}}
}

func (w *world) Now() time.Time {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.now
}
func (w *world) Wait(_ context.Context, d time.Duration) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.now = w.now.Add(d)
	return nil
}

func (w *world) Evaluate(context.Context) (Verdict, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.evals++
	w.now = w.now.Add(w.evalDur)
	if w.evalErr != nil {
		return Verdict{}, w.evalErr
	}
	if w.unavail {
		return Verdict{Status: "unavailable", Unavailable: []string{"traffic prober unavailable"}}, nil
	}
	if len(w.pending) > 0 {
		return Verdict{Status: "pending", Pending: w.pending}, nil
	}
	raised := append([]Raise(nil), w.healthyRaise...)
	for c := range w.broken {
		raised = append(raised, w.raiseFor[c]...)
		if l, ok := w.late[c]; ok && w.now.Sub(w.brokenAt[c]) >= l.after {
			raised = append(raised, l.raise...)
		}
	}
	if len(raised) == 0 {
		return Verdict{Status: "clear", Progress: w.progress()}, nil
	}
	return Verdict{Status: "firing", Raised: raised}, nil
}

func (w *world) Mode() string { return "scale-to-zero" }

func (w *world) Break(_ context.Context, c appmap.Component) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.breakErr[c.Name]; err != nil {
		return err
	}
	w.log = append(w.log, "break "+c.Name)
	w.broken[c.Name] = true
	if w.brokenAt == nil {
		w.brokenAt = map[string]time.Time{}
	}
	w.brokenAt[c.Name] = w.now
	return nil
}

func (w *world) Restore(_ context.Context, c appmap.Component) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.log = append(w.log, "restore "+c.Name)
	if err := w.restoreErr[c.Name]; err != nil {
		return err
	}
	if !w.stuck[c.Name] {
		delete(w.broken, c.Name)
	}
	return nil
}

const liveMap = `version: 1
components:
  - {name: web, workload: web, services: [web]}
  - {name: api, workload: api, services: [api]}
  - {name: db, workload: db, services: [db], holds_data: true, data: {engine: postgres, role: authoritative}}
  - {name: worker, workload: worker, services: []}
edges:
  - {from: web, to: api, evidence: [config], source: x}
  - {from: api, to: db, evidence: [config], source: x}
  - {from: api, to: worker, evidence: [config], source: x}
entries: []
operations:
  - id: list-items
    entry: {service: web, port: 80, method: GET, path: /items}
    path: [web, api, db]
  - id: send-mail
    writes: true
    entry: {service: web, port: 80, method: POST, path: /mail}
    path: [web, api, worker]
probes:
  - {scenario: browse, kind: call, operations: [list-items]}
`

func liveConfig(t *testing.T, w *world, quiet, brk bool) Config {
	t.Helper()
	m, err := appmap.Parse(liveMap)
	if err != nil {
		t.Fatal(err)
	}
	p := &fakeProm{}
	cfg := Config{Map: m, Commit: "abc", Prom: p, Sender: fakeSender{prom: p, emit: perRoute}, Requests: 1, Poll: time.Second, Wait: w.Wait, Now: w.Now}
	if quiet {
		cfg.Quiet = &QuietConfig{Eval: w, Window: 10 * time.Minute, Interval: time.Minute}
	}
	if brk {
		cfg.Break = &BreakConfig{Eval: w, Breaker: w, Bound: 60 * time.Second, Interval: 5 * time.Second}
	}
	return cfg
}

func runLiveItems(t *testing.T, w *world, quiet, brk bool) Report {
	t.Helper()
	r, err := Run(context.Background(), liveConfig(t, w, quiet, brk))
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestQuietWhenHealthyGreenSamplesTheWholeWindow(t *testing.T) {
	w := newWorld()
	r := runLiveItems(t, w, true, false)
	if status(r, ItemQuiet, "detectors") != Green {
		t.Fatalf("expected green:\n%s", r.Text())
	}
	if w.evals < 5 {
		t.Fatalf("window not sampled repeatedly: %d evaluations", w.evals)
	}
	if elapsed := w.now.Sub(time.Unix(1000, 0)); elapsed < 10*time.Minute {
		t.Fatalf("returned after %s, before the window ended", elapsed)
	}
	if !strings.Contains(r.Text(), "time "+ItemQuiet) {
		t.Fatalf("duration of the item missing:\n%s", r.Text())
	}
}

func TestQuietWhenHealthyNamesDetectorScenarioAndStep(t *testing.T) {
	w := newWorld()
	w.healthyRaise = []Raise{{DetectorID: "traffic-probe-write", Rule: "slo", Summary: "scenario write-commit failed at step push-event", Evidence: "no push event for the branch"}}
	r := runLiveItems(t, w, true, false)
	if status(r, ItemQuiet, "traffic-probe-write") != Red {
		t.Fatalf("expected red naming the detector:\n%s", r.Text())
	}
	for _, want := range []string{"write-commit", "push-event"} {
		if !strings.Contains(r.Text(), want) {
			t.Fatalf("report does not name %q:\n%s", want, r.Text())
		}
	}
	if w.evals > 1 {
		t.Fatalf("kept sampling after a raise: %d", w.evals)
	}
}

func TestQuietWhenHealthyUnavailableDetectorsAreRed(t *testing.T) {
	w := newWorld()
	w.unavail = true
	r := runLiveItems(t, w, true, false)
	if status(r, ItemQuiet, "detectors") != Red || !strings.Contains(r.Text(), "traffic prober unavailable") {
		t.Fatalf("unavailable must not read as quiet:\n%s", r.Text())
	}
}

func TestQuietWhenHealthyEvaluationErrorIsRed(t *testing.T) {
	w := newWorld()
	w.evalErr = errors.New("host build failed")
	r := runLiveItems(t, w, true, false)
	if status(r, ItemQuiet, "detectors") != Red || !strings.Contains(r.Text(), "host build failed") {
		t.Fatalf("%s", r.Text())
	}
}

func raiseNaming(detector, text string) []Raise {
	return []Raise{{DetectorID: detector, Rule: "r", Summary: text}}
}

func TestFailsUnderBreakGreenWhenATestNamesTheOperationOrItsScenario(t *testing.T) {
	w := newWorld()
	w.raiseFor["web"] = raiseNaming("traffic-probe-call", "scenario browse failed at step list")
	w.raiseFor["api"] = raiseNaming("workload-health", "workload api has no ready pods")
	w.raiseFor["db"] = raiseNaming("store-state", "postgres db is unreachable")
	w.raiseFor["worker"] = raiseNaming("request-metrics", "send-mail failed 12 of 12 requests")
	r := runLiveItems(t, w, false, true)
	if status(r, ItemBreak, "list-items") != Green {
		t.Fatalf("a scenario of list-items fails when web is down:\n%s", r.Text())
	}
	if status(r, ItemBreak, "send-mail") != Green {
		t.Fatalf("a raise names send-mail when worker is down:\n%s", r.Text())
	}
	for _, c := range []string{"web", "api", "db", "worker"} {
		if status(r, ItemBreak, c) != Green {
			t.Fatalf("component %s was noticed:\n%s", c, r.Text())
		}
	}
	if len(w.broken) != 0 {
		t.Fatalf("components left broken: %v", w.broken)
	}
}

func TestFailsUnderBreakRedWhenOnlyGenericDetectorsNameTheComponent(t *testing.T) {
	w := newWorld()
	for _, c := range []string{"web", "api", "db", "worker"} {
		w.raiseFor[c] = raiseNaming("service-endpoints", "Service "+c+" has no ready endpoints")
	}
	r := runLiveItems(t, w, false, true)
	for _, c := range []string{"web", "api", "db", "worker"} {
		if status(r, ItemBreak, c) != Green {
			t.Fatalf("the generic detector noticed component %s:\n%s", c, r.Text())
		}
	}
	for _, op := range []string{"list-items", "send-mail"} {
		if status(r, ItemBreak, op) != Red {
			t.Fatalf("%s: no test of the operation noticed, expected red:\n%s", op, r.Text())
		}
	}
	if !strings.Contains(r.Text(), "only generic detectors noticed") || !strings.Contains(r.Text(), "service-endpoints") {
		t.Fatalf("report must say that only generic detectors noticed:\n%s", r.Text())
	}
}

func TestFailsUnderBreakWaitsForTestsAfterAGenericDetectorHasNoticed(t *testing.T) {
	w := newWorld()
	// The generic detector raises at once; the scenario raise appears only after 20 s broken.
	w.late = map[string]lateRaise{"web": {after: 20 * time.Second, raise: raiseNaming("traffic-probe-call", "scenario browse failed")}}
	w.raiseFor["web"] = raiseNaming("service-endpoints", "Service web has no ready endpoints")
	r := runLiveItems(t, w, false, true)
	if status(r, ItemBreak, "list-items") != Green {
		t.Fatalf("stopped at the first generic hit instead of waiting for the test:\n%s", r.Text())
	}
}

func TestFailsUnderBreakRedWhenNothingNoticesOperation(t *testing.T) {
	w := newWorld()
	// The detectors raise something unrelated to the operations while broken.
	for _, c := range []string{"web", "api", "db", "worker"} {
		w.raiseFor[c] = raiseNaming("node-health", "node pressure")
	}
	r := runLiveItems(t, w, false, true)
	for _, op := range []string{"list-items", "send-mail"} {
		if status(r, ItemBreak, op) != Red {
			t.Fatalf("%s: nothing named it, expected red:\n%s", op, r.Text())
		}
	}
	if !strings.Contains(r.Text(), "scale-to-zero") {
		t.Fatalf("report does not say how the break was made:\n%s", r.Text())
	}
}

func TestFailsUnderBreakCountsOnlyRaisesThatAppearUnderBreak(t *testing.T) {
	w := newWorld()
	// A raise that names the operation but is present while healthy is not a response to the break.
	w.healthyRaise = raiseNaming("traffic-probe-write", "list-items failing")
	r := runLiveItems(t, w, false, true)
	if status(r, ItemBreak, "list-items") != Red {
		t.Fatalf("a standing raise is not noticing the break:\n%s", r.Text())
	}
}

func TestFailsUnderBreakWaitsUpToBoundThenGivesUp(t *testing.T) {
	w := newWorld()
	r := runLiveItems(t, w, false, true)
	if status(r, ItemBreak, "list-items") != Red {
		t.Fatalf("%s", r.Text())
	}
	// 4 components in 3 waves, each wave waits at least the bound.
	if elapsed := w.now.Sub(time.Unix(1000, 0)); elapsed < 3*60*time.Second {
		t.Fatalf("gave up before the bound: %s", elapsed)
	}
}

func TestFailsUnderBreakRestoresWhenEvaluationFails(t *testing.T) {
	w := newWorld()
	calls := 0
	failing := &failAfter{world: w, after: 2, calls: &calls}
	cfg := liveConfig(t, w, false, true)
	cfg.Break.Eval = failing
	r, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(w.broken) != 0 {
		t.Fatalf("left broken after an evaluation failure: %v", w.broken)
	}
	if status(r, ItemBreak, "list-items") != Red {
		t.Fatalf("%s", r.Text())
	}
}

type failAfter struct {
	world *world
	after int
	calls *int
}

func (f *failAfter) Evaluate(ctx context.Context) (Verdict, error) {
	*f.calls++
	if *f.calls > f.after {
		return Verdict{}, fmt.Errorf("prometheus gone")
	}
	return f.world.Evaluate(ctx)
}

func TestFailsUnderBreakStopsWhenRestoreIsUncertain(t *testing.T) {
	w := newWorld()
	w.restoreErr["web"] = errors.New("deployment web has 0 ready replicas after 60s")
	r := runLiveItems(t, w, false, true)
	if status(r, ItemBreak, "web") != Red || !strings.Contains(r.Text(), "restore") {
		t.Fatalf("restore failure is red and named:\n%s", r.Text())
	}
	for _, line := range w.log {
		if line == "break api" || line == "break db" || line == "break worker" {
			t.Fatalf("broke %s after an uncertain restore: %v", line, w.log)
		}
	}
	if r.Clean() {
		t.Fatalf("report must not be clean:\n%s", r.Text())
	}
}

func TestFailsUnderBreakRestoresEveryBrokenComponentInOrder(t *testing.T) {
	w := newWorld()
	runLiveItems(t, w, false, true)
	if len(w.log) != 8 || strings.Join(w.log[:4], ",") != "break web,restore web,break api,restore api" {
		t.Fatalf("shared components go one at a time: %v", w.log)
	}
	// db (list-items) and worker (send-mail) share no operation: one wave, both
	// broken before either is restored.
	last := w.log[4:]
	if !strings.HasPrefix(last[0], "break ") || !strings.HasPrefix(last[1], "break ") || !strings.HasPrefix(last[2], "restore ") || !strings.HasPrefix(last[3], "restore ") {
		t.Fatalf("disjoint components break together: %v", w.log)
	}
}

func TestFailsUnderBreakIsSkippedWhenTheHealthyAppAlreadyRaises(t *testing.T) {
	w := newWorld()
	w.healthyRaise = raiseNaming("traffic-probe-write", "write-commit failed")
	r := runLiveItems(t, w, true, true)
	if status(r, ItemQuiet, "traffic-probe-write") != Red {
		t.Fatalf("%s", r.Text())
	}
	if len(w.log) != 0 {
		t.Fatalf("broke components of an app that is not quiet: %v", w.log)
	}
	if status(r, ItemBreak, "all") != Note {
		t.Fatalf("expected a skip note:\n%s", r.Text())
	}
}

func TestComponentWithoutWorkloadIsANoteNotBroken(t *testing.T) {
	w := newWorld()
	cfg := liveConfig(t, w, false, true)
	for i := range cfg.Map.Components {
		if cfg.Map.Components[i].Name == "worker" {
			cfg.Map.Components[i].Workload = ""
		}
	}
	r, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range w.log {
		if strings.Contains(line, "worker") {
			t.Fatalf("broke a component with no workload: %v", w.log)
		}
	}
	if status(r, ItemBreak, "worker") != Note {
		t.Fatalf("%s", r.Text())
	}
}

func TestDurationsAreReportedPerItem(t *testing.T) {
	w := newWorld()
	r := runLiveItems(t, w, true, true)
	got := map[string]bool{}
	for _, d := range r.Durations {
		got[d.Item] = d.Seconds > 0
	}
	if !got[ItemQuiet] || !got[ItemBreak] {
		t.Fatalf("durations: %+v\n%s", r.Durations, r.Text())
	}
}

func TestFailsUnderBreakStillRunsWhenSomeDetectorsCannotBeEvaluated(t *testing.T) {
	w := newWorld()
	w.unavail = true
	r := runLiveItems(t, w, true, true)
	if status(r, ItemQuiet, "detectors") != Red {
		t.Fatalf("%s", r.Text())
	}
	if len(w.log) == 0 {
		t.Fatalf("the break item must still run: unavailable detectors are not a raise")
	}
}

func TestQuietWhenHealthyIsRedForADetectorThatNeverJudged(t *testing.T) {
	w := newWorld()
	w.pending = []string{"traffic-probe-write: no synthetic scenario has succeeded since observation began: write-commit (last failure: identity unavailable)"}
	r := runLiveItems(t, w, true, false)
	if status(r, ItemQuiet, "traffic-probe-write") != Red || !strings.Contains(r.Text(), "identity unavailable") {
		t.Fatalf("a detector still pending at the end proves nothing about quiet:\n%s", r.Text())
	}
	if elapsed := w.now.Sub(time.Unix(1000, 0)); elapsed < 10*time.Minute {
		t.Fatalf("the window must still be sampled in full: %s", elapsed)
	}
}

// A memory that holds no detector cannot be judged quiet or to fail under a
// break: silence is SDO's gap, said so, never a pass.
func TestLiveItemsAreRedWhenNoDetectorWasEvaluated(t *testing.T) {
	w := newWorld()
	cfg := liveConfig(t, w, true, true)
	cfg.NoDetectors = true
	r, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if status(r, ItemQuiet, "detectors") != Red || !strings.Contains(r.Text(), "SDO: no detectors were evaluated") {
		t.Fatalf("quiet must be red when nothing was evaluated:\n%s", r.Text())
	}
	if status(r, ItemBreak, "all") != Red || w.evals != 0 {
		t.Fatalf("break must not run or pass without detectors (evaluations %d):\n%s", w.evals, r.Text())
	}
}

// detectorSim is a detector judging every interval, with a clearing horizon.
type detectorSim struct {
	id                string
	interval, horizon time.Duration
}

func (w *world) progress() []DetectorProgress {
	if len(w.sim) == 0 {
		return nil
	}
	if w.simStart.IsZero() {
		w.simStart = w.now
	}
	elapsed := w.now.Sub(w.simStart)
	var out []DetectorProgress
	for _, d := range w.sim {
		p := DetectorProgress{DetectorID: d.id, Interval: d.interval, Horizon: d.horizon}
		for at := time.Duration(0); at <= elapsed; at += d.interval {
			p.Evaluations++
			if at >= d.horizon {
				p.PastHorizon++
			}
		}
		out = append(out, p)
	}
	return out
}

func TestParallelBreaksStillNameTheRightOperation(t *testing.T) {
	w := newWorld()
	// Only the worker's break is noticed, by a scenario of send-mail. db is
	// broken in the same wave but nothing names it or list-items.
	w.raiseFor["worker"] = raiseNaming("traffic-probe-send", "send-mail failing")
	r := runLiveItems(t, w, false, true)
	if status(r, ItemBreak, "send-mail") != Green {
		t.Fatalf("send-mail is noticed:\n%s", r.Text())
	}
	if status(r, ItemBreak, "list-items") != Red {
		t.Fatalf("a raise naming send-mail must not credit list-items:\n%s", r.Text())
	}
	waves := BreakWaves(liveMapParsed(t))
	if len(waves) != 3 || len(waves[2]) != 2 {
		t.Fatalf("waves %v", waves)
	}
}

func liveMapParsed(t *testing.T) appmap.Map {
	m, err := appmap.Parse(liveMap)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestQuickModeNeverReportsClean(t *testing.T) {
	w := newWorld()
	w.sim = []detectorSim{{"d1", 30 * time.Second, time.Minute}}
	cfg := liveConfig(t, w, true, false)
	cfg.Quick = true
	r, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if r.Mode != ModeQuick || r.Clean() || status(r, ItemQuiet, "detectors") != Green {
		t.Fatalf("quick run is never clean:\n%s", r.Text())
	}
	if rr := (Report{Mode: ModeQuick}); rr.Clean() || !rr.NoRed() {
		t.Fatal("a quick report with nothing red has no red item but is not clean")
	}
	if rr := (Report{Mode: ModeFull}); !rr.Clean() {
		t.Fatal("a full report with nothing red is clean")
	}
	if !strings.Contains(r.Text(), "mode: quick") {
		t.Fatalf("report names the mode:\n%s", r.Text())
	}
	if elapsed := w.now.Sub(time.Unix(1000, 0)); elapsed > 30*time.Second {
		t.Fatalf("quick smoke should end once each detector judged: %s", elapsed)
	}
	cfg = liveConfig(t, w, true, true)
	cfg.Quick = true
	if _, err := Run(context.Background(), cfg); err == nil {
		t.Fatal("quick mode refuses the break item")
	}
}

func TestQuickModeRedWhenNoDetectorEvaluated(t *testing.T) {
	w := newWorld()
	cfg := liveConfig(t, w, true, false)
	cfg.Quick = true
	cfg.Quiet.Window = time.Minute
	r, _ := Run(context.Background(), cfg)
	if status(r, ItemQuiet, "detectors") != Red {
		t.Fatalf("a smoke that evaluated nothing proves nothing:\n%s", r.Text())
	}
}

func TestQuietWindowCoversEveryDetectorsHorizon(t *testing.T) {
	w := newWorld()
	w.sim = []detectorSim{{"fast", time.Minute, 30 * time.Second}, {"slow", 30 * time.Second, 4 * time.Minute}}
	r := runLiveItems(t, w, true, false)
	if status(r, ItemQuiet, "detectors") != Green {
		t.Fatalf("expected green:\n%s", r.Text())
	}
	elapsed := w.now.Sub(time.Unix(1000, 0))
	// slow: horizon 4m, then 3 judgements 30s apart.
	if need := 4*time.Minute + 2*30*time.Second; elapsed < need {
		t.Fatalf("window ended after %s, before slow judged 3 times past its horizon (%s)", elapsed, need)
	}
	if elapsed >= 10*time.Minute {
		t.Fatalf("window should be derived, not the full cap: %s", elapsed)
	}
	for _, p := range w.progress() {
		if p.PastHorizon < DefaultQuietEvaluations {
			t.Fatalf("%s judged %d times past its horizon", p.DetectorID, p.PastHorizon)
		}
	}
}

func TestQuietWindowCapShorterThanHorizonIsRed(t *testing.T) {
	w := newWorld()
	w.sim = []detectorSim{{"slow", 30 * time.Second, 20 * time.Minute}}
	r := runLiveItems(t, w, true, false)
	if status(r, ItemQuiet, "slow") != Red {
		t.Fatalf("a detector that never judged past its horizon is red:\n%s", r.Text())
	}
}
