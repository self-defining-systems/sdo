package onboardcheck

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/memory/appmap"
)

// world is a deterministic fake application: a clock that only moves when
// something waits or evaluates, the set of broken components, and the raises
// the detectors would produce for them.
type world struct {
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
	evals      int
}

func newWorld() *world {
	return &world{now: time.Unix(1000, 0), evalDur: 2 * time.Second, broken: map[string]bool{}, raiseFor: map[string][]Raise{}, restoreErr: map[string]error{}, breakErr: map[string]error{}, stuck: map[string]bool{}}
}

func (w *world) Now() time.Time { return w.now }
func (w *world) Wait(_ context.Context, d time.Duration) error {
	w.now = w.now.Add(d)
	return nil
}

func (w *world) Evaluate(context.Context) (Verdict, error) {
	w.evals++
	w.now = w.now.Add(w.evalDur)
	if w.evalErr != nil {
		return Verdict{}, w.evalErr
	}
	if w.unavail {
		return Verdict{Status: "unavailable", Unavailable: []string{"traffic prober unavailable"}}, nil
	}
	raised := append([]Raise(nil), w.healthyRaise...)
	for c := range w.broken {
		raised = append(raised, w.raiseFor[c]...)
	}
	if len(raised) == 0 {
		return Verdict{Status: "clear"}, nil
	}
	return Verdict{Status: "firing", Raised: raised}, nil
}

func (w *world) Mode() string { return "scale-to-zero" }

func (w *world) Break(_ context.Context, c appmap.Component) error {
	if err := w.breakErr[c.Name]; err != nil {
		return err
	}
	w.log = append(w.log, "break "+c.Name)
	w.broken[c.Name] = true
	return nil
}

func (w *world) Restore(_ context.Context, c appmap.Component) error {
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
probes: []
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

func TestFailsUnderBreakGreenWhenDetectorNamesComponent(t *testing.T) {
	w := newWorld()
	w.raiseFor["web"] = raiseNaming("workload-health", "workload web has no ready pods")
	w.raiseFor["api"] = raiseNaming("workload-health", "workload api has no ready pods")
	w.raiseFor["db"] = raiseNaming("store-state", "postgres db is unreachable")
	w.raiseFor["worker"] = nil // nothing notices the worker
	r := runLiveItems(t, w, false, true)
	if status(r, ItemBreak, "list-items") != Green {
		t.Fatalf("list-items is noticed:\n%s", r.Text())
	}
	// send-mail still has web and api on its path, which are noticed.
	if status(r, ItemBreak, "send-mail") != Green {
		t.Fatalf("send-mail path has noticed components:\n%s", r.Text())
	}
	if status(r, ItemBreak, "worker") != Red {
		t.Fatalf("the unnoticed component is red:\n%s", r.Text())
	}
	if len(w.broken) != 0 {
		t.Fatalf("components left broken: %v", w.broken)
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
	// 4 components, each waits at least the bound.
	if elapsed := w.now.Sub(time.Unix(1000, 0)); elapsed < 4*60*time.Second {
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
	want := []string{"break web", "restore web", "break api", "restore api", "break db", "restore db", "break worker", "restore worker"}
	if strings.Join(w.log, ",") != strings.Join(want, ",") {
		t.Fatalf("log %v want %v", w.log, want)
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
