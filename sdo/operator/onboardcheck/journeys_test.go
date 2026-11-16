package onboardcheck

import (
	"context"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/memory/appmap"
)

// metricItemsClean is true when no metric item is red; the probe and pointer
// items belong to the journey checklist.
func metricItemsClean(r Report) bool {
	for _, x := range r.Results {
		if x.Status == Red && x.Item != ItemProbe && x.Item != ItemPointers && x.Item != ItemRoutesAssigned {
			return false
		}
	}
	return true
}

const journeyMap = `version: 1
components:
  - {name: web, workload: web, services: [web], observe: {metrics: "job=web", logs: "container web", trace_service: web}}
  - {name: api, workload: api, services: [api], observe: {metrics: "job=api", logs: "container api"}}
  - {name: db, workload: db, services: [db], holds_data: true, data: {engine: postgres, role: authoritative}, observe: {metrics: "job=db", logs: "container db"}}
  - {name: worker, workload: worker, services: [], observe: {metrics: "job=worker", logs: "container worker"}}
edges:
  - {from: web, to: api, evidence: [config], source: x}
  - {from: api, to: db, evidence: [config], source: x}
  - {from: api, to: worker, evidence: [config], source: x}
entries:
  - {service: web, metric: http_requests_total, selector: {job: web}, labels: {route: route, method: method, status: code}, errors: "5.."}
  - {service: queue, metric: jobs_total, selector: {job: web}, labels: {route: class, status: outcome}, errors: "failed"}
operations:
  - id: list-items
    entry: {service: web, port: 80, method: GET, path: /items}
    path: [web, api, db]
  - id: send-mail
    writes: true
    triggers: [MailWorker]
    entry: {service: web, port: 80, method: POST, path: /mail}
    path: [web, api, worker]
routes:
  list: {component: web, command: [routes]}
  excluded:
    - {route: GET /healthz, why: liveness only}
probes:
  - {scenario: browse, kind: call, operations: [list-items]}
  - {scenario: mail, kind: write-read, operations: [send-mail]}
`

func journeyRun(t *testing.T, mapText string, w *world, brk bool) Report {
	t.Helper()
	cfg := liveConfig(t, w, false, brk)
	m, err := appmap.Parse(mapText)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Map = m
	cfg.Routes = fakeRoutes{"GET /items", "POST /mail", "GET /healthz"}
	cfg.Prom.(*fakeProm).add("jobs_total", map[string]string{"job": "web", "class": "MailWorker", "outcome": "done"}, 5)
	r, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func journeyOf(t *testing.T, r Report, op string) Journey {
	t.Helper()
	for _, j := range r.Journeys {
		if j.Operation == op {
			return j
		}
	}
	t.Fatalf("no journey row for %s:\n%s", op, r.Text())
	return Journey{}
}

func allDetected(w *world) {
	w.raiseFor["web"] = raiseNaming("traffic-probe-call", "scenario browse failed at step list")
	w.raiseFor["api"] = []Raise{{DetectorID: "traffic-probe-call", Rule: "scenario-slo.mail", Summary: "scenario mail failed: send-mail 503"}, {DetectorID: "traffic-probe-call", Rule: "scenario-slo.browse", Summary: "scenario browse failed"}}
	w.raiseFor["db"] = []Raise{{DetectorID: "traffic-probe-call", Rule: "scenario-slo.browse", Summary: "scenario browse failed"}}
	w.raiseFor["worker"] = []Raise{{DetectorID: "traffic-probe-call", Rule: "scenario-slo.mail", Summary: "scenario mail failed: send-mail result never appeared"}}
}

func TestEveryJourneyHasARowAndCleanNeedsThemComplete(t *testing.T) {
	w := newWorld()
	allDetected(w)
	// web breaks both operations (both start at web).
	w.raiseFor["web"] = append(raiseNaming("traffic-probe-call", "scenario browse failed"), Raise{DetectorID: "traffic-probe-call", Rule: "scenario-slo.mail", Summary: "scenario mail failed: send-mail 503"})
	r := journeyRun(t, journeyMap, w, true)
	if len(r.Journeys) != 2 {
		t.Fatalf("one row per declared operation:\n%s", r.Text())
	}
	for _, op := range []string{"list-items", "send-mail"} {
		j := journeyOf(t, r, op)
		if !j.Complete() || j.Probe.Status != Green || j.Detection.Status != Green || j.Map.Status != Green {
			t.Fatalf("%s should be complete: %+v\n%s", op, j, r.Text())
		}
	}
	if !strings.Contains(r.Text(), "journeys (a mapping") || !r.Clean() {
		t.Fatalf("every journey complete means clean:\n%s", r.Text())
	}
}

func TestJourneyWithoutBreakIsNotComplete(t *testing.T) {
	r := journeyRun(t, journeyMap, newWorld(), false)
	j := journeyOf(t, r, "list-items")
	if j.Detection.Status == Green || !j.Detection.NotRun || j.Complete() || r.Clean() {
		t.Fatalf("detection was not run, so the journey is incomplete:\n%s", r.Text())
	}
}

func TestOperationWithoutAResultCheckingProbeIsRed(t *testing.T) {
	text := strings.Replace(journeyMap, "  - {scenario: browse, kind: call, operations: [list-items]}\n", "", 1)
	r := journeyRun(t, text, newWorld(), false)
	if status(r, ItemProbe, "list-items") != Red || journeyOf(t, r, "list-items").Probe.Status != Red {
		t.Fatalf("no probe:\n%s", r.Text())
	}
}

func TestWriteOrBackgroundJourneyNeedsAProbeThatReadsTheEffect(t *testing.T) {
	text := strings.Replace(journeyMap, "kind: write-read, operations: [send-mail]", "kind: call, operations: [send-mail]", 1)
	r := journeyRun(t, text, newWorld(), false)
	if status(r, ItemProbe, "send-mail") != Red || !strings.Contains(r.Text(), "triggers background work") {
		t.Fatalf("a call probe does not show the triggered work finished:\n%s", r.Text())
	}
}

func TestPathComponentsNeedObservabilityPointers(t *testing.T) {
	text := strings.Replace(journeyMap, `, observe: {metrics: "job=db", logs: "container db"}`, "", 1)
	r := journeyRun(t, text, newWorld(), false)
	if status(r, ItemPointers, "list-items") != Red || status(r, ItemPointers, "send-mail") != Green || !strings.Contains(r.Text(), "db") {
		t.Fatalf("db has no pointers:\n%s", r.Text())
	}
	if c := journeyOf(t, r, "list-items").Traces; c.Status == Red {
		t.Fatalf("traces are only a note: %+v", c)
	}
}

func TestMissingEdgeIsRedAndSpuriousEdgeIsANote(t *testing.T) {
	w := newWorld()
	allDetected(w)
	// Breaking db fails send-mail too, but send-mail's path does not list db.
	w.raiseFor["db"] = append(w.raiseFor["db"], Raise{DetectorID: "traffic-probe-call", Rule: "scenario-slo.mail", Summary: "scenario mail failed: send-mail 503"})
	// Breaking api leaves send-mail healthy although its path lists api.
	w.raiseFor["api"] = []Raise{{DetectorID: "traffic-probe-call", Rule: "scenario-slo.browse", Summary: "scenario browse failed"}}
	r := journeyRun(t, strings.Replace(journeyMap, "path: [web, api, worker]", "path: [web, api]", 1), w, true)
	if status(r, ItemMapAccuracy, "db") != Red || !strings.Contains(r.Text(), "missing edges: send-mail") {
		t.Fatalf("send-mail failed with db down but does not list db:\n%s", r.Text())
	}
	if status(r, ItemMapAccuracy, "api") != Note || !strings.Contains(r.Text(), "spurious edges: send-mail") {
		t.Fatalf("send-mail stayed healthy with api down:\n%s", r.Text())
	}
	j := journeyOf(t, r, "send-mail")
	if j.Map.Status != Red || r.Clean() {
		t.Fatalf("a missing edge makes the journey's map red: %+v", j.Map)
	}
}

func TestUnattributedBackgroundWorkIsReportedAndCanBeExcludedWithAReason(t *testing.T) {
	seed := func(p *fakeProm) {
		p.add("jobs_total", map[string]string{"job": "web", "class": "MailWorker", "outcome": "done"}, 5)
		p.add("jobs_total", map[string]string{"job": "web", "class": "VacuumWorker", "outcome": "done"}, 9)
	}
	run := func(text string) Report {
		m, err := appmap.Parse(text)
		if err != nil {
			t.Fatal(err)
		}
		p := &fakeProm{}
		seed(p)
		r, err := Run(context.Background(), Config{Map: m, Commit: "c", Prom: p, Sender: fakeSender{prom: p, emit: perRoute}, Requests: 1, Settle: time.Second, Poll: time.Second, Wait: p.wait})
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	r := run(journeyMap)
	if status(r, ItemBackground, "queue") != Red || !strings.Contains(r.Text(), "unattributed background work") || !strings.Contains(r.Text(), "VacuumWorker (9)") || strings.Contains(r.Text(), "MailWorker (5)") {
		t.Fatalf("VacuumWorker has no triggering journey; MailWorker is triggered by send-mail:\n%s", r.Text())
	}
	r = run(strings.Replace(journeyMap, "probes:", "reasons:\n  - {item: background-covered, subject: VacuumWorker, reason: periodic maintenance}\nprobes:", 1))
	if status(r, ItemBackground, "queue") != Green {
		t.Fatalf("an excluded class is accepted with a reason:\n%s", r.Text())
	}
	r = run(strings.Replace(journeyMap, "triggers: [MailWorker]", "triggers: [MailWorker, GhostWorker]", 1))
	if journeyOf(t, r, "send-mail").Mapping.Status != Red || !strings.Contains(r.Text(), "GhostWorker") {
		t.Fatalf("a triggered class that never appears makes mapping red:\n%s", r.Text())
	}
}

func TestErrorPatternMustSeparateServerFromClientErrors(t *testing.T) {
	if got := errorClasses("5..", []string{"200", "404", "500"}); got != "" {
		t.Fatalf("5.. separates: %s", got)
	}
	if got := errorClasses("[45]..", []string{"200"}); !strings.Contains(got, "client errors") {
		t.Fatalf("4xx counted: %q", got)
	}
	if got := errorClasses("503", []string{"200"}); !strings.Contains(got, "server errors") {
		t.Fatalf("500 not counted: %q", got)
	}
	if got := errorClasses("failed", []string{"done", "failed"}); got != "" {
		t.Fatalf("non-HTTP statuses are not judged: %q", got)
	}
}

var _ = time.Second

type fakeRoutes []string

func (f fakeRoutes) List(context.Context, appmap.Component, appmap.RouteList) ([]string, error) {
	return f, nil
}

func TestListedRoutesMustBeAJourneyOrExcluded(t *testing.T) {
	cfg := func(routes ...string) Report {
		m, _ := appmap.Parse(journeyMap)
		c := liveConfig(t, newWorld(), false, false)
		c.Map, c.Routes = m, fakeRoutes(routes)
		r, err := Run(context.Background(), c)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	r := cfg("GET /items", "get /mail", "POST /mail", "GET /healthz", "DELETE /items/:id")
	if status(r, ItemRoutesAssigned, "DELETE /items/:id") != Red || status(r, ItemRoutesAssigned, "routes") == Green {
		t.Fatalf("an unassigned route is red:\n%s", r.Text())
	}
	if status(r, ItemRoutesAssigned, "get /mail") != Red {
		t.Fatalf("method is part of the match:\n%s", r.Text())
	}
	if r := cfg("GET /items", "POST /mail", "GET /healthz"); status(r, ItemRoutesAssigned, "routes") != Green {
		t.Fatalf("all assigned:\n%s", r.Text())
	}
	m, _ := appmap.Parse(strings.Replace(journeyMap, "routes:\n  list: {component: web, command: [routes]}\n", "", 1))
	c := liveConfig(t, newWorld(), false, false)
	c.Map = m
	r, _ = Run(context.Background(), c)
	if status(r, ItemRoutesAssigned, "routes") != Red || !strings.Contains(r.Text(), "no route list") {
		t.Fatalf("red while the list is missing:\n%s", r.Text())
	}
}

func TestGivenJourneysNeedAnOperationAndOperationsNeedAKnownJourney(t *testing.T) {
	text := strings.Replace(journeyMap, "id: list-items\n", "id: list-items\n    journey: browse-items\n", 1)
	text = strings.Replace(text, "id: send-mail\n", "id: send-mail\n    journey: nonesuch\n", 1)
	m, err := appmap.Parse(text)
	if err != nil {
		t.Fatal(err)
	}
	c := liveConfig(t, newWorld(), false, false)
	c.Map = m
	c.Journeys = []appmap.GivenJourney{{ID: "browse-items", What: "a user lists items"}, {ID: "send-a-message", What: "a user sends mail"}}
	r, err := Run(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	if status(r, ItemGivenJourneys, "send-a-message") != Red || !strings.Contains(r.Text(), "no operation serves it") {
		t.Fatalf("a given journey with no operation is red:\n%s", r.Text())
	}
	if status(r, ItemGivenJourneys, "send-mail") != Red || !strings.Contains(r.Text(), "nonesuch") {
		t.Fatalf("an operation naming an unknown journey is red:\n%s", r.Text())
	}
	if status(r, ItemGivenJourneys, "browse-items") != Green {
		t.Fatalf("a served journey is green:\n%s", r.Text())
	}
	if !strings.Contains(r.Text(), "list-items (browse-items)") {
		t.Fatalf("the table names the journey:\n%s", r.Text())
	}
	// Without a list nothing is judged.
	c.Journeys = nil
	r, _ = Run(context.Background(), c)
	if status(r, ItemGivenJourneys, "send-mail") != "missing" {
		t.Fatalf("no list, no item:\n%s", r.Text())
	}
}

// An operation that enters through the job metric declares its own class as a
// route, but that is not attribution: no operation triggers the class, so the
// background work is still unattributed (the grader's attribution follows
// triggers).
func TestAnOperationEnteringAtTheJobMetricDoesNotAttributeItsOwnClass(t *testing.T) {
	text := strings.Replace(journeyMap, "routes:\n  list:", "  - id: vacuum\n    route: VacuumWorker\n    entry: {service: queue}\n    path: [worker]\nroutes:\n  list:", 1)
	m, err := appmap.Parse(text)
	if err != nil {
		t.Fatal(err)
	}
	p := &fakeProm{}
	p.add("jobs_total", map[string]string{"job": "web", "class": "MailWorker", "outcome": "done"}, 5)
	p.add("jobs_total", map[string]string{"job": "web", "class": "VacuumWorker", "outcome": "done"}, 9)
	r, err := Run(context.Background(), Config{Map: m, Commit: "c", Prom: p, Sender: fakeSender{prom: p, emit: perRoute}, Requests: 1, Settle: time.Second, Poll: time.Second, Wait: p.wait})
	if err != nil {
		t.Fatal(err)
	}
	if status(r, ItemBackground, "queue") != Red || !strings.Contains(r.Text(), "VacuumWorker (9)") {
		t.Fatalf("a class only its own operation declares is unattributed:\n%s", r.Text())
	}
}
