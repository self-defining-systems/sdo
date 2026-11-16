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
		if x.Status == Red && x.Item != ItemProbe && x.Item != ItemPointers {
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
