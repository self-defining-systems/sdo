package onboardcheck

import (
	"context"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/memory/appmap"
)

const observedMap = `version: 1
components: []
edges: []
entries:
  - {service: web, metric: http_requests_total, selector: {job: web}, labels: {route: route, method: method, status: code}, errors: "5.."}
  - {service: worker, metric: jobs_total, selector: {job: web}, labels: {route: class, status: outcome}, errors: "failed"}
operations:
  - id: list-items
    entry: {service: web, port: 80, method: GET, path: /items}
    path: [web]
  - id: add-item
    writes: true
    entry: {service: web, port: 80, method: POST, path: /items}
    route: /items/new
    path: [web]
  - id: send-mail
    entry: {service: worker, port: 0, method: "", path: MailWorker}
    route: MailWorker
    path: [web]
probes: []
`

func runObservedWith(t *testing.T, yaml string, seed func(*fakeProm), noLatency bool) Report {
	t.Helper()
	m, err := appmap.Parse(yaml)
	if err != nil {
		t.Fatal(err)
	}
	p := &fakeProm{}
	if seed != nil {
		seed(p)
	}
	r, err := Run(context.Background(), Config{Map: m, Commit: "c", Prom: p,
		Sender:   fakeSender{prom: p, emit: perRoute, noLatency: noLatency},
		Requests: 5, Settle: time.Second, Poll: time.Second, Wait: p.wait})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestDeclaredRouteAbsentFromMetricIsRedEvenWithAReason(t *testing.T) {
	yaml := strings.Replace(observedMap, "probes: []", "reasons:\n  - {item: journeys-move-own-metrics, subject: add-item, reason: hard to drive}\nprobes: []", 1)
	m, _ := appmap.Parse(yaml)
	p := &fakeProm{}
	r, err := Run(context.Background(), Config{Map: m, Commit: "c", Prom: p,
		// the application labels the request /items/create, not the declared /items/new
		Sender: fakeSender{prom: p, emit: func(op appmap.Operation) map[string]string {
			l := perRoute(op)
			if op.ID == "add-item" {
				l["route"] = "/items/create"
			}
			return l
		}},
		Requests: 5, Settle: time.Second, Poll: time.Second, Wait: p.wait})
	if err != nil {
		t.Fatal(err)
	}
	if status(r, ItemJourneys, "add-item") != Note {
		t.Fatalf("the reason still excuses the journeys item:\n%s", r.Text())
	}
	if status(r, ItemObserved, "add-item") != Red || !strings.Contains(r.Text(), `nearest observed route label: "/items/create"`) {
		t.Fatalf("absent route must be red with the nearest label:\n%s", r.Text())
	}
	if status(r, ItemObserved, "list-items") != Green {
		t.Fatalf("present route:\n%s", r.Text())
	}
}

func TestOperationWithoutLatencySeriesIsRed(t *testing.T) {
	r := runObservedWith(t, observedMap, nil, true)
	for _, op := range []string{"list-items", "add-item"} {
		if status(r, ItemLatency, op) != Red || !strings.Contains(r.Text(), "no request-duration histogram") {
			t.Fatalf("%s needs a latency series:\n%s", op, r.Text())
		}
	}
	r = runObservedWith(t, observedMap, nil, false)
	if status(r, ItemLatency, "list-items") != Green {
		t.Fatalf("latency present:\n%s", r.Text())
	}
}

func TestLatencyFamilyThatMissesTheRouteNamesTheNearestLabel(t *testing.T) {
	r := runObservedWith(t, observedMap, func(p *fakeProm) {
		p.add("http_request_duration_seconds_bucket", map[string]string{"job": "web", "route": "/item"}, 1)
	}, true)
	if status(r, ItemLatency, "list-items") != Red || !strings.Contains(r.Text(), `nearest latency route label: "/item"`) {
		t.Fatalf("expected the nearest label:\n%s", r.Text())
	}
}

func TestUndeclaredBackgroundClassesAreRed(t *testing.T) {
	r := runObservedWith(t, observedMap, func(p *fakeProm) {
		p.add("jobs_total", map[string]string{"job": "web", "class": "PostReceiveWorker", "outcome": "done"}, 47)
		p.add("jobs_total", map[string]string{"job": "web", "class": "MailWorker", "outcome": "done"}, 3)
	}, false)
	if status(r, ItemBackground, "worker") != Red || !strings.Contains(r.Text(), "PostReceiveWorker (47)") {
		t.Fatalf("undeclared job class:\n%s", r.Text())
	}
	r = runObservedWith(t, observedMap, func(p *fakeProm) {
		p.add("jobs_total", map[string]string{"job": "web", "class": "MailWorker", "outcome": "done"}, 3)
	}, false)
	if status(r, ItemBackground, "worker") != Green {
		t.Fatalf("declared class:\n%s", r.Text())
	}
}

func TestMostTrafficOnUndeclaredRoutesIsRed(t *testing.T) {
	r := runObservedWith(t, observedMap, func(p *fakeProm) {
		p.add("http_requests_total", map[string]string{"job": "web", "route": "/api/other", "method": "GET", "code": "200"}, 100)
	}, false)
	if status(r, ItemObserved, "web") != Red || !strings.Contains(r.Text(), "/api/other (100)") {
		t.Fatalf("low coverage:\n%s", r.Text())
	}
	r = runObservedWith(t, observedMap, func(p *fakeProm) {
		p.add("http_requests_total", map[string]string{"job": "web", "route": "/items", "method": "GET", "code": "200"}, 100)
		p.add("http_requests_total", map[string]string{"job": "web", "route": "/health", "method": "GET", "code": "200"}, 10)
	}, false)
	if status(r, ItemObserved, "web") != Green {
		t.Fatalf("mostly declared:\n%s", r.Text())
	}
}
