package appmap

import "testing"

const entriesBase = `version: 1
components: []
edges: []
operations:
  - id: list-items
    entry: {service: web, port: 80, method: GET, path: /items}
    route: "GET /items"
    correct: "200 with the items"
    path: [web]
  - id: send-receipt
    writes: true
    entry: {service: internal, path: receipts}
    path: [web]
probes: []
`

func TestEntriesSchema(t *testing.T) {
	good := entriesBase + `entries:
  - {service: web, metric: http_requests_total, selector: {job: web}, labels: {route: route, method: method, status: code}, errors: "5.."}
  - {service: internal, metric: jobs_total, labels: {route: queue, status: outcome}, errors: "failed|dead"}
reasons:
  - {item: journeys-move-own-metrics, subject: list-items, reason: no route label}
`
	m := mustParse(t, good)
	if got := m.Operations[0].RouteValue(); got != "GET /items" {
		t.Fatalf("route %q", got)
	}
	if got := m.Operations[1].RouteValue(); got != "receipts" {
		t.Fatalf("default route %q", got)
	}
	if !m.Internal(m.Operations[1]) || m.Internal(m.Operations[0]) {
		t.Fatal("internal entry detection")
	}
	c := checker{m: m}
	c.entries()
	if len(c.findings) != 0 {
		t.Fatalf("good map: %v", c.findings)
	}
	for name, mut := range map[string]string{
		"no route label":      `entries: [{service: web, metric: m, labels: {method: m}}]`,
		"errors without stat": `entries: [{service: web, metric: m, labels: {route: r}, errors: "5.."}]`,
		"status no errors":    `entries: [{service: web, metric: m, labels: {route: r, status: s}}]`,
		"bad regexp":          `entries: [{service: web, metric: m, labels: {route: r, status: s}, errors: "("}]`,
		"duplicate":           `entries: [{service: web, metric: m, labels: {route: r}}, {service: web, metric: m, labels: {route: r}}]`,
		"reason item":         `reasons: [{item: quiet-when-healthy, subject: list-items, reason: x}]`,
		"reason subject":      `reasons: [{item: journeys-move-own-metrics, subject: nobody, reason: x}]`,
	} {
		c := checker{m: mustParse(t, entriesBase+mut)}
		c.entries()
		if len(c.findings) == 0 {
			t.Errorf("%s accepted", name)
		}
	}
}

func TestSeriesAreBuiltFromDeclaredLabels(t *testing.T) {
	e := MetricEntry{Service: "web", Metric: "http_requests_total", Selector: map[string]string{"job": "web", "env": "p"}, Labels: EntryLabels{Route: "route", Method: "method", Status: "code"}, Errors: "5.."}
	if got, want := e.RequestCount("/items", "GET"), `sum(http_requests_total{env="p",job="web",route="/items",method=~"(?i:GET)"})`; got != want {
		t.Fatalf("requests %s", got)
	}
	if got, want := e.ErrorCount("/items", "GET"), `sum(http_requests_total{env="p",job="web",route="/items",method=~"(?i:GET)",code=~"5.."})`; got != want {
		t.Fatalf("errors %s", got)
	}
	if got, want := e.RateErrors("/i", "POST", "5m"), `sum(increase(http_requests_total{env="p",job="web",route="/i",method=~"(?i:POST)",code=~"5.."}[5m]))`; got != want {
		t.Fatalf("rate %s", got)
	}
}

func TestInternalEntryNeedsAnEntriesRecord(t *testing.T) {
	m := mustParse(t, entriesBase)
	findings := m.Check(Inventory{Workloads: map[string]bool{}, Services: map[string]bool{}}, nil)
	if !has(findings, RuleSchema, "send-receipt") {
		t.Fatalf("findings %v", findings)
	}
}
