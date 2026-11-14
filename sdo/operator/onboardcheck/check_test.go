package onboardcheck

import (
	"context"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/sdk/metrics"
)

// fakeProm is an in-memory Prometheus: counters keyed by label set, evaluated
// for the two query shapes SDO builds (sum and sum by). Increments become
// visible after `lag` waits, like a scrape interval.
type fakeProm struct {
	series  []*fakeSeries
	pending []func()
	lag     int
	waits   int
	queries []string
}

type fakeSeries struct {
	metric string
	labels map[string]string
	value  float64
}

var (
	queryShape   = regexp.MustCompile(`^sum(?: by \((\w+)\) \()?\(?([a-zA-Z_:][a-zA-Z0-9_:]*)\{(.*)\}\)\)?$`)
	matcherShape = regexp.MustCompile(`(\w+)(=~|=)("(?:[^"\\]|\\.)*")`)
)

func (p *fakeProm) add(metric string, labels map[string]string, delta float64) {
	for _, s := range p.series {
		if s.metric == metric && sameLabels(s.labels, labels) {
			s.value += delta
			return
		}
	}
	p.series = append(p.series, &fakeSeries{metric: metric, labels: labels, value: delta})
}

func sameLabels(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func (p *fakeProm) Query(_ context.Context, text string) ([]metrics.Series, error) {
	p.queries = append(p.queries, text)
	m := queryShape.FindStringSubmatch(text)
	if m == nil {
		return nil, context.DeadlineExceeded
	}
	by, metric := m[1], m[2]
	type matcher struct {
		label string
		re    *regexp.Regexp
	}
	var ms []matcher
	for _, x := range matcherShape.FindAllStringSubmatch(m[3], -1) {
		value, _ := strconv.Unquote(x[3])
		if x[2] == "=" {
			value = regexp.QuoteMeta(value)
		}
		ms = append(ms, matcher{x[1], regexp.MustCompile("^(?:" + value + ")$")})
	}
	groups := map[string]float64{}
	for _, s := range p.series {
		if s.metric != metric {
			continue
		}
		ok := true
		for _, x := range ms {
			if !x.re.MatchString(s.labels[x.label]) {
				ok = false
			}
		}
		if ok {
			groups[s.labels[by]] += s.value
		}
	}
	var out []metrics.Series
	for key, v := range groups {
		labels := map[string]string{}
		if by != "" {
			labels[by] = key
		}
		out = append(out, metrics.Series{Labels: labels, Samples: []metrics.Sample{{At: time.Unix(0, 0), Value: v}}})
	}
	return out, nil
}

func (p *fakeProm) wait(context.Context, time.Duration) error {
	p.waits++
	if p.waits >= p.lag {
		for _, f := range p.pending {
			f()
		}
		p.pending = nil
	}
	return nil
}

// fakeSender emits one request per call into the fake's counters, labelled by
// emit, and returns what it sent.
type fakeSender struct {
	prom *fakeProm
	emit func(op appmap.Operation) map[string]string
	bg   func()
}

func (s fakeSender) Send(_ context.Context, op appmap.Operation, n int) (int, error) {
	if s.bg != nil {
		s.bg()
	}
	if op.Entry.Method == "" {
		return 0, nil
	}
	labels := s.emit(op)
	apply := func() { s.prom.add("http_requests_total", labels, float64(n)) }
	if s.prom.lag > 0 {
		s.prom.pending = append(s.prom.pending, apply)
	} else {
		apply()
	}
	return n, nil
}

const mapYAML = `version: 1
components: []
edges: []
entries:
  - {service: web, metric: http_requests_total, selector: {job: web}, labels: {route: route, method: method, status: code}, errors: "5.."}
operations:
  - id: list-items
    entry: {service: web, port: 80, method: GET, path: /items}
    path: [web]
  - id: add-item
    writes: true
    entry: {service: web, port: 80, method: POST, path: /items}
    route: /items/new
    path: [web]
probes: []
`

func run(t *testing.T, yaml string, lag int, settle time.Duration, emit func(appmap.Operation) map[string]string, exposed ...Exposure) (Report, *fakeProm) {
	t.Helper()
	m, err := appmap.Parse(yaml)
	if err != nil {
		t.Fatal(err)
	}
	p := &fakeProm{lag: lag}
	report, err := Run(context.Background(), Config{
		Map: m, Commit: "abc123", Prom: p, Sender: fakeSender{prom: p, emit: emit}, Exposed: exposed,
		Requests: 5, Settle: settle, Poll: time.Second, Wait: p.wait,
	})
	if err != nil {
		t.Fatal(err)
	}
	return report, p
}

func perRoute(op appmap.Operation) map[string]string {
	return map[string]string{"job": "web", "route": op.RouteValue(), "method": op.Entry.Method, "code": "200"}
}

func status(r Report, item, subject string) string {
	for _, x := range r.Results {
		if x.Item == item && x.Subject == subject {
			return x.Status
		}
	}
	return "missing"
}

func TestPerRouteSeriesAreGreen(t *testing.T) {
	r, _ := run(t, mapYAML, 0, 10*time.Second, perRoute, Exposure{Service: "web", Via: "NodePort"})
	if !strings.HasPrefix(r.Text(), "commit abc123\n") {
		t.Fatalf("report is not headed by the commit:\n%s", r.Text())
	}
	if !r.Clean() {
		t.Fatalf("expected clean:\n%s", r.Text())
	}
	if status(r, ItemJourneys, "add-item") != Green || status(r, ItemErrorsSeparable, "list-items") != Green || status(r, ItemRoutes, "web") != Green {
		t.Fatalf("items:\n%s", r.Text())
	}
}

func TestUnknownRouteLabelIsRed(t *testing.T) {
	unknown := func(op appmap.Operation) map[string]string {
		return map[string]string{"job": "web", "route": "unknown", "method": op.Entry.Method, "code": "200"}
	}
	r, _ := run(t, mapYAML, 0, 3*time.Second, unknown)
	for _, op := range []string{"list-items", "add-item"} {
		if status(r, ItemJourneys, op) != Red || status(r, ItemErrorsSeparable, op) != Red {
			t.Fatalf("%s should be red:\n%s", op, r.Text())
		}
	}
	if !strings.Contains(r.Text(), "route values on this metric: unknown") {
		t.Fatalf("detail missing:\n%s", r.Text())
	}
	if status(r, ItemRoutes, "web") != Red {
		t.Fatalf("undeclared route should be red:\n%s", r.Text())
	}
}

func TestSharedRouteValueIsRed(t *testing.T) {
	yaml := strings.Replace(mapYAML, "route: /items/new", "route: /items", 1)
	r, _ := run(t, yaml, 0, 3*time.Second, perRoute)
	// Same method differs (GET vs POST) so the label pair is distinct; make methods equal.
	if !r.Clean() {
		t.Fatalf("distinct methods share a route legitimately:\n%s", r.Text())
	}
	yaml = strings.Replace(yaml, "method: POST", "method: GET", 1)
	r, _ = run(t, yaml, 0, 3*time.Second, perRoute)
	if status(r, ItemJourneys, "add-item") != Red || !strings.Contains(r.Text(), "shared by operations") {
		t.Fatalf("shared value not red:\n%s", r.Text())
	}
}

func TestNoStatusLabelIsNotSeparable(t *testing.T) {
	yaml := strings.Replace(mapYAML, ", status: code}, errors: \"5..\"", "}", 1)
	r, _ := run(t, yaml, 0, 3*time.Second, perRoute)
	if status(r, ItemJourneys, "list-items") != Green || status(r, ItemErrorsSeparable, "list-items") != Red {
		t.Fatalf("volume green, errors red expected:\n%s", r.Text())
	}
	withReason := yaml + "reasons:\n  - {item: journeys-move-own-metrics, subject: list-items, reason: no status in this build}\n"
	r, _ = run(t, withReason, 0, 3*time.Second, perRoute)
	if status(r, ItemErrorsSeparable, "list-items") != Note {
		t.Fatalf("reason should turn red into note:\n%s", r.Text())
	}
}

func TestScrapeLagWithinSettleIsGreenBeyondIsRed(t *testing.T) {
	r, _ := run(t, mapYAML, 3, 10*time.Second, perRoute)
	if !r.Clean() {
		t.Fatalf("lag inside settle:\n%s", r.Text())
	}
	r, _ = run(t, mapYAML, 30, 2*time.Second, perRoute)
	if status(r, ItemJourneys, "list-items") != Red {
		t.Fatalf("lag past settle:\n%s", r.Text())
	}
}

func TestNarrowSelectorAndStrayRoutes(t *testing.T) {
	// The application emits under a job the selector does not match.
	other := func(op appmap.Operation) map[string]string {
		l := perRoute(op)
		l["job"] = "web-canary"
		return l
	}
	r, _ := run(t, mapYAML, 0, 2*time.Second, other)
	if status(r, ItemJourneys, "list-items") != Red {
		t.Fatalf("narrow selector:\n%s", r.Text())
	}
	m, _ := appmap.Parse(mapYAML)
	p := &fakeProm{}
	r, err := Run(context.Background(), Config{Map: m, Commit: "c", Prom: p,
		Sender: fakeSender{prom: p, emit: perRoute, bg: func() {
			p.add("http_requests_total", map[string]string{"job": "web", "route": "/healthz", "method": "GET", "code": "200"}, 3)
		}},
		Requests: 5, Settle: time.Second, Poll: time.Second, Wait: p.wait})
	if err != nil {
		t.Fatal(err)
	}
	if status(r, ItemRoutes, "web") != Red || !strings.Contains(r.Text(), "/healthz") {
		t.Fatalf("stray route:\n%s", r.Text())
	}
}

func TestExposedEntryNeedsARecord(t *testing.T) {
	r, _ := run(t, mapYAML, 0, time.Second, perRoute, Exposure{Service: "gateway", Via: "Ingress"})
	if status(r, ItemRoutes, "gateway") != Red {
		t.Fatalf("exposed entry without record:\n%s", r.Text())
	}
}

func TestConfigIsValidated(t *testing.T) {
	if _, err := Run(context.Background(), Config{}); err == nil {
		t.Fatal("empty config accepted")
	}
}
