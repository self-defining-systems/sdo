package onboardcheck

import (
	"context"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/memory/appmap"
)

// The routes-match-operations red names `routes: {excluded: [...]}` as one of
// its fixes, so a route listed there must clear it, and the same route must
// not count against operations-observed's share of declared traffic.
func TestExcludedRoutesClearTheRouteReds(t *testing.T) {
	excluded := "routes:\n  excluded:\n    - {route: GET /healthz, why: kubelet probe}\n    - {route: /metrics, why: Prometheus scrape}\nprobes: []"
	stray := func(yaml string) Report {
		t.Helper()
		m, err := appmap.Parse(yaml)
		if err != nil {
			t.Fatal(err)
		}
		p := &fakeProm{}
		r, err := Run(context.Background(), Config{Map: m, Commit: "c", Prom: p,
			Sender: fakeSender{prom: p, emit: perRoute, bg: func() {
				p.add("http_requests_total", map[string]string{"job": "web", "route": "/healthz", "method": "GET", "code": "200"}, 3)
				p.add("http_requests_total", map[string]string{"job": "web", "route": "/metrics", "method": "GET", "code": "200"}, 2)
			}},
			Requests: 5, Settle: time.Second, Poll: time.Second, Wait: p.wait})
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	if r := stray(mapYAML); status(r, ItemRoutes, "web") != Red {
		t.Fatalf("stray routes without an exclusion:\n%s", r.Text())
	}
	if r := stray(strings.Replace(mapYAML, "probes: []", excluded, 1)); status(r, ItemRoutes, "web") == Red {
		t.Fatalf("excluded routes still red:\n%s", r.Text())
	}

	busy := func(p *fakeProm) {
		p.add("http_requests_total", map[string]string{"job": "web", "route": "/healthz", "method": "GET", "code": "200"}, 100)
		p.add("http_requests_total", map[string]string{"job": "web", "route": "/metrics", "method": "GET", "code": "200"}, 100)
	}
	if r := runObservedWith(t, observedMap, busy, false); status(r, ItemObserved, "web") != Red {
		t.Fatalf("undeclared health and scrape traffic:\n%s", r.Text())
	}
	if r := runObservedWith(t, strings.Replace(observedMap, "probes: []", excluded, 1), busy, false); status(r, ItemObserved, "web") != Green {
		t.Fatalf("excluded health and scrape traffic still counted:\n%s", r.Text())
	}
}
