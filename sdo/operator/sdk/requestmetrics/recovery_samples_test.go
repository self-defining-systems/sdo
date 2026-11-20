package requestmetrics_test

import (
	"strings"
	"testing"

	"sdo.dev/operator/sdk/logs"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/requestmetrics"
	"sdo.dev/operator/sdk/sdktest"
)

// 12 steps (60 s): failing through step 5, then the route goes quiet.
func quietAfterFailures(tail ...float64) []metrics.Series {
	ok := append(append([]float64{}, steady(11, 6)...), tail...)
	bad := append([]float64{0, 0, 5, 8, 8, 8}, make([]float64, len(tail))...)
	return []metrics.Series{series(pkgRoute, "200", ok...), series(pkgRoute, "404", bad...)}
}

func TestQuietStretchDoesNotClearAFinding(t *testing.T) {
	// The failed requests rolled out of the window and almost nothing arrived: 2 requests.
	recent := quietAfterFailures(2, 2, 2, 2, 2, 2)
	got := detect(t, snapshot(recent, healthyBaseline(), presence()))
	if len(got) != 1 || got[0].Metadata["operation"] != "package-download" {
		t.Fatalf("finding cleared on a quiet stretch: %+v", got)
	}
	if !strings.Contains(got[0].Evidence, "Insufficient recent traffic to confirm recovery") || got[0].Metadata["recovery_unconfirmed"] != true {
		t.Fatalf("does not say recovery is unconfirmed: %q", got[0].Evidence)
	}
}

func TestHealthyVolumeClearsTheFinding(t *testing.T) {
	recent := quietAfterFailures(12, 12, 12, 12, 12, 12)
	if got := detect(t, snapshot(recent, healthyBaseline(), presence())); len(got) != 0 {
		t.Fatalf("healthy volume did not clear: %+v", got)
	}
}

func TestEnoughRequestsWithRemainingFailuresDoNotClear(t *testing.T) {
	// 20 requests, 3 failed: below the raise rule, but not all healthy.
	ok := append(append([]float64{}, steady(11, 6)...), steady(17, 6)...)
	bad := append([]float64{0, 0, 5, 8, 8, 8}, steady(3, 6)...)
	recent := []metrics.Series{series(pkgRoute, "200", ok...), series(pkgRoute, "500", bad...)}
	got := detect(t, snapshot(recent, healthyBaseline(), presence()))
	if len(got) != 1 || !strings.Contains(got[0].Evidence, "Recovery is not confirmed") {
		t.Fatalf("findings = %+v", got)
	}
}

func TestRecoveryRangeReachesBackFurtherThanTheWindow(t *testing.T) {
	for _, q := range newDetector().(metrics.Consumer).MetricQueries() {
		if q.Range > 0 && q.Range <= requestmetrics.DefaultRule().Window {
			t.Fatalf("range %s does not reach past the window", q.Range)
		}
	}
}

func webLogs(lines ...string) sdktest.Snapshot {
	s := snapshot([]metrics.Series{
		series(pkgRoute, "200", steady(11, 7)...),
		series(pkgRoute, "404", 0, 0, 0, 5, 8, 8, 8),
	}, healthyBaseline(), presence())
	expr := func(i int) string { return newDetector().(logs.Consumer).LogQueries()[0].Patterns[i].RE2 }
	m := map[string]logs.Match{}
	for _, l := range lines {
		i := 1
		if strings.HasPrefix(l, "{") {
			i = 0
		}
		cur := m[expr(i)]
		cur.Count++
		cur.Excerpts = append(cur.Excerpts, l)
		m[expr(i)] = cur
	}
	s.Logs = map[string]logs.Result{requestmetrics.LogQueryID: {Windows: []logs.Window{
		{Pod: "web-6d9f-abc", Container: "web", Cycles: []logs.Cycle{{Matches: m}}},
		{Pod: "db-0", Container: "db", Cycles: []logs.Cycle{{Matches: map[string]logs.Match{expr(1): {Count: 1, Excerpts: []string{`1.2.3.4 - - [01/Jan/2026:12:00:01 +0000] "GET /api/v4/projects/9/packages/generic/db/1/f HTTP/1.1" 404 12`}}}}}},
	}}}
	return s
}

func TestFindingCarriesFailingSamplesFromJSONLogLines(t *testing.T) {
	s := webLogs(
		`{"time":"2026-01-01T12:00:03.000Z","method":"GET","path":"/api/v4/projects/42/packages/generic/pkg/1.0/a.tgz?private_token=secret","status":404,"duration_s":0.01}`,
		`{"time":"2026-01-01T12:00:04.000Z","method":"GET","path":"/api/v4/projects/7/packages/generic/pkg/1.0/b.tgz","status":404}`,
		`{"time":"2026-01-01T12:00:05.000Z","method":"POST","path":"/api/v4/projects/7/packages/generic/pkg/1.0/c.tgz","status":404}`,
		`{"time":"2026-01-01T12:00:06.000Z","method":"GET","path":"/api/v4/projects/7","status":404}`,
	)
	got := detect(t, s)
	if len(got) != 1 {
		t.Fatalf("findings = %+v", got)
	}
	ev := got[0].Evidence
	for _, want := range []string{"GET /api/v4/projects/42/packages/generic/pkg/1.0/a.tgz -> 404 at 2026-01-01T12:00:03.000Z", "/projects/7/packages/generic/pkg/1.0/b.tgz -> 404", "web-6d9f-abc"} {
		if !strings.Contains(ev, want) {
			t.Fatalf("evidence lacks %q: %s", want, ev)
		}
	}
	if strings.Contains(ev, "secret") || strings.Contains(ev, "POST") || strings.Contains(ev, "db-0") || strings.Contains(ev, "projects/7 ") {
		t.Fatalf("query string, other method/route or other service leaked: %s", ev)
	}
	if samples, _ := got[0].Metadata["samples"].([]map[string]string); len(samples) != 2 || samples[0]["status"] != "404" {
		t.Fatalf("metadata samples = %+v", got[0].Metadata["samples"])
	}
}

func TestFindingCarriesFailingSamplesFromTextLogLines(t *testing.T) {
	s := webLogs(
		`10.0.0.1 - - [01/Jan/2026:12:00:03 +0000] "GET /api/v4/projects/42/packages/generic/pkg/1.0/a.tgz HTTP/1.1" 404 153 "-" "curl/8"`,
		`10.0.0.1 - - [01/Jan/2026:12:00:04 +0000] "GET /api/v4/projects/43/packages/generic/pkg/1.0/a.tgz HTTP/1.1" 404 153`,
	)
	got := detect(t, s)
	if len(got) != 1 || !strings.Contains(got[0].Evidence, "GET /api/v4/projects/43/packages/generic/pkg/1.0/a.tgz -> 404 at 01/Jan/2026:12:00:04 +0000") {
		t.Fatalf("findings = %+v", got)
	}
}

func TestNoSamplesSaysWhy(t *testing.T) {
	cases := map[string]struct {
		s    sdktest.Snapshot
		want string
	}{
		"no log result":  {snapshot([]metrics.Series{series(pkgRoute, "200", steady(11, 7)...), series(pkgRoute, "404", 0, 0, 0, 5, 8, 8, 8)}, healthyBaseline(), presence()), "no access-log result"},
		"not parseable":  {webLogs(`{"weird": 1}`, `{"status": 500`), "not parseable"},
		"other route":    {webLogs(`{"method":"GET","path":"/other","status":404}`), "none of the 1 failing access-log lines"},
		"no failed line": {webLogs(), "no failing access-log lines"},
	}
	for name, c := range cases {
		got := detect(t, c.s)
		if len(got) != 1 || !strings.Contains(got[0].Evidence, "No failing request samples") || !strings.Contains(got[0].Evidence, c.want) {
			t.Errorf("%s: evidence = %+v", name, got)
		}
	}
}

func TestDetectorDeclaresTheLogQuery(t *testing.T) {
	qs := newDetector().(logs.Consumer).LogQueries()
	if len(qs) != 1 || qs[0].Validate() != nil {
		t.Fatalf("log queries = %+v", qs)
	}
}

// A route nobody calls cannot be failing: once the newest window holds no
// requests the finding clears, so the incident can reach Recovered.
func TestRouteWithNoRequestsInTheWindowClearsTheFinding(t *testing.T) {
	recent := quietAfterFailures(0, 0, 0, 0, 0, 0)
	if got := detect(t, snapshot(recent, healthyBaseline(), presence())); len(got) != 0 {
		t.Fatalf("a route with no requests stayed failing: %+v", got)
	}
}

func TestHeldFindingClearsWhenTheRouteGetsNoRequests(t *testing.T) {
	held := snapshot([]metrics.Series{series(pkgRoute, "200", steady(0, 60)...), series(webRoute, "200", steady(40, 60)...)}, pollutedBaseline(), presence())
	held.HeldKeys = []string{pkgErrorsKey}
	if got := detect(t, held); len(got) != 0 {
		t.Fatalf("held finding stayed failing on zero samples: %+v", got)
	}
}
