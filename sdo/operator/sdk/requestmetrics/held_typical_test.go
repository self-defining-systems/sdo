package requestmetrics_test

import (
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/sdktest"
)

// history is a baseline history: one point per ten minutes, the newest at the
// snapshot's baseline time, oldest first.
func history(route, status string, values ...float64) metrics.Series {
	s := metrics.Series{Labels: map[string]string{"endpoint_id": route, "method": "GET", "status": status}}
	for i, v := range values {
		at := t0.Add(-time.Duration(len(values)-1-i) * 10 * time.Minute)
		s.Samples = append(s.Samples, metrics.Sample{At: at, Value: v})
	}
	return s
}

// routeHistory is six hours of a route answering clientShare client errors
// and serverShare server errors of 1000 requests per ten minutes, with the
// newest failing points answering failShare client errors instead.
func routeHistory(clientShare, serverShare, failShare float64, failing int) []metrics.Series {
	const points, total = 36, 1000.0
	ok, client, server := make([]float64, points), make([]float64, points), make([]float64, points)
	for i := range points {
		share := clientShare
		if i >= points-failing {
			share = failShare
		}
		client[i], server[i] = share*total, serverShare*total
		ok[i] = total - client[i] - server[i]
	}
	return []metrics.Series{history(pkgRoute, "200", ok...), history(pkgRoute, "404", client...), history(pkgRoute, "500", server...), history(webRoute, "200", steady(400, points)...)}
}

// traffic is five minutes of 30 s windows of 300 requests on the route with
// the given client and server errors per window.
func traffic(clientErrors, serverErrors float64) []metrics.Series {
	return []metrics.Series{
		series(pkgRoute, "200", steady(300-clientErrors-serverErrors, 60)...),
		series(pkgRoute, "404", steady(clientErrors, 60)...),
		series(pkgRoute, "500", steady(serverErrors, 60)...),
		series(webRoute, "200", steady(40, 60)...),
	}
}

func heldWithHistory(recent, baseline []metrics.Series, key string) sdktest.Snapshot {
	s := snapshot(recent, baseline, presence())
	s.HeldKeys = []string{key}
	return s
}

// A held finding recovers once the route answers its own healthy mix again,
// however large its routine client-error share is: a route that always
// answers 8% client errors never sees a window under a fixed 5%.
func TestHeldFindingRecoversAtTheRoutesOwnClientErrorShare(t *testing.T) {
	for _, d := range []struct {
		name string
		key  string
		det  func(*testing.T, sdktest.Snapshot) []string
	}{
		{"declared", pkgErrorsKey, func(t *testing.T, s sdktest.Snapshot) []string { return fingerprints(detect(t, s)) }},
		{"undeclared", undeclaredPkgErrorsKey, func(t *testing.T, s sdktest.Snapshot) []string {
			return fingerprints(detectWith(t, undeclaredDetector(0), s))
		}},
	} {
		// Two hours of the six were the failure (40% client errors).
		baseline := routeHistory(0.08, 0, 0.40, 12)
		if got := d.det(t, heldWithHistory(traffic(24, 0), baseline, d.key)); len(got) != 0 {
			t.Fatalf("%s: routine 8%% client errors hold a recovered route's incident: %v", d.name, got)
		}
		if got := d.det(t, heldWithHistory(traffic(150, 0), baseline, d.key)); len(got) != 1 {
			t.Fatalf("%s: 50%% client errors against a typical 8%% cleared the held finding: %v", d.name, got)
		}
	}
}

// Routine server errors are part of a route's healthy mix too; errors above it
// keep the finding.
func TestHeldFindingRecoversAtTheRoutesOwnServerErrorShare(t *testing.T) {
	baseline := routeHistory(0.03, 0.01, 0.03, 0)
	if got := detect(t, heldWithHistory(traffic(9, 3), baseline, pkgErrorsKey)); len(got) != 0 {
		t.Fatalf("routine 1%% server errors hold a recovered route's incident: %+v", got)
	}
	if got := detect(t, heldWithHistory(traffic(9, 30), baseline, pkgErrorsKey)); len(got) != 1 {
		t.Fatalf("10%% server errors against a typical 1%% cleared the held finding: %+v", got)
	}
	got := detect(t, heldWithHistory(traffic(9, 6), baseline, pkgErrorsKey))
	if len(got) != 1 {
		t.Fatalf("2%% server errors against a typical 1%% cleared the held finding: %+v", got)
	}
	if ev := got[0].Evidence; !strings.Contains(ev, "server errors under 1.0%") {
		t.Fatalf("evidence does not name the route's own server-error share: %q", ev)
	}
}

// A failure that fills most of the history becomes the route's typical share
// only then; with too little history the held finding keeps the strict
// judgement, so a route that always answers 8% client errors stays held.
func TestHeldFindingWithoutEnoughHistoryKeepsTheFixedAllowance(t *testing.T) {
	short := routeHistory(0.08, 0, 0.08, 0)
	for i := range short {
		short[i].Samples = short[i].Samples[len(short[i].Samples)-3:]
	}
	if got := detect(t, heldWithHistory(traffic(24, 0), short, pkgErrorsKey)); len(got) != 1 {
		t.Fatalf("three history points were trusted as the route's typical share: %+v", got)
	}
	// Five of six hours of failure are the failure's own typical share.
	if got := detect(t, heldWithHistory(traffic(120, 0), routeHistory(0.08, 0, 0.40, 30), pkgErrorsKey)); len(got) != 0 {
		t.Fatalf("a failure filling most of the history is not the typical share: %+v", got)
	}
}

// The raise rule's baseline is the history's newest point: an old point of a
// route nobody called lately is not part of it.
func TestRaiseBaselineIsTheNewestHistoryPoint(t *testing.T) {
	old := history(pkgRoute, "404", 600, 0)
	old.Samples = old.Samples[:1]
	baseline := []metrics.Series{history(pkgRoute, "200", 40, 400), old, history(webRoute, "200", 400, 400)}
	got := detect(t, snapshot(failingFor(6), baseline, presence()))
	if len(got) != 1 || got[0].Fingerprint != "requestmetrics/errors/package-download" {
		t.Fatalf("an old history point hid a client-error jump: %+v", got)
	}
}

func fingerprints(findings []sdk.Finding) []string {
	out := make([]string, 0, len(findings))
	for _, f := range findings {
		out = append(out, f.Fingerprint)
	}
	return out
}
