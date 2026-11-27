package requestmetrics_test

import (
	"strings"
	"testing"

	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/sdktest"
)

const undeclaredPkgErrorsKey = "request-metrics\x00requestmetrics/errors/undeclared/web/" + pkgRoute

// outageBaseline has absorbed a server-error outage on the route.
func outageBaseline() []metrics.Series {
	return []metrics.Series{instant(pkgRoute, "200", 300), instant(pkgRoute, "404", 9), instant(pkgRoute, "500", 120), instant(webRoute, "200", 400)}
}

// routineClientErrors is five minutes of a route whose healthy traffic always
// carries about 3% client errors, with serverErrors 5xx per window.
func routineClientErrors(serverErrors float64) []metrics.Series {
	return []metrics.Series{
		series(pkgRoute, "200", steady(97, 60)...),
		series(pkgRoute, "404", steady(3, 60)...),
		series(pkgRoute, "500", steady(serverErrors, 60)...),
		series(webRoute, "200", steady(40, 60)...),
	}
}

func heldSnapshot(recent []metrics.Series, key string) sdktest.Snapshot {
	s := snapshot(recent, outageBaseline(), presence())
	s.HeldKeys = []string{key}
	return s
}

// A held finding is recovered when the failure that opened it is gone, not
// when the route stops answering the client errors its healthy traffic
// always has: such a route never sees a window without one.
func TestHeldFindingClearsOnRoutineClientErrorsOnceServerErrorsStop(t *testing.T) {
	if got := detect(t, heldSnapshot(routineClientErrors(0), pkgErrorsKey)); len(got) != 0 {
		t.Fatalf("routine client errors hold a recovered route's incident open: %+v", got)
	}
}

func TestHeldFindingStaysWhileServerErrorsContinue(t *testing.T) {
	for _, server := range []float64{2, 12} {
		got := detect(t, heldSnapshot(routineClientErrors(server), pkgErrorsKey))
		if len(got) != 1 || got[0].Fingerprint != "requestmetrics/errors/package-download" {
			t.Fatalf("%v server errors per window cleared the held finding: %+v", server, got)
		}
	}
}

func TestHeldUndeclaredRouteClearsOnRoutineClientErrors(t *testing.T) {
	if got := detectWith(t, undeclaredDetector(0), heldSnapshot(routineClientErrors(0), undeclaredPkgErrorsKey)); len(got) != 0 {
		t.Fatalf("routine client errors hold an undeclared route's incident open: %+v", got)
	}
	got := detectWith(t, undeclaredDetector(0), heldSnapshot(routineClientErrors(2), undeclaredPkgErrorsKey))
	if len(got) != 1 {
		t.Fatalf("continuing server errors cleared the held undeclared route: %+v", got)
	}
	if ev := got[0].Evidence; !strings.Contains(ev, "recovery is not confirmed") || !strings.Contains(ev, "undeclared route") {
		t.Fatalf("evidence drops the held reason or the undeclared note: %q", ev)
	}
}

// A sparse route's lookback is recovered with its routine share of client
// errors too.
func TestHeldSparseRouteClearsWithRoutineClientErrors(t *testing.T) {
	ok, bad := steady(1, 60), steady(0, 60)
	ok[20], bad[20] = 0, 1
	recent := []metrics.Series{series(pkgRoute, "200", ok...), series(pkgRoute, "404", bad...), series(webRoute, "200", steady(40, 60)...)}
	if got := detect(t, heldSnapshot(recent, pkgErrorsKey)); len(got) != 0 {
		t.Fatalf("one routine client error in a sparse lookback holds the incident: %+v", got)
	}
}
