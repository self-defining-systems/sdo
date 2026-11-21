package requestmetrics_test

import (
	"testing"

	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/sdktest"
)

// A route that only ever sees a few requests per window can never show the
// healthy volume recovery asks for, and its traffic never stops, so a held
// finding on it would hold its incident open for as long as the route lives.
// A whole lookback with requests and not one failure is recovery.
func TestHeldFindingOnASparseRouteClearsOnceTheWholeLookbackIsFailureFree(t *testing.T) {
	sparse := func(steps int) sdktest.Snapshot {
		held := snapshot([]metrics.Series{series(pkgRoute, "200", steady(1, steps)...), series(webRoute, "200", steady(40, steps)...)}, pollutedBaseline(), presence())
		held.HeldKeys = []string{pkgErrorsKey}
		return held
	}
	if got := detect(t, sparse(60)); len(got) != 0 {
		t.Fatalf("a sparse route with no failure in its whole lookback still holds its incident: %+v", got)
	}
	if got := detect(t, sparse(12)); len(got) != 1 {
		t.Fatalf("recovery was confirmed from a minute of a five minute lookback: %+v", got)
	}
}
