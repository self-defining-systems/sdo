package traffic

import (
	"testing"
	"time"

	"sdo.dev/operator/sdk"
)

func heldFailures(window Window, spec sdk.DetectorSpec) []HeldFailure {
	return HeldFailures(spec, namespacedWindow{windowContext{window: window}}, "probe-read")
}

var heldSpec = sdk.DetectorSpec{ID: "traffic-probe-read", Class: sdk.DetectorClassHealth}

// The first error after successes is held, hard, from its own start; the
// finding it would report has the fingerprint of the one the hold reports
// later, so an early report and the later one are the same alert.
func TestHeldFailuresReportTheFirstHardFailure(t *testing.T) {
	ok := seconds(-3, -2, -1)
	held := heldFailures(holdWindow(ok, seconds(0)), heldSpec)
	if len(held) != 1 || !held[0].Hard || held[0].ScenarioID != "read" {
		t.Fatalf("held = %+v, want one hard failure of read", held)
	}
	if want := time.Unix(1000, 0); !held[0].FirstFailureAt.Equal(want) {
		t.Fatalf("first failure = %s, want %s", held[0].FirstFailureAt, want)
	}
	early := held[0].Finding("corroborated")
	later := holdFindings(t, holdWindow(ok, seconds(0, 4, 9)))
	if len(later) != 1 || sdk.FindingFingerprint(early) != sdk.FindingFingerprint(later[0]) {
		t.Fatalf("early %q and later %+v differ", sdk.FindingFingerprint(early), later)
	}
	if got := early.Metadata["violations"].([]string); got[len(got)-1] != "corroborated" {
		t.Fatalf("violations = %v, want the reason last", got)
	}
}

// Once the hold reports a scenario it is no longer held; a healthy one never is.
func TestHeldFailuresLeaveOutReportedAndHealthyScenarios(t *testing.T) {
	ok := seconds(-3, -2, -1)
	if held := heldFailures(holdWindow(ok, seconds(0, 4, 9)), heldSpec); len(held) != 0 {
		t.Fatalf("a reported failure is still held: %+v", held)
	}
	if held := heldFailures(holdWindow(seconds(-3, -2, -1, 0), nil), heldSpec); len(held) != 0 {
		t.Fatalf("a healthy scenario is held: %+v", held)
	}
	if held := heldFailures(holdWindow(seconds(-3, -2, -1, 1), seconds(0)), heldSpec); len(held) != 0 {
		t.Fatalf("a scenario whose newest sample succeeded is held: %+v", held)
	}
}

// A timeout or a slow success is not a hard failure: a data-plane stall
// produces exactly those.
func TestHeldFailuresTimeoutIsNotHard(t *testing.T) {
	window := holdWindow(seconds(-3, -2, -1), nil)
	window.Scenarios[0].Samples = append(window.Scenarios[0].Samples, Sample{At: time.Unix(1000, 0), Outcome: OutcomeTimeout, Error: "deadline"})
	window.ObservedAt = time.Unix(1000, 0)
	held := heldFailures(window, heldSpec)
	if len(held) != 1 || held[0].Hard {
		t.Fatalf("held = %+v, want one failure that is not hard", held)
	}
}

// A detector with its own MinDuration, or no health class, has no hold.
func TestHeldFailuresNeedTheDefaultHold(t *testing.T) {
	window := holdWindow(seconds(-3, -2, -1), seconds(0))
	own := heldSpec
	own.Persistence.MinDuration = time.Second
	for _, spec := range []sdk.DetectorSpec{own, {ID: "traffic-probe-read"}} {
		if held := heldFailures(window, spec); len(held) != 0 {
			t.Fatalf("spec %+v held %+v", spec, held)
		}
	}
}
