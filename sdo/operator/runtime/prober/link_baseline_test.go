package prober

import (
	"testing"
	"time"

	"sdo.dev/operator/sdk/traffic"
)

func TestLinkBaselineIsMedianOfFirstSuccessfulDialsAndThenFixed(t *testing.T) {
	state := &linkState{link: traffic.Link{From: "web", To: "store", Port: 5432}}
	state.record(traffic.LinkSample{OK: false, Duration: time.Second}, 64)
	for index := range traffic.LinkBaselineSamples {
		state.record(traffic.LinkSample{OK: true, Duration: time.Duration(index+1) * time.Millisecond}, 64)
	}
	if !state.qualified || state.baseline != 5*time.Millisecond {
		t.Fatalf("qualified=%v baseline=%s, want true and the 5ms median of successful dials", state.qualified, state.baseline)
	}
	for range 30 {
		state.record(traffic.LinkSample{OK: true, Duration: 2 * time.Second}, 64)
	}
	if state.baseline != 5*time.Millisecond {
		t.Fatalf("a later slow period moved the healthy baseline to %s", state.baseline)
	}
	window := (&Prober{}).linkWindowLocked(&linkProbe{links: []*linkState{state}}, time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC))
	if got := window.Links[0].BaselineLatency; got != 5*time.Millisecond {
		t.Fatalf("window baseline = %s", got)
	}
}

func TestLinkBaselineRecordsSpreadOfHealthyDials(t *testing.T) {
	state := &linkState{link: traffic.Link{From: "web", To: "store", Port: 5432}}
	for _, ms := range []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 30} {
		state.record(traffic.LinkSample{OK: true, Duration: time.Duration(ms) * time.Millisecond}, 64)
	}
	// Median 5ms; absolute deviations 4,3,2,1,0,1,2,3,4,25 have median 2ms.
	if state.baseline != 5*time.Millisecond || state.spread != 2*time.Millisecond {
		t.Fatalf("baseline=%s spread=%s, want 5ms and 2ms", state.baseline, state.spread)
	}
	window := (&Prober{}).linkWindowLocked(&linkProbe{links: []*linkState{state}}, time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC))
	if got := window.Links[0].BaselineSpread; got != 2*time.Millisecond {
		t.Fatalf("window spread = %s", got)
	}
}
