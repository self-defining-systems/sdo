package runtime

import (
	"testing"
	"time"
)

// Every healthy history settles by one rule: a stretch settles only after
// health has lasted the settle period, and an unhealthy evaluation discards it.
func TestHealthyStretchSettlesOnlyAfterTheSettlePeriod(t *testing.T) {
	at := time.Unix(1_000, 0)
	s := newHealthyStretch(time.Minute)
	steps := []struct {
		offset  time.Duration
		healthy bool
		want    stretchEvent
	}{
		{0, true, stretchStarted},
		{30 * time.Second, true, stretchGrowing},
		{40 * time.Second, false, stretchBroken},
		{50 * time.Second, true, stretchStarted},
		{110 * time.Second, true, stretchSettled},
		{120 * time.Second, true, stretchGrowing},
	}
	for _, step := range steps {
		got, from := s.observe(at.Add(step.offset), step.healthy)
		if got != step.want {
			t.Fatalf("at %s: event %d, want %d", step.offset, got, step.want)
		}
		if got == stretchSettled && !from.Equal(at.Add(50*time.Second)) {
			t.Fatalf("settled stretch began at %s, want the restart after the unhealthy evaluation", from.Sub(at))
		}
	}
}

func TestSpreadByTimeKeepsTheEndsAndDropsTheDensest(t *testing.T) {
	at := time.Unix(0, 0)
	offsets := []int{0, 10, 11, 20, 30}
	var times []time.Time
	for _, o := range offsets {
		times = append(times, at.Add(time.Duration(o)*time.Second))
	}
	got := spreadByTime(times, func(t time.Time) time.Time { return t }, 4)
	want := []int{0, 10, 20, 30}
	if len(got) != len(want) {
		t.Fatalf("kept %d, want %d", len(got), len(want))
	}
	for i, w := range want {
		if !got[i].Equal(at.Add(time.Duration(w) * time.Second)) {
			t.Fatalf("kept %v, want offsets %v", got, want)
		}
	}
}

// The client-link and platform baselines promote the value seen at the start
// of a healthy stretch only once it has lasted the settle period; an unhealthy
// evaluation discards the candidate, so a fault is never absorbed.
func TestSettledBaselinePromotesTheStartOfASettledStretch(t *testing.T) {
	at := time.Unix(1_000, 0)
	b := newSettledBaseline[string](time.Minute)
	if _, ok := b.Current(); ok {
		t.Fatal("a baseline before any evaluation")
	}
	b.Observe(at, true, "first")
	b.Observe(at.Add(30*time.Second), true, "drifting")
	if got, ok := b.Current(); ok {
		t.Fatalf("promoted %q before the stretch settled", got)
	}
	b.Observe(at.Add(time.Minute), true, "next")
	if got, _ := b.Current(); got != "first" {
		t.Fatalf("promoted %q, want the value at the start of the stretch", got)
	}
	b.Observe(at.Add(90*time.Second), false, "fault")
	b.Observe(at.Add(100*time.Second), true, "again")
	b.Observe(at.Add(150*time.Second), true, "x")
	if got, _ := b.Current(); got != "first" {
		t.Fatalf("a candidate promoted across an unhealthy evaluation: %q", got)
	}
	b.Observe(at.Add(160*time.Second), true, "y")
	if got, _ := b.Current(); got != "again" {
		t.Fatalf("second promotion %q, want the restart after the fault", got)
	}
}
