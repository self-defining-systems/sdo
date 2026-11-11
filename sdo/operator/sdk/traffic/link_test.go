package traffic

import (
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
)

func linkTestWindow(now time.Time, baseline time.Duration, samples []LinkSample) (Window, LinkObservations) {
	observed := LinkObservations{
		Link:    Link{From: "web", To: "store", Port: 5432, Protocol: LinkProtocolPostgres},
		Samples: samples, Qualified: true, BaselineLatency: baseline,
	}
	window := Window{
		Workload:   Workload{Name: "links", Purpose: PurposeLinkProbe, Interval: Duration(time.Second), Timeout: Duration(3 * time.Second), Failures: 5},
		ObservedAt: now, Links: []LinkObservations{observed},
	}
	return window, observed
}

// linkSamples builds one sample per second ending at now; failed[i] marks the
// i-th sample (oldest first) as a refused dial.
func linkSamples(now time.Time, durations []time.Duration, failed map[int]bool) []LinkSample {
	samples := make([]LinkSample, 0, len(durations))
	for index, duration := range durations {
		sample := LinkSample{At: now.Add(-time.Duration(len(durations)-1-index) * time.Second), Duration: duration, OK: !failed[index]}
		if failed[index] {
			sample.Class, sample.Error = LinkFailureRefused, "connection refused"
		}
		samples = append(samples, sample)
	}
	return samples
}

func repeatDuration(value time.Duration, count int) []time.Duration {
	result := make([]time.Duration, count)
	for index := range result {
		result[index] = value
	}
	return result
}

func TestLinkFindingReportsSlowLinkAgainstHealthyBaseline(t *testing.T) {
	now := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	window, observed := linkTestWindow(now, 5*time.Millisecond, linkSamples(now, repeatDuration(2*time.Second, 20), nil))
	finding, firing := LinkFinding(sdk.DetectorSpec{ID: "traffic-links"}, "app", window, observed)
	if !firing {
		t.Fatal("a link answering every dial in 2s against a 5ms healthy baseline is not reported")
	}
	if finding.Metadata["condition"] != string(LinkSlow) {
		t.Fatalf("condition = %v, want %s", finding.Metadata["condition"], LinkSlow)
	}
	if finding.Metadata["p50_ns"] != int64(2*time.Second) || finding.Metadata["baseline_ns"] != int64(5*time.Millisecond) {
		t.Fatalf("missing p50/baseline evidence: %v", finding.Metadata)
	}
	if !strings.Contains(finding.Evidence, "p50") || !strings.Contains(finding.Summary, "slow") {
		t.Fatalf("finding does not explain the latency: %q / %q", finding.Summary, finding.Evidence)
	}
	if finding.RuleID != LinkRuleIDPrefix+observed.Link.ID() {
		t.Fatalf("rule ID %q changed for a slow link", finding.RuleID)
	}
}

func TestLinkFindingReportsIntermittentLossWithoutConsecutiveFailures(t *testing.T) {
	now := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	// 6 of 20 fail, never 3 in a row.
	failed := map[int]bool{1: true, 2: true, 6: true, 10: true, 11: true, 16: true}
	window, observed := linkTestWindow(now, 5*time.Millisecond, linkSamples(now, repeatDuration(5*time.Millisecond, 20), failed))
	if _, failing := LinkFailing(window, observed); failing {
		t.Fatal("fixture must not be a consecutive-failure run")
	}
	finding, firing := LinkFinding(sdk.DetectorSpec{ID: "traffic-links"}, "app", window, observed)
	if !firing {
		t.Fatal("a link failing 6 of its last 20 dials is not reported")
	}
	if finding.Metadata["condition"] != string(LinkIntermittent) || finding.Metadata["failed"] != 6 || finding.Metadata["samples"] != 20 {
		t.Fatalf("metadata = %v", finding.Metadata)
	}
	if !strings.Contains(finding.Summary, "intermittent") {
		t.Fatalf("summary = %q", finding.Summary)
	}
}

// One stall of a few seconds (a slow node, a DNS timeout burst) fails a run of
// dials, possibly with a stray success inside it, then the link recovers. It
// is neither unreachable (no run of Failures consecutive failures) nor
// intermittent: the failures are one event, not a recurring loss.
func TestLinkFindingIgnoresOneStall(t *testing.T) {
	now := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	for name, failed := range map[string]map[int]bool{
		"four consecutive failures":           {8: true, 9: true, 10: true, 11: true},
		"six failures with one success inside": {8: true, 9: true, 11: true, 12: true, 13: true, 14: true},
	} {
		t.Run(name, func(t *testing.T) {
			window, observed := linkTestWindow(now, 5*time.Millisecond, linkSamples(now, repeatDuration(5*time.Millisecond, 20), failed))
			if _, failing := LinkFailing(window, observed); failing {
				t.Fatal("fixture must not be a consecutive-failure run")
			}
			if finding, firing := LinkFinding(sdk.DetectorSpec{ID: "traffic-links"}, "app", window, observed); firing {
				t.Fatalf("one stall reported: %s", finding.Summary)
			}
		})
	}
}

// Stalls that recur within the window are an intermittent link.
func TestLinkFindingReportsRecurringStalls(t *testing.T) {
	now := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	failed := map[int]bool{2: true, 3: true, 4: true, 5: true, 14: true, 15: true, 16: true, 17: true}
	window, observed := linkTestWindow(now, 5*time.Millisecond, linkSamples(now, repeatDuration(5*time.Millisecond, 20), failed))
	finding, firing := LinkFinding(sdk.DetectorSpec{ID: "traffic-links"}, "app", window, observed)
	if !firing || finding.Metadata["condition"] != string(LinkIntermittent) {
		t.Fatalf("recurring stalls not reported intermittent: firing=%v metadata=%v", firing, finding.Metadata)
	}
}

func TestLinkFindingIgnoresHealthyJitterAndRareLoss(t *testing.T) {
	now := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	durations := make([]time.Duration, 0, 20)
	for index := range 20 {
		// 3ms..45ms, with occasional spikes, all within 5x baseline or the 50ms floor.
		durations = append(durations, time.Duration(3+(index*7)%43)*time.Millisecond)
	}
	durations[7] = 400 * time.Millisecond
	durations[15] = 300 * time.Millisecond
	window, observed := linkTestWindow(now, 5*time.Millisecond, linkSamples(now, durations, map[int]bool{4: true, 12: true, 13: true}))
	if finding, firing := LinkFinding(sdk.DetectorSpec{ID: "traffic-links"}, "app", window, observed); firing {
		t.Fatalf("healthy jitter reported: %s", finding.Summary)
	}
}

func TestLinkFindingNeedsBaselineForLatency(t *testing.T) {
	now := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	window, observed := linkTestWindow(now, 0, linkSamples(now, repeatDuration(2*time.Second, 20), nil))
	if _, firing := LinkFinding(sdk.DetectorSpec{ID: "traffic-links"}, "app", window, observed); firing {
		t.Fatal("latency judged without a healthy baseline")
	}
}

func TestLinkFindingKeepsUnreachableAheadOfLoss(t *testing.T) {
	now := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	failed := map[int]bool{}
	for index := 13; index < 20; index++ {
		failed[index] = true
	}
	window, observed := linkTestWindow(now, 5*time.Millisecond, linkSamples(now, repeatDuration(5*time.Millisecond, 20), failed))
	finding, firing := LinkFinding(sdk.DetectorSpec{ID: "traffic-links"}, "app", window, observed)
	if !firing || finding.Metadata["condition"] != string(LinkUnreachable) || finding.Metadata["consecutive_failures"] != 5 {
		t.Fatalf("firing=%v metadata=%v", firing, finding.Metadata)
	}
}

func TestLinkFindingIgnoresStaleSamples(t *testing.T) {
	now := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	window, observed := linkTestWindow(now, 5*time.Millisecond, linkSamples(now.Add(-time.Hour), repeatDuration(2*time.Second, 20), nil))
	if _, firing := LinkFinding(sdk.DetectorSpec{ID: "traffic-links"}, "app", window, observed); firing {
		t.Fatal("stale samples judged")
	}
}

// multiLinkWindow builds a link-probe window of several links sampled at the
// same instants; durations[i] are the i-th link's dials, oldest first.
func multiLinkWindow(now time.Time, baseline time.Duration, durations ...[]time.Duration) Window {
	window, _ := linkTestWindow(now, baseline, nil)
	window.Links = nil
	for index, series := range durations {
		window.Links = append(window.Links, LinkObservations{
			Link:    Link{From: "web", To: "store" + string(rune('a'+index)), Port: 5432},
			Samples: linkSamples(now, series, nil), Qualified: true, BaselineLatency: baseline,
		})
	}
	return window
}

func inflated(count int) []time.Duration {
	durations := make([]time.Duration, count)
	for index := range durations {
		durations[index] = time.Duration(75+(index*13)%44) * time.Millisecond
	}
	return durations
}

// A loaded prober node slows every fresh dial alike. That is host slowness,
// not a link fault, so no link is reported.
func TestLinkFindingDiscountsSlownessSharedByEveryLink(t *testing.T) {
	now := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	window := multiLinkWindow(now, 2*time.Millisecond, inflated(20), inflated(20), inflated(20))
	for _, observed := range window.Links {
		if finding, firing := LinkFinding(sdk.DetectorSpec{ID: "traffic-links"}, "app", window, observed); firing {
			t.Fatalf("uniform host slowness reported: %s", finding.Summary)
		}
	}
}

// One link far slower than its own baseline, while its peers dialled from
// the same prober stay at theirs, is reported once the slowness persists.
func TestLinkFindingReportsOneSlowLinkAfterPersistence(t *testing.T) {
	now := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	healthy := repeatDuration(2*time.Millisecond, 20)
	brief := append(repeatDuration(2*time.Millisecond, 15), repeatDuration(300*time.Millisecond, 5)...)
	window := multiLinkWindow(now, 2*time.Millisecond, healthy, brief, healthy)
	if finding, firing := LinkFinding(sdk.DetectorSpec{ID: "traffic-links"}, "app", window, window.Links[1]); firing {
		t.Fatalf("one window of slow dials reported without persistence: %s", finding.Summary)
	}
	persistent := append(repeatDuration(2*time.Millisecond, 5), repeatDuration(300*time.Millisecond, 15)...)
	window = multiLinkWindow(now, 2*time.Millisecond, healthy, persistent, healthy)
	finding, firing := LinkFinding(sdk.DetectorSpec{ID: "traffic-links"}, "app", window, window.Links[1])
	if !firing || finding.Metadata["condition"] != string(LinkSlow) {
		t.Fatalf("persistently slow link not reported: firing=%v metadata=%v", firing, finding.Metadata)
	}
	if finding.Severity != sdk.SeverityWarning {
		t.Fatalf("slow link severity = %s, want %s: a slow link still serves", finding.Severity, sdk.SeverityWarning)
	}
	for _, peer := range []LinkObservations{window.Links[0], window.Links[2]} {
		if _, firing := LinkFinding(sdk.DetectorSpec{ID: "traffic-links"}, "app", window, peer); firing {
			t.Fatalf("healthy peer %s reported", peer.Link)
		}
	}
}

// A link whose healthy dials already spread widely is judged against that
// spread, not only against a multiple of its median.
func TestLinkFindingAllowsForBaselineSpread(t *testing.T) {
	now := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	window, observed := linkTestWindow(now, 20*time.Millisecond, linkSamples(now, repeatDuration(120*time.Millisecond, 20), nil))
	observed.BaselineSpread = 15 * time.Millisecond
	window.Links[0] = observed
	if finding, firing := LinkFinding(sdk.DetectorSpec{ID: "traffic-links"}, "app", window, observed); firing {
		t.Fatalf("dials within the healthy spread reported: %s", finding.Summary)
	}
	window, observed = linkTestWindow(now, 20*time.Millisecond, linkSamples(now, repeatDuration(400*time.Millisecond, 20), nil))
	observed.BaselineSpread = 15 * time.Millisecond
	window.Links[0] = observed
	if _, firing := LinkFinding(sdk.DetectorSpec{ID: "traffic-links"}, "app", window, observed); !firing {
		t.Fatal("dials far beyond the healthy spread not reported")
	}
}

// A peer that is down times out on every dial; its failed dials are not host
// slowness and must not hide another link's real slowdown.
func TestLinkFindingIgnoresFailedPeerDialsForSharedSlowdown(t *testing.T) {
	now := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	window := multiLinkWindow(now, 2*time.Millisecond, repeatDuration(time.Second, 20), repeatDuration(300*time.Millisecond, 20))
	down := map[int]bool{}
	for index := range 20 {
		down[index] = true
	}
	window.Links[0].Samples = linkSamples(now, repeatDuration(time.Second, 20), down)
	if _, firing := LinkFinding(sdk.DetectorSpec{ID: "traffic-links"}, "app", window, window.Links[1]); !firing {
		t.Fatal("a down peer's timed-out dials discounted a slow link")
	}
}
