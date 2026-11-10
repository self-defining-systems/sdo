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
