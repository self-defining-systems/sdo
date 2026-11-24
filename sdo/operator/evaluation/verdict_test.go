package evaluation

import "testing"

// Only the acceptance signal waits: an unavailable user-test probe keeps a
// verdict from clearing; any other detector that cannot be read in this
// environment (resource usage, a clue, request metrics, a Kubernetes
// condition read, a detector left out outside the cluster) is a coverage gap.
func TestOnlyAnUnavailableUserTestHoldsAVerdict(t *testing.T) {
	cases := map[string]bool{
		"traffic-probe-write":  true,
		"traffic-links":        true,
		"resource-saturation":  false,
		"cpu-throttling":       false,
		"request-metrics":      false,
		"workload-health":      false,
		"service-reachability": false,
		"traffic":              false,
		"":                     false,
	}
	for id, want := range cases {
		if got := UnavailableHolds(UnavailableDetector{DetectorID: id, Reason: "resource usage: CFS counters unavailable for p/c"}); got != want {
			t.Errorf("UnavailableHolds(%q) = %v, want %v", id, got, want)
		}
	}
}

// The status a verdict reports for its unavailable detectors follows the same
// rule, so the producer and every reader agree.
func TestStatusOfUnavailableFollowsTheRule(t *testing.T) {
	gap := UnavailableDetector{DetectorID: "resource-saturation", Reason: "resource usage: CFS counters unavailable for p/c"}
	user := UnavailableDetector{DetectorID: "traffic-probe-call", Reason: "traffic prober unavailable: connection refused"}
	if got := StatusWithUnavailable(Clear, []UnavailableDetector{gap}); got != Clear {
		t.Fatalf("clear with a coverage gap = %q, want clear", got)
	}
	if got := StatusWithUnavailable(Pending, []UnavailableDetector{gap}); got != Pending {
		t.Fatalf("pending with a coverage gap = %q, want pending", got)
	}
	if got := StatusWithUnavailable(Clear, []UnavailableDetector{gap, user}); got != Unavailable {
		t.Fatalf("clear with an unavailable user test = %q, want unavailable", got)
	}
	if got := StatusWithUnavailable(Pending, []UnavailableDetector{user}); got != Unavailable {
		t.Fatalf("pending with an unavailable user test = %q, want unavailable", got)
	}
}
