package traffic

import (
	"testing"
	"time"

	"sdo.dev/operator/sdk"
)

// stalled marks every sample as dialed while the host's CPU stall was
// percent, and turns each failed sample into a dial timeout.
func stalled(samples []LinkSample, percent float64) []LinkSample {
	for index := range samples {
		samples[index].HostCPUStall = percent
		if !samples[index].OK {
			samples[index].Class, samples[index].Error = LinkFailureTimeout, "dial tcp: i/o timeout"
		}
	}
	return samples
}

func timeouts(indices ...int) map[int]bool {
	failed := map[int]bool{}
	for _, index := range indices {
		failed[index] = true
	}
	return failed
}

// Shapes seen on a healthy application while its host's CPU was stalled 35-90%
// of the time: a heavy handshake (a database backend, an HTTP worker) times out
// in recurring bursts or for several seconds in a row, and its served dials
// slow down, while light targets dialed from the same place stay fast. A stalled
// host explains timeouts and slowness; they are judged against it, not raised as
// one link's fault.
func TestLinkFindingJudgesTimeoutsAndSlownessAgainstAStalledHost(t *testing.T) {
	now := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	for name, samples := range map[string][]LinkSample{
		"recurring timeout bursts": stalled(linkSamples(now, repeatDuration(80*time.Millisecond, 20), timeouts(2, 3, 4, 5, 6, 7, 12, 13, 14, 15, 16, 17)), 40),
		"seconds of timeouts":      stalled(linkSamples(now, repeatDuration(60*time.Millisecond, 20), timeouts(11, 12, 13, 14, 15, 16, 17, 18, 19)), 40),
		"slow heavy handshake":     stalled(linkSamples(now, repeatDuration(287*time.Millisecond, 20), nil), 40),
	} {
		window, observed := linkTestWindow(now, 5*time.Millisecond, samples)
		if finding, firing := LinkFinding(sdk.DetectorSpec{ID: "traffic-links"}, "app", window, observed); firing {
			t.Errorf("%s: reported on a stalled host: %s", name, finding.Summary)
		}
	}
}

// What a stalled host cannot explain stays a link finding: refused or reset
// dials, a target that times out for a whole loss window, and the same
// timeouts and slowness on a host that is not stalled.
func TestLinkFindingKeepsFaultsAStalledHostDoesNotExplain(t *testing.T) {
	now := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	refused := stalled(linkSamples(now, repeatDuration(5*time.Millisecond, 20), nil), 40)
	for _, index := range []int{15, 16, 17, 18, 19} {
		refused[index].OK, refused[index].Class, refused[index].Error = false, LinkFailureRefused, "connection refused"
	}
	everyDial := make(map[int]bool, LinkLossWindow)
	for index := range LinkLossWindow {
		everyDial[index] = true
	}
	for name, test := range map[string]struct {
		samples []LinkSample
		want    LinkCondition
	}{
		"refused on a stalled host":         {refused, LinkUnreachable},
		"a whole window of timeouts":        {stalled(linkSamples(now, repeatDuration(time.Second, LinkLossWindow), everyDial), 40), LinkUnreachable},
		"timeout bursts on a calm host":     {stalled(linkSamples(now, repeatDuration(5*time.Millisecond, 20), timeouts(2, 3, 4, 12, 13, 14)), 5), LinkIntermittent},
		"seconds of timeouts, calm host":    {stalled(linkSamples(now, repeatDuration(5*time.Millisecond, 20), timeouts(15, 16, 17, 18, 19)), 0), LinkUnreachable},
		"slow link on a calm host":          {stalled(linkSamples(now, repeatDuration(287*time.Millisecond, 20), nil), 5), LinkSlow},
	} {
		window, observed := linkTestWindow(now, 5*time.Millisecond, test.samples)
		finding, firing := LinkFinding(sdk.DetectorSpec{ID: "traffic-links"}, "app", window, observed)
		if !firing || finding.Metadata["condition"] != string(test.want) {
			t.Errorf("%s: firing=%v condition=%v, want %s", name, firing, finding.Metadata["condition"], test.want)
		}
	}
}
