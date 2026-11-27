package runtime

import (
	"testing"

	"sdo.dev/operator/sdk"
)

// driftFindings are the drift findings that link the history into a request.
func driftFindings(c *Controller) []sdk.Finding {
	var findings []sdk.Finding
	for i := range c.driftEvents {
		c.driftEvents[i].Classification = sdk.DriftUnexplained
		event := c.driftEvents[i]
		findings = append(findings, sdk.Finding{DetectorID: "drift", Status: sdk.FindingActive, Metadata: map[string]any{"drift_event": event}})
	}
	return findings
}

// A drift event counts as seen healthy once the application stayed healthy for
// a whole settle window that began after the change was observed: not before,
// and never while it fails. The mark reaches the incident request and survives
// a restart.
func TestDriftPresentThroughAHealthySettleIsSeenHealthy(t *testing.T) {
	probe := probeSamples(400, func(int) bool { return false })
	c := startupController(t, 0, 0, probe)
	c.driftEvents = []sdk.DriftEvent{
		departureEvent(dref("Service", "old"), "kubectl", at(0), "spec"),
		departureEvent(dref("Service", "recent"), "kubectl", at(5), "spec"),
	}
	seen := func(c *Controller) map[string]bool {
		marks := map[string]bool{}
		for _, event := range c.incidentRequest(at(500), driftFindings(c)).DriftEvents {
			marks[event.Ref.Name] = event.SeenHealthy
		}
		return marks
	}
	for second := int64(0); second < 120; second++ {
		step(t, c, second)
	}
	if got := seen(c); got["old"] || got["recent"] {
		t.Fatalf("marked before a settle window passed: %v", got)
	}
	step(t, c, 120)
	if got := seen(c); !got["old"] || got["recent"] {
		t.Fatalf("after the first settled window %v, want only old", got)
	}
	driftFindings(c) // classifies the history the state stores
	state, err := c.ExportState()
	if err != nil {
		t.Fatal(err)
	}
	restarted := startupController(t, 0, 130, probe)
	if err := restarted.RestoreState(state); err != nil {
		t.Fatal(err)
	}
	if got := seen(restarted); !got["old"] || got["recent"] {
		t.Fatalf("after a restart %v, want only old", got)
	}
	for second := int64(121); second <= 240; second++ {
		step(t, c, second)
	}
	if got := seen(c); !got["recent"] {
		t.Fatalf("after the next settled window %v, want recent too", got)
	}
}

func TestDriftIsNotSeenHealthyWhileTheApplicationFails(t *testing.T) {
	c := startupController(t, 0, 0, probeSamples(300, func(int) bool { return true }))
	c.driftEvents = []sdk.DriftEvent{departureEvent(dref("Service", "api"), "kubectl", at(0), "spec")}
	for second := int64(0); second <= 250; second++ {
		step(t, c, second)
	}
	events := c.incidentRequest(at(500), driftFindings(c)).DriftEvents
	if len(events) != 1 {
		t.Fatalf("events %v", events)
	}
	for _, event := range events {
		if event.SeenHealthy {
			t.Fatalf("a failing application marked %s seen healthy", event.Ref.Name)
		}
	}
}
