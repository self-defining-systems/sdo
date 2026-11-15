package runtime

import (
	"slices"
	"testing"
	"time"

	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
)

// Recovered depends only on the incident's alert set. A health detector that
// owns none of its alerts (here a traffic probe pending for lack of samples)
// is not evidence about this incident and never holds its recovery.
func TestUnrelatedHealthDetectorNeverHoldsRecovery(t *testing.T) {
	c := openersController(t, &recordingDispatcher{})
	opener := failingOperation("requestmetrics/errors/browse-items")
	opener.DetectorID = memory.RequestMetricsDetectorID
	c.healthDetectorIDs = []string{"traffic-probe-call", memory.RequestMetricsDetectorID, "workload-health"}
	request := IncidentRequest{IncidentID: "i", Findings: []sdk.Finding{opener}, OpeningKeys: findingKeys([]sdk.Finding{opener})}
	c.incidentOpen, c.currentIncidentRequest = true, &request

	if got := c.recoveryDetectorIDs(request); !slices.Equal(got, []string{memory.RequestMetricsDetectorID}) {
		t.Fatalf("recovery waits on detectors %v, want only the opening alert's detector", got)
	}
	since := time.Unix(100, 0).UTC()
	c.history = []DetectorEvaluation{
		{DetectorID: memory.RequestMetricsDetectorID, EvaluatedAt: since.Add(time.Second), Status: DetectorEvaluationClear},
		{DetectorID: "traffic-probe-call", EvaluatedAt: since.Add(time.Second), Status: DetectorEvaluationPending, Reason: "insufficient usable synthetic samples"},
		{DetectorID: "workload-health", EvaluatedAt: since.Add(time.Second), Status: DetectorEvaluationError, Error: "unavailable"},
	}
	c.detectorClearSince = map[string]time.Time{memory.RequestMetricsDetectorID: since}
	c.incidentDispatchedAt, c.responderDone, c.responderCompletedAt = since, true, since
	if _, ok := c.finalVerificationStates(); !ok {
		t.Fatal("an unrelated health detector that is not clear held the incident open")
	}

	// The opening alert's own detector still holds recovery.
	c.history[0].Status = DetectorEvaluationPending
	if _, ok := c.finalVerificationStates(); ok {
		t.Fatal("the opening alert's detector pending did not hold recovery")
	}
}

// A user test that joined the alert set brings its detector into scope.
func TestJoinedAlertDetectorIsInRecoveryScope(t *testing.T) {
	c := openersController(t, &recordingDispatcher{})
	c.healthDetectorIDs = []string{"traffic-probe-call", "traffic-probe-write", memory.RequestMetricsDetectorID}
	request := IncidentRequest{
		OpeningKeys: []string{FindingStateKey(memory.RequestMetricsDetectorID, "a")},
		JoinedKeys:  []string{FindingStateKey("traffic-probe-write", "b")},
	}
	want := []string{memory.RequestMetricsDetectorID, "traffic-probe-write"}
	if got := c.recoveryDetectorIDs(request); !slices.Equal(got, want) {
		t.Fatalf("recovery detectors = %v, want %v", got, want)
	}
}
