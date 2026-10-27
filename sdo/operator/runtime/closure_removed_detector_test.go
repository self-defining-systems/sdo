package runtime

import (
	"context"
	"reflect"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
)

// A detector the adopted policy removed is not evidence about the cluster. The
// closure is rechecked by the detectors in force, the skipped IDs are reported,
// and a recheck with nothing left to evaluate fails closed.
func TestRecheckSkipsDetectorRemovedByAdoptedPolicy(t *testing.T) {
	for _, tc := range []struct {
		name          string
		healthFinding sdk.Finding
		dropHealth    bool
		wantVerdict   closureRecheckVerdict
		wantSkipped   []string
	}{
		{name: "clean health confirms", wantVerdict: closureRecheckConfirmed, wantSkipped: []string{"removed"}},
		{name: "active health contradicts", healthFinding: stateFinding("health"), wantVerdict: closureRecheckContradicted},
		{name: "nothing left contradicts", dropHealth: true, wantVerdict: closureRecheckContradicted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Unix(1000, 0).UTC()
			c := newClosureRetryController(t, ClosureRetryPolicy{}, &recordingIncidentBroker{}, &now)
			c.detectors = map[string]sdk.Detector{"health": controllerDetector("health", time.Hour, tc.healthFinding)}
			if tc.dropHealth {
				delete(c.detectors, "health")
			}
			closure := *c.pendingClosure
			closure.FinalDetectorStates = []DetectorEvaluation{
				{DetectorID: "removed", Status: DetectorEvaluationClear},
				{DetectorID: "health", Status: DetectorEvaluationClear},
			}
			snapshot, err := c.provider.Snapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			got := c.revalidateClosure(context.Background(), snapshot, closure)
			if got.Verdict != tc.wantVerdict {
				t.Fatalf("verdict %v (%s), want %v", got.Verdict, got.Reason, tc.wantVerdict)
			}
			if !reflect.DeepEqual(got.Skipped, tc.wantSkipped) {
				t.Fatalf("skipped %v, want %v", got.Skipped, tc.wantSkipped)
			}
		})
	}
}
