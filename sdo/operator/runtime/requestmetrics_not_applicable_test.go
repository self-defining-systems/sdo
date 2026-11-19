package runtime

import (
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/evaluation"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/requestmetrics"
)

// An application whose map declares no request metric for an operation's
// entry service has no request-metrics source for it. The generic detector Go
// installs then reports the operation as not applicable: the deployment
// gate's evaluation is clear with a coverage note naming it, and the install's
// own health check is not held waiting on it.
func TestEvaluateOnceReportsOperationsWithoutRequestMetricsAsNotApplicable(t *testing.T) {
	d := requestmetrics.NewDetector(requestmetrics.Config{
		// The spec Go's generated detector declares (memory.genericSpec).
		Spec: sdk.DetectorSpec{
			ID: "request-metrics", Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge,
			Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 2},
			Batching:    sdk.BatchingPolicy{Severity: sdk.SeverityWarning, Debounce: 500 * time.Millisecond},
			Playbooks:   []string{"health-objective"}, OriginatingCommit: "lifecycle-bootstrap", Interval: 10 * time.Second,
		},
		Operations: []requestmetrics.Operation{{ID: "reserve", Service: "frontend", Route: "/reservation", Unmeasurable: "no entries record for service frontend"}},
		Rule:       requestmetrics.DefaultRule(),
	})
	verdict, err := evaluateOnceClassed(t, []sdk.Detector{d})
	if err != nil || verdict.Status != evaluation.Clear || len(verdict.Findings) != 0 {
		t.Fatalf("verdict = %+v, %v", verdict, err)
	}
	var note string
	for _, state := range verdict.DetectorStates {
		if state.DetectorID == "request-metrics" && state.Status == string(DetectorEvaluationClear) {
			note = state.Reason
		}
	}
	if !strings.Contains(note, "not applicable") || !strings.Contains(note, "reserve") {
		t.Fatalf("detector states = %+v", verdict.DetectorStates)
	}
	if !detectorConfirmed(d, syntheticShopSnapshot()) {
		t.Fatal("an operation without request metrics held the install's confirmation")
	}
}
