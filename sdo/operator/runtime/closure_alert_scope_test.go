package runtime

import (
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/traffic"
)

type oneWindowView struct {
	sdk.DetectionContext
	window traffic.Window
}

func (v oneWindowView) TrafficWindow(string) (traffic.Window, bool) { return v.window, true }

// scenarioWindow holds one healthy, evaluable "write-issue" and a "write-commit"
// sibling whose single sample is below the minimum.
func scenarioWindow() traffic.Window {
	at := time.Unix(1000, 0)
	workload := traffic.Workload{Name: "probe-write", Purpose: traffic.PurposeHealthProbe, Scenarios: []traffic.WorkloadScenario{{ID: "write-issue"}, {ID: "write-commit"}}}.WithDefaults()
	samples := func(n int) []traffic.Sample {
		var out []traffic.Sample
		for i := range n {
			out = append(out, traffic.Sample{At: at.Add(time.Duration(i) * time.Second), Outcome: traffic.OutcomeOK, Status: 200})
		}
		return out
	}
	return traffic.Window{Workload: workload, ObservedAt: at.Add(5 * time.Second), Scenarios: []traffic.ScenarioObservations{
		{Scenario: traffic.Descriptor{ID: "write-issue", Target: traffic.Target{Service: "web", Port: 80}}, Samples: samples(5), Qualified: true},
		{Scenario: traffic.Descriptor{ID: "write-commit", Target: traffic.Target{Service: "web", Port: 80}}, Samples: samples(1), Qualified: true},
	}}
}

func scenarioKey(detectorID, scenario string) string {
	return FindingStateKey(detectorID, detectorID+"/"+traffic.RuleIDPrefix+scenario+"/app/Service/web")
}

func TestClosureConfirmationJudgesOnlyTheAlertSetScenarios(t *testing.T) {
	const id = "traffic-probe-write"
	detector := traffic.NewDetector(sdk.DetectorSpec{ID: id, Class: sdk.DetectorClassHealth}, "probe-write")
	view := oneWindowView{window: scenarioWindow()}

	if detectorConfirmedIn(detector, view, nil) {
		t.Fatal("whole-detector confirmation must still require every scenario")
	}
	t.Run("a never-alerting insufficient sibling does not block", func(t *testing.T) {
		scope := newRecoveryScope([]string{scenarioKey(id, "write-issue")})
		if !detectorConfirmedIn(detector, view, scope) {
			t.Fatalf("blocked by a sibling outside the alert set: %s", detectorConfirmationReasonIn(detector, view, scope))
		}
	})
	t.Run("an insufficient alert-set scenario blocks", func(t *testing.T) {
		scope := newRecoveryScope([]string{scenarioKey(id, "write-commit")})
		if detectorConfirmedIn(detector, view, scope) {
			t.Fatal("an alert-set scenario without enough samples was confirmed")
		}
		if reason := detectorConfirmationReasonIn(detector, view, scope); !strings.Contains(reason, "write-commit") {
			t.Fatalf("reason = %q, want it to name write-commit", reason)
		}
	})
	t.Run("a joined scenario is part of the set", func(t *testing.T) {
		request := IncidentRequest{JoinedKeys: []string{scenarioKey(id, "write-commit")}}
		if detectorConfirmedIn(detector, view, newRecoveryScope(alertKeys(&request))) {
			t.Fatal("a joined scenario without enough samples was confirmed")
		}
	})
	t.Run("another detector's key does not scope this detector", func(t *testing.T) {
		scope := newRecoveryScope([]string{scenarioKey("traffic-probe-call", "write-commit")})
		if !detectorConfirmedIn(detector, view, scope) {
			t.Fatal("a key of another detector blocked this one")
		}
	})
}
