package runtime

import (
	"context"
	"testing"
	"time"

	"sdo.dev/operator/runtime/prober"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/servicehealth"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/traffic"
)

type captureTestProber struct{ windows map[string]traffic.Window }

func (p captureTestProber) Windows(context.Context) (map[string]traffic.Window, error) {
	return p.windows, nil
}
func (captureTestProber) Burst(context.Context, prober.BurstRequest) (prober.BurstResult, error) {
	return prober.BurstResult{}, nil
}
func (captureTestProber) Reset(context.Context) error { return nil }

type captureTestBase struct{ snapshot sdktest.Snapshot }

func (b captureTestBase) Snapshot(context.Context) (sdk.DetectionContext, error) { return b.snapshot, nil }

// A capture of the controller's composed snapshot is what the memory check
// replays detectors on (healthy baseline, incident cases). It must carry the
// synthetic-traffic and reachability windows the live evaluation had, or every
// detector that reads traffic fails the healthy replay with "traffic
// unavailable" although it was quiet on the live healthy state.
func TestCapturedSnapshotKeepsTheObservationsDetectorsRead(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	window := traffic.Window{Workload: traffic.Workload{Name: "health"}, ObservedAt: now}
	scenario := traffic.ScenarioObservations{Qualified: true, Scenario: traffic.Descriptor{ID: "read", Target: traffic.Target{Service: "front"}}}
	for i := 0; i < 20; i++ {
		scenario.Samples = append(scenario.Samples, traffic.Sample{At: now.Add(-time.Duration(20-i) * time.Second), Outcome: traffic.OutcomeOK, Latency: 10 * time.Millisecond})
	}
	window.Scenarios = []traffic.ScenarioObservations{scenario}
	observer := NewTrafficObserver(captureTestProber{windows: map[string]traffic.Window{"health": window}}, []string{"health"}, nil, time.Second)
	observer.Poll(context.Background())
	provider := TrafficSnapshotProvider{Base: captureTestBase{sdktest.Snapshot{NamespaceName: "app", ObservedAtTime: &now}}, Observer: observer}
	live, err := provider.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	dials := servicehealth.ReachabilityWindow{ObservedAt: now, Failures: 3}
	live = inputs.New(live).WithReachability(sdktest.Snapshot{Reachability: &dials})
	captured, err := sdktest.CaptureSnapshot(live, "app")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := captured.TrafficWindow("health"); !ok {
		t.Fatal("captured snapshot lost the health traffic window")
	}
	if _, ok := captured.ServiceReachability(); !ok {
		t.Fatal("captured snapshot lost the Service reachability window")
	}
	detector := traffic.NewDetector(sdk.DetectorSpec{ID: "journey-stall", Class: sdk.DetectorClassIncident}, "health")
	if violations := sdktest.HealthyBaselineViolations(detector, []sdktest.Snapshot{captured}); len(violations) > 0 {
		t.Fatalf("traffic detector fails the healthy replay of a capture: %v", violations)
	}
}
