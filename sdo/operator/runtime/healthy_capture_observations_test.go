package runtime

import (
	"context"
	"testing"
	"time"

	"sdo.dev/operator/runtime/prober"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/metrics"
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

// The controller evaluates detectors on its cycle read view (P8: learned
// detectors guarding on traffic.WindowSet never fired live) (a recording,
// caching wrapper around the live view). Every observation the capture keeps
// must pass through that wrapper too: P8's healthy capture of the cycle view
// had reachability and alerts but no traffic windows or metric results.
func TestCycleReadViewCaptureKeepsEveryObservationWindow(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	source := sdktest.Snapshot{NamespaceName: "app", ObservedAtTime: &now,
		Traffic:      map[string]traffic.Window{"health": {Workload: traffic.Workload{Name: "health"}, ObservedAt: now}},
		Metrics:      map[string]metrics.Result{"p99": {Error: "synthetic"}},
		Alerts:       &metrics.AlertsResult{Error: "synthetic"},
		Reachability: &servicehealth.ReachabilityWindow{ObservedAt: now, Failures: 3}}
	want, err := sdktest.CaptureSnapshot(source, "app")
	if err != nil {
		t.Fatal(err)
	}
	cycle := NewCycleReadView(LiveReadView{Carrier: inputs.New(source), Context: context.Background()})
	// Live detection: learned detectors guard on these sets of the view they
	// are evaluated on; a guard that is always false never fires.
	var view sdk.DetectionContext = cycle
	if set, ok := view.(traffic.WindowSet); !ok || len(set.TrafficWindows()) != 1 {
		t.Fatal("detectors on the cycle read view cannot list traffic windows")
	}
	if set, ok := view.(metrics.ResultSet); !ok || len(set.MetricResults()) != 1 {
		t.Fatal("detectors on the cycle read view cannot list metric results")
	}
	got, err := sdktest.CaptureSnapshot(cycle, "app")
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Traffic) != len(want.Traffic) || len(got.Metrics) != len(want.Metrics) || (got.Alerts == nil) != (want.Alerts == nil) || (got.Reachability == nil) != (want.Reachability == nil) {
		t.Fatalf("cycle-view capture lost observations: traffic %d/%d metrics %d/%d alerts %v reachability %v",
			len(got.Traffic), len(want.Traffic), len(got.Metrics), len(want.Metrics), got.Alerts != nil, got.Reachability != nil)
	}
}
