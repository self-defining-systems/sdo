package runtime

import (
	"context"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

// advancingProvider spends fake time in the snapshot: a second of its own and
// three inside a named layer, as the layered providers do.
type advancingProvider struct{ clock *fakeClock }

func (p advancingProvider) Snapshot(ctx context.Context) (sdk.DetectionContext, error) {
	p.clock.Advance(time.Second)
	end := measurePhase(ctx, "fake_layer")
	p.clock.Advance(3 * time.Second)
	end()
	return sdktest.Snapshot{}, nil
}

type advancingDetector struct {
	*sequenceDetector
	clock *fakeClock
}

func (d advancingDetector) Detect(ctx context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	d.clock.Advance(2 * time.Second)
	return d.sequenceDetector.Detect(ctx, view)
}

// A step that holds the observation lock past the warning bound names where the
// time went: each snapshot layer, the cache snapshot, drift, detector evaluation
// and the work ahead of the snapshot, so a live run attributes the hold.
func TestObservationLockHeldWarningAttributesTheStepToPhases(t *testing.T) {
	read := loggedRecords(t, "observation_lock_held")
	clock := &fakeClock{}
	detector := advancingDetector{controllerDetector("d", time.Second, sdk.Finding{}), clock}
	controller, err := NewController(testControllerConfig(), []sdk.Detector{detector}, advancingProvider{clock}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	controller.now = clock.Now
	if err := controller.StepEvents(context.Background(), clock.Now(), nil); err != nil {
		t.Fatal(err)
	}
	records := read()
	if len(records) != 1 || records[0]["holder"] != "step_events" {
		t.Fatalf("observation_lock_held records = %v", records)
	}
	phases := phaseMillis(t, records[0])
	for name, want := range map[string]float64{"snapshot": 1000, "fake_layer": 3000, "detectors": 2000} {
		if phases[name] != want {
			t.Fatalf("phase %s = %v ms, want %v (phases %v)", name, phases[name], want, phases)
		}
	}
	for _, name := range []string{"dispatch_completions", "retire_expired", "source_inventory", "drift"} {
		if _, ok := phases[name]; !ok {
			t.Fatalf("phase %s missing from %v", name, phases)
		}
	}
	if records[0]["held_ms"] != float64(6000) || records[0]["unattributed_ms"] != float64(0) {
		t.Fatalf("held_ms = %v unattributed_ms = %v, want 6000 and 0", records[0]["held_ms"], records[0]["unattributed_ms"])
	}
}

// A holder that never marks a phase still logs its total, with the whole hold
// unattributed.
func TestObservationLockHeldWarningWithoutPhasesIsUnattributed(t *testing.T) {
	read := loggedRecords(t, "observation_lock_held")
	clock := &fakeClock{}
	c := &Controller{now: clock.Now}
	release := c.lockObservation("pause_barrier")
	clock.Advance(2 * time.Second)
	release()
	records := read()
	if len(records) != 1 || records[0]["held_ms"] != float64(2000) || records[0]["unattributed_ms"] != float64(2000) {
		t.Fatalf("records = %v", records)
	}
}
