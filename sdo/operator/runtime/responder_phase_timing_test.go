package runtime

import (
	"context"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type recordedSnapshotProvider struct{ clock *fakeClock }

func (p recordedSnapshotProvider) Snapshot(ctx context.Context) (sdk.DetectionContext, error) {
	end := measurePhase(ctx, "fake_layer")
	p.clock.Advance(2 * time.Second)
	end()
	p.clock.Advance(time.Second)
	return sdktest.Snapshot{}, nil
}

// The tool timing record splits the call into its phases, each snapshot layer
// and the detector evaluation among them, so a slow responder call is attributed.
func TestResponderToolTimingNamesThePhases(t *testing.T) {
	read := loggedRecords(t, "responder_tool_timing")
	clock := &fakeClock{}
	detector := advancingDetector{controllerDetector("d", time.Second, sdk.Finding{}), clock}
	loop := NewResponderLoop(IncidentRequest{IncidentID: "incident"}, nil, recordedSnapshotProvider{clock}, []sdk.Detector{detector}, PlanExecutor{}, 3)
	loop.Now = clock.Now
	loop.ReadView = func(v sdk.DetectionContext) program.ReadView { return program.ReadFrom(v) }
	if response := loop.Handle(context.Background(), ToolRequest{Command: "status"}); response.Error != "" {
		t.Fatal(response.Error)
	}
	records := read()
	if len(records) != 1 {
		t.Fatalf("responder_tool_timing records = %v", records)
	}
	phases := phaseMillis(t, records[0])
	for name, want := range map[string]float64{"snapshot": 1000, "fake_layer": 2000, "detectors": 2000} {
		if phases[name] != want {
			t.Fatalf("phase %s = %v ms, want %v (phases %v)", name, phases[name], want, phases)
		}
	}
	if records[0]["capture_ms"] != float64(3000) || records[0]["total_ms"] != float64(5000) || records[0]["unattributed_ms"] != float64(0) {
		t.Fatalf("record = %v", records[0])
	}
}

// The accepted-source comparison, which takes its own snapshot and a diff per
// resource outside capture_ms, is timed under its own names.
func TestSourceComparisonIsTimedSeparately(t *testing.T) {
	clock := &fakeClock{}
	loop := NewResponderLoop(IncidentRequest{IncidentID: "incident"}, nil, recordedSnapshotProvider{clock}, nil, PlanExecutor{}, 3)
	loop.Now = clock.Now
	loop.ReadView = func(v sdk.DetectionContext) program.ReadView { return program.ReadFrom(v) }
	loop.CorpusSessionRoot = t.TempDir()
	loop.trace = newPhaseTrace(clock.Now)
	ctx := withPhaseTrace(context.Background(), loop.trace)
	loop.recordSourceComparisons(ctx, "open@pre-apply", nil, sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "demo", Name: "web"})
	got := loop.trace.durations()
	if got["source_comparison_snapshot"] != time.Second || got["fake_layer"] != 2*time.Second {
		t.Fatalf("durations = %v, want source_comparison_snapshot 1s and fake_layer 2s", got)
	}
	if _, ok := got["source_comparison"]; !ok {
		t.Fatalf("durations = %v, want a source_comparison phase", got)
	}
}
