package runtime

import (
	"context"
	"maps"
	"testing"
	"time"

	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memoryhost"
	"sdo.dev/operator/memoryhost/hostio"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/podread"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/traffic"
	"sdo.dev/operator/sdk/vantage"
)

// cycleInputsProgram reads cycle inputs no memory-host detector declared: a
// controller-side detector froze them, so the controller's own view answers.
type cycleInputsProgram struct{ seen *[]string }

func (cycleInputsProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "cycle-inputs-program"}
}

func (p cycleInputsProgram) Diagnose(_ context.Context, v program.ReadView, _ program.Bindings) (program.Verdict, error) {
	var missing []string
	if _, ok := v.MetricResult("request-errors"); !ok {
		missing = append(missing, "metric")
	}
	if _, ok := v.TrafficWindow("checkout"); !ok {
		missing = append(missing, "traffic")
	}
	if _, ok := podread.ResultFrom(v, "queue-depth"); !ok {
		missing = append(missing, "pod read")
	}
	if _, ok := vantage.ResultFrom(v, "edge-probe"); !ok {
		missing = append(missing, "vantage")
	}
	*p.seen = missing
	if len(missing) > 0 {
		return program.Verdict{Result: program.Unknown}, nil
	}
	return program.Verdict{Result: program.Match, Evidence: []program.Evidence{{Fact: "inputs read"}}}, nil
}

// cycleInputsView is the controller's cycle view: it froze results that
// detectors outside the memory host declared.
type cycleInputsView struct{ *sdktest.ReplayReadView }

func (v cycleInputsView) MetricResults() map[string]metrics.Result {
	return maps.Clone(v.Snapshot.Metrics)
}
func (v cycleInputsView) TrafficWindows() map[string]traffic.Window {
	return maps.Clone(v.Snapshot.Traffic)
}
func (v cycleInputsView) PodReadResults() map[string]podread.Result {
	return maps.Clone(v.Snapshot.PodReads)
}
func (v cycleInputsView) VantageResults() map[string]vantage.Result {
	return maps.Clone(v.Snapshot.Vantage)
}

// A learned Diagnose in the memory host reads the same cycle inputs the
// controller's own view holds, not only those a host detector declared.
func TestRemoteDiagnoseReadsEveryCycleInputTheControllerHolds(t *testing.T) {
	server, err := memoryhost.NewServer("accepted", "binary", nil)
	if err != nil {
		t.Fatal(err)
	}
	var missing []string
	registry, err := program.NewRegistry([]program.Diagnoser{cycleInputsProgram{&missing}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	server.WithPrograms(registry).WithReadFactory(func(ctx context.Context, snapshot sdktest.Snapshot) (program.ReadView, error) {
		return hostio.NewLiveReadView(ctx, snapshot, hostio.ReadBackends{}), nil
	})
	if err := server.Apply(&pb.MirrorRequest{Namespace: "app", Sequence: 1, Complete: true, ResetMirror: true}); err != nil {
		t.Fatal(err)
	}
	at := time.Unix(1, 0).UTC()
	snapshot := sdktest.Snapshot{NamespaceName: "app",
		Metrics:  map[string]metrics.Result{"request-errors": {Query: metrics.Query{ID: "request-errors", Expr: "up"}, ObservedAt: at}},
		Traffic:  map[string]traffic.Window{"checkout": {ObservedAt: at}},
		PodReads: map[string]podread.Result{"queue-depth": {Query: podread.Query{ID: "queue-depth"}}},
		Vantage:  map[string]vantage.Result{"edge-probe": {Probe: vantage.Probe{ID: "edge-probe"}}},
	}
	p := &remoteProgram{spec: cycleInputsProgram{}.Spec(), remote: &RemoteMemory{rpc: acceptedReadsHost{server}, memoryCommit: "accepted"}}
	controller := cycleInputsView{&sdktest.ReplayReadView{Snapshot: snapshot}}
	verdict, err := p.Diagnose(context.Background(), NewCycleReadView(controller), program.Bindings{})
	if err != nil || verdict.Result != program.Match {
		t.Fatalf("remote diagnose = %+v, %v; host missed %v", verdict, err, missing)
	}
}
