package runtime

import (
	"context"
	"testing"
	"time"

	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memoryhost"
	"sdo.dev/operator/memoryhost/hostio"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

var liveMetricQuery = metrics.Query{ID: "container-cpu-throttle-ratio", Expr: "up"}

// metricProgram queries a metric on demand, as a learned Diagnose may: the
// query is not declared anywhere the controller could read ahead of time.
type metricProgram struct{}

func (metricProgram) Spec() program.ProgramSpec { return program.ProgramSpec{ID: "metric-program"} }

func (metricProgram) Diagnose(_ context.Context, v program.ReadView, _ program.Bindings) (program.Verdict, error) {
	r := v.QueryMetrics(liveMetricQuery)
	if r.Error != "" || len(r.Series) == 0 {
		return program.Verdict{Result: program.Unknown}, nil
	}
	return program.Verdict{Result: program.Match, Evidence: []program.Evidence{{Fact: "throttled"}}}, nil
}

// A live Diagnose that queries metrics gets the controller's live answer; the
// host has no metrics source of its own, so the request is answered, not
// reported as an unrecorded call.
func TestRemoteDiagnoseAnswersAnOnDemandMetricQueryLive(t *testing.T) {
	server, err := memoryhost.NewServer("accepted", "binary", nil)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := program.NewRegistry([]program.Diagnoser{metricProgram{}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	server.WithPrograms(registry).WithReadFactory(func(ctx context.Context, snapshot sdktest.Snapshot) (program.ReadView, error) {
		return hostio.NewLiveReadView(ctx, snapshot, hostio.ReadBackends{}), nil
	})
	if err := server.Apply(&pb.MirrorRequest{Namespace: "app", Sequence: 1, Complete: true, ResetMirror: true}); err != nil {
		t.Fatal(err)
	}
	p := &remoteProgram{spec: metricProgram{}.Spec(), remote: &RemoteMemory{rpc: acceptedReadsHost{server}, memoryCommit: "accepted"}}
	// The controller answers any query live; its snapshot holds none ahead of time.
	controller := liveMetricsView{&sdktest.ReplayReadView{Snapshot: sdktest.Snapshot{NamespaceName: "app"}}}
	verdict, err := p.Diagnose(context.Background(), NewCycleReadView(controller), program.Bindings{})
	if err != nil || verdict.Result != program.Match {
		t.Fatalf("remote diagnose = %+v, %v", verdict, err)
	}
}

// A controller with no metrics source says so: Unknown, with the cause, never
// the host's unrecorded-call text.
func TestRemoteDiagnoseNamesAMissingMetricsSource(t *testing.T) {
	server, err := memoryhost.NewServer("accepted", "binary", nil)
	if err != nil {
		t.Fatal(err)
	}
	var seen string
	registry, err := program.NewRegistry([]program.Diagnoser{errorCapture{&seen}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	server.WithPrograms(registry).WithReadFactory(func(ctx context.Context, snapshot sdktest.Snapshot) (program.ReadView, error) {
		return hostio.NewLiveReadView(ctx, snapshot, hostio.ReadBackends{}), nil
	})
	if err := server.Apply(&pb.MirrorRequest{Namespace: "app", Sequence: 1, Complete: true, ResetMirror: true}); err != nil {
		t.Fatal(err)
	}
	p := &remoteProgram{spec: errorCapture{}.Spec(), remote: &RemoteMemory{rpc: acceptedReadsHost{server}, memoryCommit: "accepted"}}
	controller := noMetricsView{&sdktest.ReplayReadView{Snapshot: sdktest.Snapshot{NamespaceName: "app"}}}
	if _, err := p.Diagnose(context.Background(), NewCycleReadView(controller), program.Bindings{}); err != nil {
		t.Fatal(err)
	}
	if seen != "no telemetry source bound" {
		t.Fatalf("metric error = %q, want the controller's answer", seen)
	}
}

type errorCapture struct{ seen *string }

func (errorCapture) Spec() program.ProgramSpec { return program.ProgramSpec{ID: "error-capture"} }
func (e errorCapture) Diagnose(_ context.Context, v program.ReadView, _ program.Bindings) (program.Verdict, error) {
	*e.seen = v.QueryMetrics(liveMetricQuery).Error
	return program.Verdict{Result: program.Unknown}, nil
}

// liveMetricsSnapshot answers every metric query, as the controller's
// frozen-per-cycle source does.
type liveMetricsView struct{ *sdktest.ReplayReadView }

func (liveMetricsView) QueryMetrics(q metrics.Query) metrics.Result {
	return metrics.Result{Query: q, ObservedAt: time.Unix(1, 0).UTC(), Series: []metrics.Series{{Labels: map[string]string{}, Samples: []metrics.Sample{{Value: 0.9}}}}}
}

// noMetricsSnapshot is a controller with no telemetry source bound.
type noMetricsView struct{ *sdktest.ReplayReadView }

func (noMetricsView) QueryMetrics(q metrics.Query) metrics.Result {
	return metrics.Result{Query: q, Error: "no telemetry source bound"}
}
