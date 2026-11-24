package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"google.golang.org/protobuf/proto"
	"time"

	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/internal/programread"
	"sdo.dev/operator/memoryhost"
	"sdo.dev/operator/memoryhost/hostio"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/logs"
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
	if result, ok := v.LogResult("startup"); !ok || result.Query.ID != "startup" || !result.ObservedAt.Equal(time.Unix(1, 0)) {
		missing = append(missing, "log")
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
		Logs:     map[string]logs.Result{"startup": {Query: logs.Query{ID: "startup"}, ObservedAt: at}},
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

type objectOnlyProgram struct{}

func (objectOnlyProgram) Spec() program.ProgramSpec { return program.ProgramSpec{ID: "object-only"} }
func (objectOnlyProgram) Diagnose(_ context.Context, view program.ReadView, _ program.Bindings) (program.Verdict, error) {
	object, err := view.Object(cmRef("input"))
	if err != nil {
		return program.Verdict{}, err
	}
	return program.Verdict{Result: program.Match, Evidence: []program.Evidence{{Fact: object.GetLabels()["cycle"]}}}, nil
}

type limitedProgramHost struct{ acceptedReadsHost }

func (h limitedProgramHost) Capture(context.Context, sdk.DetectionContext) (memoryhost.Host, uint64, func(), error) {
	return h, 1, func() {}, nil
}

func (h limitedProgramHost) ExecuteProgram(ctx context.Context, invocation *pb.MemoryProgramInvocation, read func(programread.Message) programread.Message) (*pb.RunProgramResponse, error) {
	if size := proto.Size(invocation); size > memoryhost.MaxMessageBytes {
		return nil, fmt.Errorf("program invocation exceeds the message limit: %d bytes", size)
	}
	return h.Server.ExecuteProgram(ctx, invocation, func(call programread.Message) programread.Message {
		if call.Method != "Object" {
			return programread.Message{Error: "unexpected read " + call.Method}
		}
		return read(call)
	})
}

func TestRemoteObjectOnlyDiagnoseIgnoresUnrelatedRangeResults(t *testing.T) {
	remote := hostRunning(t, objectOnlyProgram{})
	remote.remote.rpc = limitedProgramHost{remote.remote.rpc.(acceptedReadsHost)}
	at := time.Unix(1, 0).UTC()
	samples := make([]metrics.Sample, metrics.MaxRangePoints)
	for i := range samples {
		samples[i] = metrics.Sample{At: at.Add(time.Duration(i) * time.Second), Value: float64(i)}
	}
	series := make([]metrics.Series, metrics.MaxSamples/len(samples))
	for i := range series {
		series[i] = metrics.Series{Labels: map[string]string{"shard": fmt.Sprint(i)}, Samples: samples}
	}
	snapshot := sdktest.Snapshot{NamespaceName: "app", Metrics: map[string]metrics.Result{}}
	for i := range 6 {
		id := fmt.Sprintf("range-%d", i)
		query := metrics.Query{ID: id, Expr: "up", Range: time.Duration(len(samples)-1) * time.Second, Step: time.Second}
		if err := query.Validate(); err != nil {
			t.Fatal(err)
		}
		snapshot.Metrics[id] = metrics.Result{Query: query, ObservedAt: samples[len(samples)-1].At, Series: series}
	}
	for i := range 2 {
		// Use distinct cycle content while keeping the same accepted program.
		label := fmt.Sprint(i)
		snapshot.ConfigMapList = []corev1.ConfigMap{{ObjectMeta: metav1.ObjectMeta{
			Name: "input", Namespace: "app", Labels: map[string]string{"cycle": label},
		}}}
		view := NewCycleReadView(cycleInputsView{&sdktest.ReplayReadView{Snapshot: snapshot}})
		verdict, err := remote.Diagnose(context.Background(), view, program.Bindings{})
		if err != nil || verdict.Result != program.Match || len(verdict.Evidence) != 1 || verdict.Evidence[0].Fact != label {
			t.Fatalf("object-only remote diagnose for cycle %d = %+v, %v", i, verdict, err)
		}
		if calls := view.SnapshotCalls(); len(calls) != 1 || calls[0].Method != "Object" || !json.Valid(calls[0].Result) {
			t.Fatalf("cycle evidence = %+v, want one object read", calls)
		}
	}
}
