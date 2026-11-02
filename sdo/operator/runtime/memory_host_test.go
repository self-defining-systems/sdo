package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"reflect"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/memoryhost/hostio"
	"sdo.dev/operator/runtime/sim"
	"sdo.dev/operator/sdk/generichealth"
	"sdo.dev/operator/sdk/servicehealth"

	"testing"
	"time"

	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memoryhost"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/logs"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/usage"
)

type timingDetector struct{ sdk.DetectorSpec }

func (d timingDetector) Spec() sdk.DetectorSpec { return d.DetectorSpec }
func (d timingDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	return nil, nil
}

func TestRemoteMemoryUnavailableFailsClosed(t *testing.T) {
	s, _ := memoryhost.NewServer("c", "d", []sdk.Detector{timingDetector{DetectorSpec: sdk.DetectorSpec{ID: "test"}}})
	ready, _ := s.Ready(context.Background(), nil)
	ready.Programs = []*pb.ProgramSpec{{Id: "p"}}
	ready.MitigatorIds = []string{"p"}
	remote, err := NewRemoteMemory(ready, memoryhost.NewManager(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = remote.Detectors[0].Detect(context.Background(), sdktest.Snapshot{}); err == nil {
		t.Fatal("host failure cleared detector")
	}
	view := &sdktest.ReplayReadView{}
	p, _ := remote.Programs.Diagnoser("p")
	if _, err = p.Diagnose(context.Background(), view, program.Bindings{}); err == nil {
		t.Fatal("host failure cleared diagnosis")
	}
	m, _ := remote.Programs.Mitigator("p")
	if _, err = m.Mitigate(context.Background(), view, program.Bindings{}); err == nil {
		t.Fatal("host failure authorized plan")
	}
}

type testMemoryRPC struct{ client pb.MemoryHostServiceClient }

func (c testMemoryRPC) Evaluate(ctx context.Context, r *pb.EvaluateRequest) (*pb.EvaluateResponse, error) {
	return c.client.Evaluate(ctx, r)
}
func (c testMemoryRPC) Diagnose(ctx context.Context, r *pb.DiagnoseRequest) (*pb.DiagnoseResponse, error) {
	return c.client.Diagnose(ctx, r)
}
func (c testMemoryRPC) Mitigate(ctx context.Context, r *pb.MitigateRequest) (*pb.MitigateResponse, error) {
	return c.client.Mitigate(ctx, r)
}

type memoryTesting interface {
	Helper()
	Cleanup(func())
	Fatal(...any)
}

func memoryTestRPC(t memoryTesting, s *memoryhost.Server) (testMemoryRPC, func()) {
	t.Helper()
	listener := bufconn.Listen(16 << 20)
	server := grpc.NewServer()
	pb.RegisterMemoryHostServiceServer(server, s)
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(listener) }()
	stop := func() { server.Stop(); <-done }
	t.Cleanup(stop)
	conn, err := grpc.NewClient("passthrough:///memory-host", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return testMemoryRPC{client: pb.NewMemoryHostServiceClient(conn)}, stop
}

type remoteInputDetector struct{ timingDetector }

func (remoteInputDetector) ConsumesServiceReachability() bool { return true }
func (remoteInputDetector) MetricQueries() []metrics.Query {
	return []metrics.Query{{ID: "load", Expr: "up"}}
}
func (remoteInputDetector) LogQueries() []logs.Query { return []logs.Query{{ID: "errors"}} }
func (remoteInputDetector) UsageRequirements() usage.Requirements {
	return usage.Requirements{Throttling: true}
}
func (remoteInputDetector) Detect(_ context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	source := inputs.New(view)
	if _, ok := source.MetricResult("load"); !ok {
		return nil, errors.New("missing metric")
	}
	if _, ok := source.LogResult("errors"); !ok {
		return nil, errors.New("missing logs")
	}
	if value, ok := source.UsageResult(); !ok || value.Error != "synthetic" {
		return nil, errors.New("missing usage")
	}
	return []sdk.Finding{{RuleID: "test", Summary: "all frozen inputs present"}}, nil
}

type remoteReadProgram struct{}

func (remoteReadProgram) Spec() program.ProgramSpec { return program.ProgramSpec{ID: "remote"} }
func (remoteReadProgram) Diagnose(_ context.Context, view program.ReadView, _ program.Bindings) (program.Verdict, error) {
	_, err := view.ProbeTCP(sdk.ObjectRef{Kind: "Service", Namespace: "toy", Name: "app"}, 80)
	return program.Verdict{Result: program.Match}, err
}
func (remoteReadProgram) Mitigate(_ context.Context, view program.ReadView, _ program.Bindings) (program.Plan, error) {
	_, err := view.ProbeTCP(sdk.ObjectRef{Kind: "Service", Namespace: "toy", Name: "app"}, 80)
	return program.Plan{Actions: []program.Action{program.RestartRollout{Ref: sdk.ObjectRef{Kind: "Deployment", Namespace: "toy", Name: "app"}}}}, err
}
func TestRemoteMemoryPreservesInputsAndCorpusReceipts(t *testing.T) {
	sim.Run(t, 1, func(t *testing.T, world *sim.World) {
		ctx := context.Background()
		s, _ := memoryhost.NewServer("c", "d", []sdk.Detector{remoteInputDetector{timingDetector{DetectorSpec: sdk.DetectorSpec{ID: "inputs"}}}})
		registry, _ := program.NewRegistry([]program.Diagnoser{remoteReadProgram{}}, []program.Mitigator{remoteReadProgram{}})
		s.WithPrograms(registry)
		calls := []sdktest.PrimitiveCall{{Method: "ProbeTCP", Arguments: []byte(`[{"kind":"Service","namespace":"toy","name":"app"},80]`), Result: []byte(`{"success":true}`)}}
		s.WithReadFactory(func(_ context.Context, snapshot sdktest.Snapshot) (program.ReadView, error) {
			return &sdktest.ReplayReadView{Snapshot: snapshot, Calls: calls}, nil
		})

		ready, _ := s.Ready(ctx, nil)
		rpc, stop := memoryTestRPC(t, s)
		remote, err := NewRemoteMemory(ready, mirroredTestHost(t, ctx, rpc, "c", "d", "toy"))
		if err != nil {
			t.Fatal(err)
		}
		if !ConsumesServiceReachability(remote.Detectors) {
			t.Fatal("lost reachability declaration")
		}
		if qs, err := metrics.Queries(remote.Detectors); err != nil || len(qs) != 1 {
			t.Fatal(qs, err)
		}
		if qs, err := logs.Queries(remote.Detectors); err != nil || len(qs) != 1 {
			t.Fatal(qs, err)
		}
		if !remote.Detectors[0].(usage.Consumer).UsageRequirements().Throttling {
			t.Fatal("lost usage declaration")
		}
		snapshot := sdktest.Snapshot{NamespaceName: "toy", Metrics: map[string]metrics.Result{"load": {}}, Logs: map[string]logs.Result{"errors": {}}, Usage: &usage.Result{Error: "synthetic"}}
		findings, err := remote.Detectors[0].Detect(ctx, snapshot)
		if err != nil || len(findings) != 1 {
			t.Fatal(findings, err)
		}
		count := 0
		view := &sdktest.RecordingReadView{ReadView: &sdktest.ReplayReadView{Snapshot: snapshot}, OnRecord: func(sdktest.PrimitiveCall) { count++ }}
		p, _ := remote.Programs.Diagnoser("remote")
		verdict := program.RunDiagnose(ctx, p, view, program.Bindings{}, time.Second)
		if verdict.Result != program.Match || count != 1 || len(view.SnapshotCalls()) != 1 {
			t.Fatal(verdict, count, view.SnapshotCalls())
		}
		m, _ := remote.Programs.Mitigator("remote")
		plan, err := m.Mitigate(ctx, view, program.Bindings{})
		if err != nil || len(plan.Actions) != 1 || count != 2 {
			t.Fatal(plan, err, count)
		}
		// The controller recorder never executed its own unavailable primitive.
		replay := &sdktest.ReplayReadView{Calls: view.SnapshotCalls()}
		if got, err := replay.ProbeTCP(sdk.ObjectRef{Kind: "Service", Namespace: "toy", Name: "app"}, 80); err != nil || !got.Success {
			t.Fatal(got, err)
		}
		stop()
		if _, err := remote.Detectors[0].Detect(ctx, snapshot); err == nil {
			t.Fatal("closed Unix socket cleared remote detector")
		}

	})
}

// This program consumes the same raw logs as the ordinary controller reader,
// but only the controller receives and applies the resulting action.
type simMemoryProgram struct{}

func (simMemoryProgram) Spec() program.ProgramSpec { return program.ProgramSpec{ID: "sim-logs"} }
func (simMemoryProgram) Diagnose(_ context.Context, v program.ReadView, _ program.Bindings) (program.Verdict, error) {
	text, err := v.Logs(sdk.ObjectRef{Kind: "Pod", Namespace: "app", Name: "api"}, "api", time.Minute)
	if err != nil {
		return program.Verdict{}, err
	}
	if strings.Contains(text, "synthetic failure") {
		return program.Verdict{Result: program.Match}, nil
	}
	return program.Verdict{Result: program.NoMatch}, nil
}
func (simMemoryProgram) Mitigate(ctx context.Context, v program.ReadView, b program.Bindings) (program.Plan, error) {
	verdict, err := (simMemoryProgram{}).Diagnose(ctx, v, b)
	if err != nil {
		return program.Plan{}, err
	}
	if verdict.Result != program.Match {
		return program.Plan{}, errors.New("no matching logs")
	}
	return program.Plan{Rationale: "synthetic failure", Actions: []program.Action{program.RestartRollout{Ref: sdk.ObjectRef{Kind: "Deployment", Namespace: "app", Name: "api"}}}}, nil
}

func TestSimMemoryHostParityAndCrash(t *testing.T) {
	sim.Run(t, 42, func(t *testing.T, w *sim.World) {
		ctx := context.Background()
		w.Kube.Put(&unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1", "kind": "Service", "metadata": map[string]any{"name": "api", "namespace": "app"},
			"spec": map[string]any{"selector": map[string]any{"app": "api"}, "ports": []any{map[string]any{"port": int64(80)}}},
		}})
		w.Kube.Put(&unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1", "kind": "Pod", "metadata": map[string]any{"name": "api", "namespace": "app", "labels": map[string]any{"app": "api"}},
			"spec":   map[string]any{"containers": []any{map[string]any{"name": "api", "image": "synthetic"}}},
			"status": map[string]any{"phase": "Running"},
		}})
		w.Logs[sim.LogKey{Namespace: "app", Pod: "api", Container: "api"}] = w.Now().Format(time.RFC3339Nano) + " synthetic failure\n"
		client, err := w.Client()
		if err != nil {
			t.Fatal(err)
		}
		local := []sdk.Detector{
			servicehealth.NewReadyEndpointsDetector(sdk.DetectorSpec{ID: "endpoints"}),
			generichealth.NewWiring(sdk.DetectorSpec{ID: "wiring"}),
		}
		registry, err := program.NewRegistry([]program.Diagnoser{simMemoryProgram{}}, []program.Mitigator{simMemoryProgram{}})
		if err != nil {
			t.Fatal(err)
		}
		host, err := memoryhost.NewServer("accepted", "sha256:test", local)
		if err != nil {
			t.Fatal(err)
		}
		host.WithPrograms(registry).WithReadFactory(func(ctx context.Context, s sdktest.Snapshot) (program.ReadView, error) {
			return hostio.NewLiveReadView(ctx, s, client), nil
		})
		rpc, stop := memoryTestRPC(t, host)
		ready, _ := host.Ready(ctx, nil)
		remote, err := NewRemoteMemory(ready, mirroredTestHost(t, ctx, rpc, "accepted", "sha256:test", "app"))
		if err != nil {
			t.Fatal(err)
		}
		snapshot, err := w.Cache.Snapshot(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for i, d := range local {
			expected, err := d.Detect(ctx, snapshot)
			if err != nil {
				t.Fatal(err)
			}
			actual, err := remote.Detectors[i].Detect(ctx, snapshot)
			if err != nil {
				t.Fatal(err)
			}
			// Local detectors can omit their own ID; the execution boundary supplies it.
			for j := range expected {
				if expected[j].DetectorID == "" {
					expected[j].DetectorID = d.Spec().ID
				}
			}
			a, _ := json.Marshal(expected)
			b, _ := json.Marshal(actual)
			if !bytes.Equal(a, b) {
				t.Fatalf("%s local=%s host=%s", d.Spec().ID, a, b)
			}
		}
		captured, err := sdktest.CaptureSnapshot(snapshot, "app")
		if err != nil {
			t.Fatal(err)
		}
		localView := hostio.NewLiveReadView(ctx, captured, client)
		p, _ := registry.Diagnoser("sim-logs")
		expectedVerdict, err := p.Diagnose(ctx, localView, program.Bindings{})
		if err != nil {
			t.Fatal(err)
		}
		recorder := &sdktest.RecordingReadView{ReadView: &sdktest.ReplayReadView{Snapshot: captured}}
		rp, _ := remote.Programs.Diagnoser("sim-logs")
		verdict := program.RunDiagnose(ctx, rp, recorder, program.Bindings{}, time.Second)
		if !reflect.DeepEqual(verdict, expectedVerdict) {
			t.Fatal(verdict, expectedVerdict)
		}
		m, _ := registry.Mitigator("sim-logs")
		expectedPlan, err := m.Mitigate(ctx, localView, program.Bindings{})
		if err != nil {
			t.Fatal(err)
		}
		rm, _ := remote.Programs.Mitigator("sim-logs")
		plan, err := rm.Mitigate(ctx, recorder, program.Bindings{})
		if err != nil || !reflect.DeepEqual(plan, expectedPlan) {
			t.Fatal(plan, expectedPlan, err)
		}
		calls := recorder.SnapshotCalls()
		if len(calls) != 2 || calls[0].Method != "Logs" || !bytes.Contains(calls[0].Result, []byte("synthetic failure")) {
			t.Fatal("missing host read evidence", calls)
		}
		stop()
		for _, d := range remote.Detectors {
			if _, err := d.Detect(ctx, snapshot); err == nil {
				t.Fatal("host crash cleared", d.Spec().ID)
			}
		}
	})
}

func mirroredTestHost(t *testing.T, ctx context.Context, rpc testMemoryRPC, commit, digest, namespace string) *memoryhost.Manager {
	t.Helper()
	m := memoryhost.NewManager(time.Second)
	t.Cleanup(func() { _ = m.Close() })
	if err := m.Publish(ctx, sdktest.Snapshot{NamespaceName: namespace}); err != nil {
		t.Fatal(err)
	}
	h := &simActivationHost{testMemoryRPC: rpc, stop: func() {}}
	if err := m.Prepare(ctx, h); err != nil {
		t.Fatal(err)
	}
	if err := m.Activate(ctx, h, commit, digest); err != nil {
		t.Fatal(err)
	}
	return m
}
