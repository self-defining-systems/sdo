package runtime

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memoryhost"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type speedHost struct {
	memoryhost.Host
	captures, releases atomic.Int32
}

func (*speedHost) MemoryCommit() string { return "accepted" }
func (h *speedHost) Capture(_ context.Context, v sdk.DetectionContext) (memoryhost.Host, uint64, func(), error) {
	h.captures.Add(1)
	// Exercise the same whole-snapshot serialization as mirror publication.
	if _, err := memoryhost.SnapshotRefs(v); err != nil {
		return nil, 0, nil, err
	}
	return h, 1, func() { h.releases.Add(1) }, nil
}
func (*speedHost) Evaluate(_ context.Context, r *pb.EvaluateRequest) (*pb.EvaluateResponse, error) {
	return &pb.EvaluateResponse{Results: []*pb.MemoryDetectorResult{{DetectorId: r.DetectorIds[0], Findings: []*pb.Finding{{Status: "active"}}}}}, nil
}
func (*speedHost) Diagnose(context.Context, *pb.DiagnoseRequest) (*pb.DiagnoseResponse, error) {
	return &pb.DiagnoseResponse{Verdict: &pb.Verdict{Result: pb.Verdict_RESULT_NO_MATCH}}, nil
}
func (*speedHost) Mitigate(context.Context, *pb.MitigateRequest) (*pb.MitigateResponse, error) {
	panic("read command planned a mitigation")
}

func speedLoop() (*ResponderLoop, *speedHost) {
	snapshot := sdktest.Snapshot{NamespaceName: "app"}
	for i := 0; i < 30; i++ {
		name := fmt.Sprintf("service-%02d", i)
		snapshot.DeploymentList = append(snapshot.DeploymentList, appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app"}})
		snapshot.ServiceList = append(snapshot.ServiceList, corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app"}})
		for j := 0; j < 3; j++ {
			snapshot.PodList = append(snapshot.PodList, corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("%s-%d", name, j), Namespace: "app"}})
		}
	}
	host := &speedHost{}
	remote := &RemoteMemory{rpc: host, memoryCommit: "accepted"}
	var detectors []sdk.Detector
	for i := 0; i < 12; i++ {
		detectors = append(detectors, &remoteDetector{Registration: memoryhost.Registration{DetectorSpec: sdk.DetectorSpec{ID: fmt.Sprintf("detector-%02d", i), Watches: []sdk.WatchKind{{Kind: "Deployment"}}}}, remote: remote})
	}
	var ds []program.Diagnoser
	for i := 0; i < 18; i++ {
		ds = append(ds, &remoteProgram{spec: program.ProgramSpec{ID: fmt.Sprintf("program-%02d", i)}, remote: remote})
	}
	ms := make([]program.Mitigator, 0, len(ds))
	for _, d := range ds {
		ms = append(ms, d.(*remoteProgram))
	}
	registry, _ := program.NewRegistry(ds, ms)
	loop := NewResponderLoop(IncidentRequest{IncidentID: "synthetic", Namespace: "app"}, registry, staticProvider{snapshot}, detectors, PlanExecutor{}, 10)
	loop.ReadView = func(v sdk.DetectionContext) program.ReadView { return program.ReadFrom(v) }
	return loop, host
}

func TestReadSnapshotCaptureBudget(t *testing.T) {
	for _, command := range []string{"status", "detect", "diagnose"} {
		t.Run(command, func(t *testing.T) {
			loop, host := speedLoop()
			response := loop.Handle(context.Background(), ToolRequest{Command: command})
			if response.Error != "" {
				t.Fatal(response.Error)
			}
			if got := host.captures.Load(); got != 1 {
				t.Fatalf("one capture per fresh snapshot: got %d", got)
			}
			if host.releases.Load() != host.captures.Load() {
				t.Fatal("host lease leaked")
			}
			response = loop.Handle(context.Background(), ToolRequest{Command: command})
			if response.Error != "" {
				t.Fatal(response.Error)
			}
			if host.captures.Load() != 2 {
				t.Fatal("next command reused an old snapshot")
			}
		})
	}
}
func BenchmarkResponderRead(b *testing.B) {
	for _, command := range []string{"status", "detect", "diagnose", "menu", "source"} {
		b.Run(command, func(b *testing.B) {
			loop, _ := speedLoop()
			loop.MenuConfig = &ResponderMenuConfig{}
			loop.Request.SourceCommit = "synthetic"
			loop.Executor.Source = speedSource{}
			request := ToolRequest{Command: command}
			if command == "source" {
				request.Ref = &sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r := loop.Handle(context.Background(), request)
				if r.Error != "" {
					b.Fatal(r.Error)
				}
			}
		})
	}
}

type barrierSnapshot struct {
	snapshot sdk.DetectionContext
	entered  chan struct{}
	release  chan struct{}
}

func (p barrierSnapshot) Snapshot(ctx context.Context) (sdk.DetectionContext, error) {
	select {
	case p.entered <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case <-p.release:
		return p.snapshot, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func TestResponderReadsOverlapAndExcludeMutation(t *testing.T) {
	loop, _ := speedLoop()
	provider := barrierSnapshot{snapshot: sdktest.Snapshot{NamespaceName: "app"}, entered: make(chan struct{}, 2), release: make(chan struct{})}
	loop.Provider = provider
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	results := make(chan ToolResponse, 2)
	for _, command := range []string{"status", "detect"} {
		go func() { results <- loop.Handle(ctx, ToolRequest{Command: command}) }()
	}
	// Both must enter Snapshot before either is released. The test timeout is
	// only a deadlock watchdog; overlap is established by this barrier.
	<-provider.entered
	<-provider.entered
	if loop.operationMu.TryLock() {
		loop.operationMu.Unlock()
		t.Fatal("a mutation can overlap reads")
	}
	close(provider.release)
	for range 2 {
		if r := <-results; r.Error != "" {
			t.Fatal(r.Error)
		}
	}
	if len(loop.ToolCalls()) != 2 {
		t.Fatal("concurrent read evidence lost")
	}
}

type speedMatchingProgram struct{ diagnoses atomic.Int32 }

func (*speedMatchingProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "synthetic-match"}
}
func (p *speedMatchingProgram) Diagnose(context.Context, program.ReadView, program.Bindings) (program.Verdict, error) {
	p.diagnoses.Add(1)
	return program.Verdict{Result: program.Match}, nil
}
func (*speedMatchingProgram) Mitigate(context.Context, program.ReadView, program.Bindings) (program.Plan, error) {
	panic("observation prepared a repair")
}
func TestResponderObservationsDoNotPrepareRepairs(t *testing.T) {
	loop, _ := speedLoop()
	p := &speedMatchingProgram{}
	registry, err := program.NewRegistry([]program.Diagnoser{p}, []program.Mitigator{p})
	if err != nil {
		t.Fatal(err)
	}
	loop.Programs = registry
	for _, command := range []string{"status", "diagnose"} {
		r := loop.Handle(context.Background(), ToolRequest{Command: command})
		if r.Error != "" {
			t.Fatal(r.Error)
		}
		if command == "status" && p.diagnoses.Load() != 0 {
			t.Fatal("status diagnosed programs")
		}
	}
	if p.diagnoses.Load() != 1 {
		t.Fatal("diagnose did not run the matching program exactly once")
	}
}

type countedDiffView struct {
	program.ReadView
	source, baseline *atomic.Int32
}

func (v countedDiffView) DiffAgainstSource(sdk.ObjectRef) (program.Diff, error) {
	v.source.Add(1)
	return program.Diff{}, nil
}
func (v countedDiffView) DiffAgainstBaseline(sdk.ObjectRef) (program.Diff, error) {
	v.baseline.Add(1)
	return program.Diff{}, nil
}
func TestRemoteDiffPreparationBudget(t *testing.T) {
	loop, _ := speedLoop()
	var source, baseline atomic.Int32
	loop.ReadView = func(view sdk.DetectionContext) program.ReadView {
		return countedDiffView{ReadView: program.ReadFrom(view), source: &source, baseline: &baseline}
	}
	for cycle := int32(1); cycle <= 2; cycle++ {
		response := loop.Handle(context.Background(), ToolRequest{Command: "diagnose"})
		if response.Error != "" {
			t.Fatal(response.Error)
		}
		if source.Load() != cycle*150 || baseline.Load() != cycle*150 {
			t.Fatalf("diffs must be once per object per fresh snapshot: source=%d baseline=%d", source.Load(), baseline.Load())
		}
	}
}

// Closing a tool must neither wait for a slow diagnosis nor retire its host
// while the call still uses it. Two diagnoses must be able to hold the lease.
func TestSnapshotLeaseAllowsConcurrentCallsAndDrainsOnClose(t *testing.T) {
	loop, host := speedLoop()
	view := NewCycleReadView(program.ReadFrom(sdktest.Snapshot{NamespaceName: "app"}))
	remote := loop.Detectors[0].(*remoteDetector).remote
	ctx, closeScope := withMemoryScope(context.Background())
	_, _, first, err := remote.scopedCapture(ctx, view)
	if err != nil {
		t.Fatal(err)
	}
	secondReady := make(chan func(), 1)
	go func() {
		_, _, release, err := remote.scopedCapture(ctx, view)
		if err != nil {
			secondReady <- nil
			return
		}
		secondReady <- release
	}()
	second := <-secondReady
	if second == nil {
		t.Fatal("second call failed")
	}
	closeScope()
	if host.releases.Load() != 0 {
		t.Fatal("closed a host still in use")
	}
	first()
	if host.releases.Load() != 0 {
		t.Fatal("second call still uses the host")
	}
	second()
	if host.captures.Load() != 1 || host.releases.Load() != 1 {
		t.Fatal("shared lease not released exactly once")
	}
	if _, _, _, err := remote.scopedCapture(ctx, view); err == nil {
		t.Fatal("closed scope accepted a new caller")
	}
}

type speedSource struct{ PlanSource }

func (speedSource) AcceptedManifest(_ context.Context, _ string, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	return &unstructured.Unstructured{Object: map[string]any{"apiVersion": ref.APIVersion, "kind": ref.Kind, "metadata": map[string]any{"name": ref.Name, "namespace": ref.Namespace}}}, nil
}
