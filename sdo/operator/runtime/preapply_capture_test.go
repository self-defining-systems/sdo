package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	ktesting "k8s.io/client-go/testing"
	"os"
	"path/filepath"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type captureCheckingClient struct {
	*planFakeClient
	beforePatch func()
}

func (c captureCheckingClient) Patch(ctx context.Context, ref sdk.ObjectRef, kind types.PatchType, patch []byte) error {
	c.beforePatch()
	return c.planFakeClient.Patch(ctx, ref, kind, patch)
}

func TestPreApplyCapturesLateFailureBeforeFirstMutation(t *testing.T) {
	for _, command := range []string{"pre-apply", "apply"} {
		t.Run(command, func(t *testing.T) {
			ctx := context.Background()
			ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
			detect := sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"}, Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 80, TargetPort: intstr.FromInt32(9999)}}}}}, ConfigMapList: []corev1.ConfigMap{{ObjectMeta: metav1.ObjectMeta{Name: "required", Namespace: "app"}}}}
			provider := &changingProvider{snapshot: detect}
			registry, err := program.NewRegistry([]program.Diagnoser{fastloopPortProgram{}}, []program.Mitigator{fastloopPortProgram{}})
			if err != nil {
				t.Fatal(err)
			}
			client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: planObject(ref, map[string]any{"ports": []any{map[string]any{"port": float64(80), "targetPort": float64(9999)}}})}}
			loop := NewResponderLoop(IncidentRequest{IncidentID: "late-failure", Namespace: "app", RepairPolicy: "recorded-actions"}, registry, provider, nil, PlanExecutor{Client: client, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}}, 3)
			loop.CorpusSessionRoot = t.TempDir()
			loop.ReadView = func(s sdk.DetectionContext) program.ReadView {
				return &sdktest.ReplayReadView{Snapshot: s.(sdktest.Snapshot)}
			}
			loop.InitialDiagnose(ctx)
			dir, err := memory.IncidentCorpusDir(loop.CorpusSessionRoot, loop.Request.IncidentID)
			if err != nil {
				t.Fatal(err)
			}
			detectBefore, err := os.ReadFile(filepath.Join(dir, "open@detect.json"))
			if err != nil {
				t.Fatal(err)
			}
			// A required ConfigMap disappears after detect but before the first repair.
			late := detect
			late.ConfigMapList = nil
			provider.snapshot = late
			assertCaptured := func() {
				raw, err := os.ReadFile(filepath.Join(dir, "open@pre-apply.json"))
				if err != nil {
					t.Fatalf("mutation lacks pre-apply capture: %v", err)
				}
				var capture sdktest.Snapshot
				if err := json.Unmarshal(raw, &capture); err != nil {
					t.Fatal(err)
				}
				if len(capture.ConfigMapList) != 0 {
					t.Fatal("pre-apply capture reused healthy detect ConfigMap")
				}
			}
			request := ToolRequest{Command: command}
			if command == "apply" {
				planned := loop.Handle(ctx, ToolRequest{Command: "plan", ProgramID: "service-targetport-mismatch", Bindings: program.Bindings{"service": ref}})
				if planned.Error != "" {
					t.Fatal(planned.Error)
				}
				request.PlanIDs = []string{planned.PlanID}
				loop.Executor.Client = captureCheckingClient{planFakeClient: client, beforePatch: assertCaptured}
			}
			response := loop.Handle(ctx, request)
			if response.Error != "" {
				t.Fatal(response.Error)
			}
			assertCaptured()
			before, err := os.ReadFile(filepath.Join(dir, "open@pre-apply.json"))
			if err != nil {
				t.Fatal(err)
			}
			provider.snapshot = detect
			// An ad hoc call after typed apply shares the first boundary, even on restart.
			restarted := NewResponderLoop(loop.Request, nil, provider, nil, PlanExecutor{}, 3)
			restarted.CorpusSessionRoot = loop.CorpusSessionRoot
			if response := restarted.Handle(ctx, ToolRequest{Command: "pre-apply"}); response.Error != "" {
				t.Fatal(response.Error)
			}
			after, _ := os.ReadFile(filepath.Join(dir, "open@pre-apply.json"))
			detectAfter, _ := os.ReadFile(filepath.Join(dir, "open@detect.json"))
			if string(before) != string(after) || string(detectBefore) != string(detectAfter) {
				t.Fatal("a protected capture was overwritten")
			}
		})
	}
}

func TestPreApplyCaptureFailureReportsEvidenceGap(t *testing.T) {
	loop := NewResponderLoop(IncidentRequest{IncidentID: "i", Namespace: "app"}, nil, nil, nil, PlanExecutor{}, 1)
	loop.CorpusSessionRoot = t.TempDir()
	response := loop.Handle(context.Background(), ToolRequest{Command: "pre-apply"})
	if response.Error == "" {
		t.Fatal("capture acknowledged without a provider")
	}
	assertPreApplyGap(t, loop)
}

func TestTrustedFastPathCapturesBeforeMutation(t *testing.T) {
	loop, client := trustedPortLoop(t, false, true, true)
	loop.CorpusSessionRoot = t.TempDir()
	dir, err := memory.IncidentCorpusDir(loop.CorpusSessionRoot, loop.Request.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	client.PrependReactor("patch", "services", func(ktesting.Action) (bool, kruntime.Object, error) {
		if _, err := os.Stat(filepath.Join(dir, "open@pre-apply.json")); err != nil {
			t.Fatalf("fast path mutation lacks pre-apply capture: %v", err)
		}
		return false, nil, nil
	})
	if attempt := loop.TryTrustedFastPath(context.Background(), 3); !attempt.Resolved {
		t.Fatalf("fast path: %+v", attempt)
	}
}

func BenchmarkFirstMutationCapture(b *testing.B) {
	snapshot := sdktest.Snapshot{NamespaceName: "app"}
	for i := 0; i < 20; i++ {
		name := fmt.Sprintf("component-%d", i)
		snapshot.PodList = append(snapshot.PodList, corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app"}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: name, Image: "toy:v1"}}}})
		snapshot.ServiceList = append(snapshot.ServiceList, corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app"}, Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 80}}}})
		snapshot.ConfigMapList = append(snapshot.ConfigMapList, corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app"}, Data: map[string]string{"config": "stable"}})
	}
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		loop := NewResponderLoop(IncidentRequest{IncidentID: "latency", Namespace: "app"}, nil, staticProvider{snapshot: snapshot}, nil, PlanExecutor{}, 1)
		loop.CorpusSessionRoot = b.TempDir()
		b.StartTimer()
		if err := loop.capturePreApply(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
}

func TestTypedApplyCaptureFailureStillRepairsAndRecordsGap(t *testing.T) {
	ctx := context.Background()
	loop, client := trustedPortLoop(t, false, true, true)
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	planned := loop.Handle(ctx, ToolRequest{Command: "plan", ProgramID: "service-targetport-mismatch", Bindings: program.Bindings{"service": ref}})
	if planned.Error != "" {
		t.Fatal(planned.Error)
	}
	loop.CorpusSessionRoot = t.TempDir()
	provider := loop.Provider
	// Fail only the capture; restore normal reads before verification.
	loop.Provider = nil
	client.PrependReactor("patch", "services", func(ktesting.Action) (bool, kruntime.Object, error) {
		loop.Provider = provider
		return false, nil, nil
	})
	var state []byte
	loop.saveToolState = func(_ context.Context, payload []byte) error {
		state = append([]byte(nil), payload...)
		return nil
	}
	response := loop.Handle(ctx, ToolRequest{Command: "apply", PlanIDs: []string{planned.PlanID}})
	if response.Error != "" || !response.Healthy || loop.LoopsUsed() != 1 {
		t.Fatalf("capture failure blocked repair: %+v", response)
	}
	applications := loop.PlanApplications()
	if len(applications) != 1 || !applications[0].Result.Applied {
		t.Fatalf("repair lacks application evidence: %+v", applications)
	}
	assertPreApplyGap(t, loop)
	restarted := NewResponderLoop(loop.Request, nil, provider, nil, PlanExecutor{}, 3)
	restarted.CorpusSessionRoot = loop.CorpusSessionRoot
	if err := restarted.restoreToolSnapshot(state); err != nil {
		t.Fatal(err)
	}
	if response := restarted.Handle(ctx, ToolRequest{Command: "pre-apply"}); response.Error == "" {
		t.Fatal("a repaired snapshot must not fill the missed first-mutation boundary")
	}
	dir, err := memory.IncidentCorpusDir(loop.CorpusSessionRoot, loop.Request.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "open@pre-apply.json")); !os.IsNotExist(err) {
		t.Fatalf("failed first capture was replaced after repair: %v", err)
	}
}

func TestTrustedFastPathCaptureFailureStillRepairsAndRecordsGap(t *testing.T) {
	loop, client := trustedPortLoop(t, false, true, true)
	loop.CorpusSessionRoot = t.TempDir()
	provider := loop.Provider
	loop.BeforeFastPathApply = func(context.Context, []ProgramRun) error {
		loop.Provider = nil
		return nil
	}
	client.PrependReactor("patch", "services", func(ktesting.Action) (bool, kruntime.Object, error) {
		loop.Provider = provider
		return false, nil, nil
	})
	if attempt := loop.TryTrustedFastPath(context.Background(), 3); !attempt.Resolved {
		t.Fatalf("capture failure blocked trusted repair: %+v", attempt)
	}
	assertPreApplyGap(t, loop)
}

func assertPreApplyGap(t *testing.T, loop *ResponderLoop) {
	t.Helper()
	for _, signal := range loop.FailureSignals() {
		if signal == "pre_apply_capture_gap" {
			return
		}
	}
	t.Fatalf("missing evidence gap: %v", loop.FailureSignals())
}
