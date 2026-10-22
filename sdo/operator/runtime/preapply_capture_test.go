package runtime

import (
	"context"
	"encoding/json"
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

func TestPreApplyCaptureFailurePreventsMutation(t *testing.T) {
	loop := NewResponderLoop(IncidentRequest{IncidentID: "i", Namespace: "app"}, nil, nil, nil, PlanExecutor{}, 1)
	loop.CorpusSessionRoot = t.TempDir()
	response := loop.Handle(context.Background(), ToolRequest{Command: "pre-apply"})
	if response.Error == "" {
		t.Fatal("capture acknowledged without a provider")
	}
}
