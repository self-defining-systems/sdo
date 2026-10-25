package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func TestMitigationReturnsWithUnrelatedFaultBeforeGlobalHealthBudget(t *testing.T) {
	for _, command := range []string{"apply", "mitigate"} {
		t.Run(command, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			client := fake.NewSimpleClientset()
			for _, name := range []string{"first", "peer"} {
				_, err := client.CoreV1().Services("app").Create(ctx, &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app"}, Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 80, TargetPort: intstr.FromInt32(9999)}}}}, metav1.CreateOptions{})
				if err != nil {
					t.Fatal(err)
				}
			}
			provider := fastloopObjects{client}
			registry, err := program.NewRegistry([]program.Diagnoser{fastloopPortProgram{}}, []program.Mitigator{fastloopPortProgram{}})
			if err != nil {
				t.Fatal(err)
			}
			snapshot, _ := provider.Snapshot(ctx)
			findings, _ := (fastloopDetector{}).Detect(ctx, snapshot)
			loop := NewResponderLoop(IncidentRequest{IncidentID: "two-fault", Namespace: "app", RepairPolicy: "recorded-actions", Findings: findings}, registry, provider, []sdk.Detector{fastloopDetector{}}, PlanExecutor{Client: TypedPlanClient{Client: client}, Rollouts: KubernetesPlanRollouts{Client: client}, Verifier: PlanHealthVerifier{Detectors: []sdk.Detector{fastloopDetector{}}, Provider: provider, Timeout: 5 * time.Second, PollInterval: time.Second}}, 3)
			loop.ReadView = func(snapshot sdk.DetectionContext) program.ReadView {
				return &sdktest.ReplayReadView{Snapshot: snapshot.(sdktest.Snapshot)}
			}
			loop.MenuConfig = productionResponderMenu()
			server := SessionToolServer{Directory: filepath.Join(t.TempDir(), "tools"), Handle: func(_ context.Context, r ToolRequest) ToolResponse { return loop.Handle(ctx, r) }}
			if err = server.Prepare(); err != nil {
				t.Fatal(err)
			}
			defer server.Close()
			plan := loop.Handle(ctx, ToolRequest{Command: "plan", ProgramID: "service-targetport-mismatch", Bindings: program.Bindings{Objects: program.ObjectBindings{"service": {APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "first"}}}})
			request := ToolRequest{Command: "apply", PlanIDs: []string{plan.PlanID}}
			if command == "mitigate" {
				menu := loop.Handle(ctx, ToolRequest{Command: "menu", ProgramID: "service-targetport-mismatch", Parameters: map[string]string{"service": "Service/app/first"}})
				var optionID string
				for _, option := range menu.Menu.GetOptions() {
					if option.ProgramId == "service-targetport-mismatch" {
						optionID = option.OptionId
					}
				}
				if menu.Error != "" || optionID == "" {
					t.Fatalf("menu: %+v", menu)
				}
				request = ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{Version: 1, OptionId: optionID, RequestId: "nonblocking", RoundLimit: 1}, Parameters: map[string]string{"service": "Service/app/first"}}
			}
			encoded, err := json.Marshal(request)
			if err != nil {
				t.Fatal(err)
			}
			started := time.Now()
			response := exchangeSelfCheck(t, &server, string(encoded))
			if response.Error != "" || ctx.Err() != nil || time.Since(started) >= time.Second {
				t.Fatalf("mitigation waited for unrelated global health: duration=%s response=%+v context=%v", time.Since(started), response, ctx.Err())
			}
			if response.MitigationHealth == nil || !response.MitigationHealth.TargetedHealthy || len(response.MitigationHealth.TargetedFindings) != 0 || len(response.MitigationHealth.Resources) != 1 || response.MitigationHealth.Resources[0].Name != "first" {
				t.Fatalf("missing scoped observation: %+v", response)
			}
			if !response.TargetedHealthy || response.Healthy || len(response.Findings) != 1 || response.Findings[0].PrimaryResource.Name != "peer" {
				t.Fatalf("target/peer verification: %+v", response)
			}
		})
	}
}

func TestMitigationDoesNotClearItsOwnRemainingFinding(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "first", Namespace: "app"}, Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 80, TargetPort: intstr.FromInt32(9999)}}}}
	client := fake.NewSimpleClientset(service)
	provider := fastloopObjects{client}
	executor := PlanExecutor{Client: TypedPlanClient{Client: client}, Rollouts: KubernetesPlanRollouts{Client: client}, Verifier: PlanHealthVerifier{Detectors: []sdk.Detector{fastloopDetector{}}, Provider: provider, Timeout: 5 * time.Second}}
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "first"}
	result, err := executor.Apply(ctx, IncidentRequest{Namespace: "app", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.SetField{Ref: ref, Path: "spec.ports[0].targetPort", Value: testPlanValue(9999)}}}, false)
	if err != nil || ctx.Err() != nil || !result.Applied || !result.VerificationCompleted || result.VerificationPassed {
		t.Fatalf("own unresolved health must return promptly as unhealthy: %+v error=%v", result, err)
	}
	if result.MitigationHealth == nil || len(result.MitigationHealth.TargetedFindings) != 1 || result.MitigationHealth.TargetedFindings[0].PrimaryResource.Name != "first" {
		t.Fatalf("own finding hidden by scope: %+v", result.MitigationHealth)
	}
}

type unavailableMitigationHealth struct{ fastloopDetector }

func (unavailableMitigationHealth) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	return nil, fmt.Errorf("prober unavailable")
}

func TestMitigationUnavailableHealthReturnsWithoutUndo(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "first", Namespace: "app"}, Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 80, TargetPort: intstr.FromInt32(9999)}}}}
	client := fake.NewSimpleClientset(service)
	provider := fastloopObjects{client}
	executor := PlanExecutor{Client: TypedPlanClient{Client: client}, Rollouts: KubernetesPlanRollouts{Client: client}, Verifier: PlanHealthVerifier{Detectors: []sdk.Detector{unavailableMitigationHealth{}}, Provider: provider, Timeout: 5 * time.Second}}
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "first"}
	result, err := executor.Apply(ctx, IncidentRequest{Namespace: "app", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.SetField{Ref: ref, Path: "spec.ports[0].targetPort", Value: testPlanValue(8080)}}}, false)
	if err == nil || !strings.Contains(err.Error(), "prober unavailable") || ctx.Err() != nil || !result.Applied || result.RolledBack || result.VerificationPassed {
		t.Fatalf("unavailable health must be prompt, unverified and leave writes in place: %+v error=%v", result, err)
	}
	live, getErr := client.CoreV1().Services("app").Get(ctx, "first", metav1.GetOptions{})
	if getErr != nil || live.Spec.Ports[0].TargetPort.IntVal != 8080 {
		t.Fatalf("health outage undid repair: live=%v error=%v", live, getErr)
	}
}
