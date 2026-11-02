package runtime

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

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
