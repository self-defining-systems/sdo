package runtime

import (
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/internal/capturefixture"
	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/sdk/sdktest"
)

func TestUndetectedRepairSurvivesFailedReflectionProjection(t *testing.T) {
	at := time.Unix(100, 0)
	ref := &contracts.ObjectRef{ApiVersion: "v1", Kind: "Service", Namespace: "demo", Name: "front"}
	outcome := &contracts.OutcomeRecord{Classification: "failed", RepairActions: []*contracts.RepairActionReceipt{{ActionId: "recorded-service-patch", Success: true, Resources: []*contracts.ObjectRef{ref}, StartedAt: timestamppb.New(at.Add(time.Second)), CompletedAt: timestamppb.New(at.Add(2 * time.Second))}}}
	capture := t.TempDir()
	for phase, port := range map[string]int32{"open@pre-apply": 9999, "fixed": 8080} {
		observed := at
		if phase == "fixed" {
			observed = at.Add(3 * time.Second)
		}
		snapshot := sdktest.Snapshot{NamespaceName: "demo", ObservedAtTime: &observed, ServiceList: []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "front", Namespace: "demo", UID: "same"}, Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 80, TargetPort: intstr.FromInt32(port)}}}}}}
		capturefixture.WriteBoundary(t, filepath.Join(capture, phase+".json"), snapshot, "demo")
	}
	outcome.UndetectedCauses = validate.CaptureUndetectedCauses(outcome, capture)
	if len(outcome.UndetectedCauses) != 1 {
		t.Fatal("missing controller-captured transition")
	}
	projected := partialReflectionOutcome(outcome)
	if len(projected.GetConfirmedRootCauses()) != 0 || len(projected.GetRepairActions()) != 1 || len(projected.GetUndetectedCauses()) != 1 || len(projected.GetReflectionEligibility().GetCauses()) != 1 || projected.ReflectionEligibility.Causes[0].LearningBasis != "observed_transition" {
		t.Fatalf("undetected learning omitted or promoted to a confirmed cause: %v", projected)
	}
	if len(validate.VerifiedReflectionCauses(projected)) != 1 {
		t.Fatal("projection dropped executable learning authority")
	}
}
