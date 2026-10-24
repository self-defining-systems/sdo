package servicehealth_test

import (
	"context"
	"strings"
	"testing"
	"time"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/servicehealth"
)

func hpaSnapshot() sdktest.Snapshot {
	s := healthyHotel()
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	s.ObservedAtTime = &now
	s.DeploymentList[0].Spec.Selector = &metav1.LabelSelector{MatchLabels: frontendLabels}
	s.PodList[0].Spec.Containers = []corev1.Container{{Name: "frontend"}}
	target := int32(60)
	generation := int64(1)
	s.HPAList = []autoscalingv2.HorizontalPodAutoscaler{{ObjectMeta: metav1.ObjectMeta{Name: "capacity", Namespace: namespace, Generation: 1}, Spec: autoscalingv2.HorizontalPodAutoscalerSpec{ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{APIVersion: "apps/v1", Kind: "Deployment", Name: "frontend"}, MaxReplicas: 5, Metrics: []autoscalingv2.MetricSpec{{Type: autoscalingv2.ResourceMetricSourceType, Resource: &autoscalingv2.ResourceMetricSource{Name: corev1.ResourceCPU, Target: autoscalingv2.MetricTarget{Type: autoscalingv2.UtilizationMetricType, AverageUtilization: &target}}}}}, Status: autoscalingv2.HorizontalPodAutoscalerStatus{ObservedGeneration: &generation, CurrentReplicas: 1, DesiredReplicas: 1, Conditions: []autoscalingv2.HorizontalPodAutoscalerCondition{{Type: autoscalingv2.ScalingActive, Status: corev1.ConditionFalse, Reason: "FailedGetResourceMetric", Message: "missing request for cpu in container frontend", LastTransitionTime: metav1.NewTime(now.Add(-time.Minute))}}}}}
	return s
}

func TestReadyEndpointsStillReportsMissingEffectiveHPARequest(t *testing.T) {
	s := hpaSnapshot()
	findings, err := servicehealth.NewReadyEndpointsDetector(spec()).Detect(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 1 || findings[0].RuleID != "hpa-cpu-request-missing" || findings[0].PrimaryResource.Kind != "HorizontalPodAutoscaler" {
		t.Fatalf("Ready app hid broken autoscaling control plane: %v", findings)
	}
	if !strings.Contains(findings[0].Evidence, "frontend") || findings[0].DetectorID != "service-endpoints" || findings[0].Severity != sdk.SeverityCritical {
		t.Fatalf("HPA finding lacks localizing evidence: %v", findings)
	}
}

func TestHPAHealthyAndStaleNearMisses(t *testing.T) {
	cases := map[string]func(*sdktest.Snapshot){
		"no HPA":          func(s *sdktest.Snapshot) { s.HPAList = nil },
		"other namespace": func(s *sdktest.Snapshot) { s.HPAList[0].Namespace = "other" },
		"scaling active despite missing container requests": func(s *sdktest.Snapshot) { s.HPAList[0].Status.Conditions[0].Status = corev1.ConditionTrue },
		"scaling disabled deliberately":                     func(s *sdktest.Snapshot) { s.HPAList[0].Status.Conditions[0].Reason = "ScalingDisabled" },
		"normal scaling limit": func(s *sdktest.Snapshot) {
			s.HPAList[0].Status.Conditions = []autoscalingv2.HorizontalPodAutoscalerCondition{{Type: autoscalingv2.ScalingLimited, Status: corev1.ConditionTrue}}
		},
		"recent metric startup": func(s *sdktest.Snapshot) {
			s.HPAList[0].Status.Conditions[0].LastTransitionTime = metav1.NewTime(s.ObservedAt().Add(-time.Second))
		},
		"future transition": func(s *sdktest.Snapshot) {
			s.HPAList[0].Status.Conditions[0].LastTransitionTime = metav1.NewTime(s.ObservedAt().Add(time.Minute))
		},
		"old generation": func(s *sdktest.Snapshot) { s.HPAList[0].Generation++ },
		"deleting HPA":   func(s *sdktest.Snapshot) { at := metav1.NewTime(s.ObservedAt()); s.HPAList[0].DeletionTimestamp = &at },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			s := hpaSnapshot()
			mutate(&s)
			if got := detect(t, s); len(got) != 0 {
				t.Fatalf("healthy/unsupported observation fired: %v", got)
			}
		})
	}
}

func TestHPAUsesAdmittedPodRequestsAndSpecificContainerMetric(t *testing.T) {
	s := hpaSnapshot()
	s.PodList[0].Spec.Containers[0].Resources.Requests = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")}
	// Correct requests do not fabricate metric success while ScalingActive is
	// still False. The finding changes to the observed generic control failure.
	got := detect(t, s)
	if len(got) != 1 || got[0].RuleID != "hpa-scaling-inactive" {
		t.Fatalf("request edit hid unverified metrics: %v", got)
	}
	s.HPAList[0].Status.Conditions[0].Status = corev1.ConditionTrue
	if got := detect(t, s); len(got) != 0 {
		t.Fatalf("admission-defaulted request was missed: %v", got)
	}
	s.HPAList[0].Status.Conditions[0].Status = corev1.ConditionFalse
	target := int32(60)
	s.HPAList[0].Spec.Metrics = []autoscalingv2.MetricSpec{{Type: autoscalingv2.ContainerResourceMetricSourceType, ContainerResource: &autoscalingv2.ContainerResourceMetricSource{Name: corev1.ResourceCPU, Container: "frontend", Target: autoscalingv2.MetricTarget{Type: autoscalingv2.UtilizationMetricType, AverageUtilization: &target}}}}
	s.PodList[0].Spec.Containers = append(s.PodList[0].Spec.Containers, corev1.Container{Name: "unrelated-sidecar"})
	got = detect(t, s)
	if len(got) != 1 || got[0].RuleID != "hpa-scaling-inactive" {
		t.Fatalf("container metric blamed unrelated sidecar: %v", got)
	}
	s.PodList[0].Spec.Containers[0].Resources.Requests = nil
	got = detect(t, s)
	if len(got) != 1 || got[0].RuleID != "hpa-cpu-request-missing" || strings.Contains(got[0].Evidence, "unrelated-sidecar") {
		t.Fatalf("container-local CPU evidence incorrect: %v", got)
	}
}

func TestHPAMetricSemanticsAndResourceScope(t *testing.T) {
	s := hpaSnapshot()
	s.HPAList[0].Spec.Metrics[0].Resource.Target.Type = autoscalingv2.AverageValueMetricType
	got := detect(t, s)
	if len(got) != 1 || got[0].RuleID != "hpa-scaling-inactive" {
		t.Fatalf("absolute CPU target falsely requires request: %v", got)
	}
	s = hpaSnapshot()
	s.PodList[0].Namespace = "other"
	got = detect(t, s)
	if len(got) != 1 || got[0].RuleID != "hpa-scaling-inactive" {
		t.Fatalf("foreign Pod affected target CPU cause: %v", got)
	}
	s = hpaSnapshot()
	s.DeploymentList[0].Name = "unrelated"
	got = detect(t, s)
	if len(got) != 1 || got[0].RuleID != "hpa-scaling-inactive" {
		t.Fatalf("unrelated Deployment proved request cause: %v", got)
	}
	s = hpaSnapshot()
	s.PodList[0].Spec.Containers[0].Resources.Requests = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("0")}
	got = detect(t, s)
	if len(got) != 1 || got[0].RuleID != "hpa-cpu-request-missing" {
		t.Fatalf("zero request treated as computable: %v", got)
	}
}

func TestHPAMissingRequestProofIsStableAcrossConditionOrder(t *testing.T) {
	s := hpaSnapshot()
	secondary := s.HPAList[0].Status.Conditions[0]
	secondary.Type = autoscalingv2.AbleToScale
	secondary.Reason = "FailedGetScale"
	secondary.Message = "cannot read target scale"
	s.HPAList[0].Status.Conditions = append([]autoscalingv2.HorizontalPodAutoscalerCondition{secondary}, s.HPAList[0].Status.Conditions...)
	got := detect(t, s)
	if len(got) != 1 || got[0].RuleID != "hpa-cpu-request-missing" {
		t.Fatalf("condition array order hid observed CPU request cause: %v", got)
	}
}
