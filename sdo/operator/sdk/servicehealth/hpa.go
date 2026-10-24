package servicehealth

import (
	"fmt"
	"sort"
	"strings"
	"time"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	"sdo.dev/operator/sdk"
)

const (
	// RuleHPAMissingCPURequest: a utilization HPA selects a ready Pod whose
	// target containers have no positive effective CPU request.
	RuleHPAMissingCPURequest = "hpa-cpu-request-missing"
	// RuleHPAScalingInactive: the autoscaler reports a persistent metric or
	// scale-target failure, independently of workload/endpoint readiness.
	RuleHPAScalingInactive = "hpa-scaling-inactive"
)

// HPA observations are already supplied by the production and replay contexts.
// Keeping this optional preserves compatibility with older third-party views.
type hpaObservation interface {
	HorizontalPodAutoscalers() []autoscalingv2.HorizontalPodAutoscaler
}

func (d readyEndpointsDetector) hpaFindings(ctx sdk.DetectionContext) []sdk.Finding {
	observed, ok := ctx.(hpaObservation)
	if !ok {
		return nil
	}
	var findings []sdk.Finding
	for _, hpa := range observed.HorizontalPodAutoscalers() {
		if hpa.Namespace != ctx.Namespace() || hpa.DeletionTimestamp != nil {
			continue
		}
		condition := hpaFailureCondition(ctx, hpa)
		if condition == nil {
			continue
		}
		missing := missingEffectiveCPURequests(ctx, hpa)
		rule, evidence := RuleHPAMissingCPURequest, ""
		if len(missing) > 0 && condition.Type == autoscalingv2.ScalingActive && condition.Reason == "FailedGetResourceMetric" {
			evidence = fmt.Sprintf("HorizontalPodAutoscaler %s uses CPU utilization for %s/%s, but ready target Pod containers have no positive effective CPU request: %s. CPU utilization cannot be computed without requests; existing readiness does not verify autoscaling.", hpa.Name, hpa.Spec.ScaleTargetRef.Kind, hpa.Spec.ScaleTargetRef.Name, strings.Join(missing, ", "))
		} else {
			rule = RuleHPAScalingInactive
			evidence = fmt.Sprintf("HorizontalPodAutoscaler %s targets %s/%s and reports %s=False (%s): %s", hpa.Name, hpa.Spec.ScaleTargetRef.Kind, hpa.Spec.ScaleTargetRef.Name, condition.Type, condition.Reason, condition.Message)
		}
		related := []sdk.ObjectRef{}
		target := hpa.Spec.ScaleTargetRef
		if target.Kind != "" && target.Name != "" {
			related = append(related, sdk.ObjectRef{APIVersion: target.APIVersion, Kind: target.Kind, Namespace: hpa.Namespace, Name: target.Name})
		}
		findings = append(findings, sdk.Finding{DetectorID: d.spec.ID, RuleID: rule, Status: sdk.FindingActive, Severity: sdk.SeverityCritical, Summary: fmt.Sprintf("HorizontalPodAutoscaler %s cannot operate its scaling control loop", hpa.Name), Evidence: evidence, PrimaryResource: sdk.ObjectRef{APIVersion: "autoscaling/v2", Kind: "HorizontalPodAutoscaler", Namespace: hpa.Namespace, Name: hpa.Name}, RelatedResources: related, Playbooks: append([]string(nil), d.spec.Playbooks...)})
	}
	sort.Slice(findings, func(i, j int) bool { return findings[i].PrimaryResource.Name < findings[j].PrimaryResource.Name })
	return findings
}

func missingEffectiveCPURequests(ctx sdk.DetectionContext, hpa autoscalingv2.HorizontalPodAutoscaler) []string {
	target := hpa.Spec.ScaleTargetRef
	if target.Kind != "Deployment" || (target.APIVersion != "apps/v1" && target.APIVersion != "") {
		return nil
	}
	all := false
	containers := map[string]bool{}
	for _, metric := range hpa.Spec.Metrics {
		if metric.Type == autoscalingv2.ResourceMetricSourceType && metric.Resource != nil && metric.Resource.Name == corev1.ResourceCPU && metric.Resource.Target.Type == autoscalingv2.UtilizationMetricType {
			all = true
		}
		if metric.Type == autoscalingv2.ContainerResourceMetricSourceType && metric.ContainerResource != nil && metric.ContainerResource.Name == corev1.ResourceCPU && metric.ContainerResource.Target.Type == autoscalingv2.UtilizationMetricType {
			containers[metric.ContainerResource.Container] = true
		}
	}
	if !all && len(containers) == 0 {
		return nil
	}
	var missing []string
	for _, deployment := range ctx.Deployments() {
		if deployment.Namespace != hpa.Namespace || deployment.Name != target.Name || deployment.Spec.Selector == nil {
			continue
		}
		selector, err := metav1.LabelSelectorAsSelector(deployment.Spec.Selector)
		if err != nil || selector.Empty() {
			continue
		}
		for _, pod := range ctx.Pods() {
			if pod.Namespace != hpa.Namespace || pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning || !podReady(pod) || !selector.Matches(labels.Set(pod.Labels)) {
				continue
			}
			// Actual admitted Pod resources include LimitRange/limit defaulting. A
			// missing request in the Deployment template alone is not evidence.
			for _, container := range pod.Spec.Containers {
				if !all && !containers[container.Name] {
					continue
				}
				request, exists := container.Resources.Requests[corev1.ResourceCPU]
				if !exists || request.Sign() <= 0 {
					missing = append(missing, pod.Name+"/"+container.Name)
				}
			}
		}
	}
	sort.Strings(missing)
	return missing
}

func hpaFailureCondition(ctx sdk.DetectionContext, hpa autoscalingv2.HorizontalPodAutoscaler) *autoscalingv2.HorizontalPodAutoscalerCondition {
	if hpa.Status.ObservedGeneration != nil && *hpa.Status.ObservedGeneration < hpa.Generation {
		return nil
	}
	now := sdk.ObservationTime(ctx)
	for _, conditionType := range []autoscalingv2.HorizontalPodAutoscalerConditionType{autoscalingv2.ScalingActive, autoscalingv2.AbleToScale} {
		for _, condition := range hpa.Status.Conditions {
			if condition.Type != conditionType {
				continue
			}
			if condition.Status != corev1.ConditionFalse || condition.Reason == "ScalingDisabled" {
				continue
			}
			if condition.Type != autoscalingv2.ScalingActive && !(condition.Type == autoscalingv2.AbleToScale && strings.HasPrefix(condition.Reason, "Failed")) {
				continue
			}
			// Missing, recent or future timestamps do not establish a persistent failure.
			if now.IsZero() || condition.LastTransitionTime.IsZero() || now.Sub(condition.LastTransitionTime.Time) < 30*time.Second {
				continue
			}
			result := condition
			return &result
		}
	}
	return nil
}
