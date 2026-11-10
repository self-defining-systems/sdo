// Package servicehealth holds app-agnostic structural health checks that
// localize synthetic-traffic failures: they say which Service stopped routing
// to which workload, not only that a user path fails.
package servicehealth

import (
	"context"
	"fmt"
	"sort"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"

	"sdo.dev/operator/sdk"
)

const (
	RuleSelectorMatchesNoPods = "service-selector-matches-no-pods"
	RuleNoReadyEndpoints      = "service-has-no-ready-endpoints"
)

// EndpointWatches are the resources whose changes can change the verdict.
func EndpointWatches() []sdk.WatchKind {
	return []sdk.WatchKind{
		{APIVersion: "v1", Kind: "Service"},
		{APIVersion: "v1", Kind: "Endpoints"},
		{APIVersion: "v1", Kind: "Pod"},
		{APIVersion: "apps/v1", Kind: "Deployment"},
		{APIVersion: "apps/v1", Kind: "StatefulSet"},
		{APIVersion: "autoscaling/v2", Kind: "HorizontalPodAutoscaler"},
	}
}

type readyEndpointsDetector struct {
	spec sdk.DetectorSpec
}

// NewReadyEndpointsDetector reports every Service with a selector that has no
// ready endpoint. A scaled-to-zero backend is a finding: the live replica count
// is what a scale-down fault changes, so it never says the workload was meant
// to be off. Only an autoscaler that may reach zero replicas declares that.
// ExternalName Services never have endpoints and are skipped.
func NewReadyEndpointsDetector(spec sdk.DetectorSpec) sdk.Detector {
	return readyEndpointsDetector{spec: spec}
}

func (d readyEndpointsDetector) Spec() sdk.DetectorSpec { return d.spec }

func (d readyEndpointsDetector) Detect(_ context.Context, ctx sdk.DetectionContext) ([]sdk.Finding, error) {
	deployments := ctx.Deployments()
	workloads := scalableWorkloads(ctx)
	autoscalers := ctx.HorizontalPodAutoscalers()
	findings := make([]sdk.Finding, 0)
	for _, service := range ctx.Services() {
		if !sdk.ServiceExpectsEndpoints(service) || len(service.Spec.Selector) == 0 {
			continue
		}
		if ctx.ReadyEndpointCountForService(service.Namespace, service.Name) > 0 {
			continue
		}
		selector := labels.SelectorFromSet(service.Spec.Selector)
		selected := ctx.PodsForService(service.Namespace, service.Name)
		matching := matchingWorkloads(workloads, service.Namespace, selector)
		if len(selected) == 0 && len(matching) > 0 && allMayBeOff(matching, autoscalers) {
			continue
		}
		ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: service.Namespace, Name: service.Name}
		if len(selected) == 0 && len(matching) == 0 {
			findings = append(findings, d.selectorFinding(service, ref, deployments))
			continue
		}
		findings = append(findings, d.unreadyFinding(service, ref, selected, matching))
	}
	return findings, nil
}

func (d readyEndpointsDetector) selectorFinding(
	service corev1.Service,
	ref sdk.ObjectRef,
	deployments []appsv1.Deployment,
) sdk.Finding {
	candidates, mismatches := nearestDeployments(deployments, service)
	related := make([]sdk.ObjectRef, 0, len(candidates))
	for _, candidate := range candidates {
		related = append(related, sdk.ObjectRef{
			APIVersion: "apps/v1", Kind: "Deployment", Namespace: candidate.Namespace, Name: candidate.Name,
		})
	}
	evidence := fmt.Sprintf("Service %s selector {%s} matches no pod and no Deployment pod template.",
		service.Name, renderLabels(service.Spec.Selector))
	for index, candidate := range candidates {
		evidence += fmt.Sprintf(" Deployment %s's pod template {%s} matches every selector label except %s.",
			candidate.Name, renderLabels(candidate.Spec.Template.Labels), strings.Join(mismatches[index], ", "))
	}
	return sdk.Finding{
		DetectorID: d.spec.ID, RuleID: RuleSelectorMatchesNoPods, Status: sdk.FindingActive,
		Severity: sdk.SeverityCritical,
		Summary: fmt.Sprintf("Service %s has no ready endpoints because its selector matches no pods",
			service.Name),
		Evidence: evidence, PrimaryResource: ref, RelatedResources: related,
		Playbooks: append([]string(nil), d.spec.Playbooks...),
		Metadata:  map[string]any{"selector": service.Spec.Selector},
	}
}

func (d readyEndpointsDetector) unreadyFinding(service corev1.Service, ref sdk.ObjectRef, selected []corev1.Pod, matching []workload) sdk.Finding {
	names := make([]string, 0, len(selected))
	for _, pod := range selected {
		names = append(names, fmt.Sprintf("%s (%s, ready=%t)", pod.Name, pod.Status.Phase, podReady(pod)))
	}
	sort.Strings(names)
	evidence := fmt.Sprintf("Service %s selector {%s} selects %d pod(s), none of them a ready endpoint: %s.",
		service.Name, renderLabels(service.Spec.Selector), len(selected), strings.Join(names, ", "))
	if len(selected) == 0 {
		evidence = fmt.Sprintf("Service %s selector {%s} matches a workload with replicas, but no pod exists.",
			service.Name, renderLabels(service.Spec.Selector))
		if names := zeroReplicaNames(matching); len(names) > 0 {
			evidence = fmt.Sprintf("Service %s selector {%s} matches %s at 0 replicas, so no pod exists.",
				service.Name, renderLabels(service.Spec.Selector), strings.Join(names, ", "))
		}
	}
	return sdk.Finding{
		DetectorID: d.spec.ID, RuleID: RuleNoReadyEndpoints, Status: sdk.FindingActive,
		Severity: sdk.SeverityCritical,
		Summary:  fmt.Sprintf("Service %s has no ready endpoints", service.Name),
		Evidence: evidence, PrimaryResource: ref,
		Playbooks: append([]string(nil), d.spec.Playbooks...),
		Metadata:  map[string]any{"selector": service.Spec.Selector, "selected_pods": len(selected)},
	}
}

// workload is a Deployment or StatefulSet, the kinds that own the pods a
// Service selects.
type workload struct {
	kind      string
	name      string
	namespace string
	replicas  int32
	labels    map[string]string
}

func scalableWorkloads(ctx sdk.DetectionContext) []workload {
	items := make([]workload, 0)
	for _, deployment := range ctx.Deployments() {
		items = append(items, workload{"Deployment", deployment.Name, deployment.Namespace, replicasOrOne(deployment.Spec.Replicas), deployment.Spec.Template.Labels})
	}
	for _, set := range ctx.StatefulSets() {
		items = append(items, workload{"StatefulSet", set.Name, set.Namespace, replicasOrOne(set.Spec.Replicas), set.Spec.Template.Labels})
	}
	return items
}

func replicasOrOne(replicas *int32) int32 {
	if replicas == nil {
		return 1
	}
	return *replicas
}

func matchingWorkloads(workloads []workload, namespace string, selector labels.Selector) []workload {
	matching := make([]workload, 0)
	for _, item := range workloads {
		if item.namespace == namespace && selector.Matches(labels.Set(item.labels)) {
			matching = append(matching, item)
		}
	}
	return matching
}

// allMayBeOff is true when every workload is at zero replicas and an
// autoscaler that may scale it to zero targets it.
func allMayBeOff(workloads []workload, autoscalers []autoscalingv2.HorizontalPodAutoscaler) bool {
	for _, item := range workloads {
		if item.replicas > 0 || !autoscalerAllowsZero(item, autoscalers) {
			return false
		}
	}
	return true
}

func autoscalerAllowsZero(item workload, autoscalers []autoscalingv2.HorizontalPodAutoscaler) bool {
	for _, autoscaler := range autoscalers {
		target := autoscaler.Spec.ScaleTargetRef
		if autoscaler.Namespace == item.namespace && target.Kind == item.kind && target.Name == item.name &&
			autoscaler.Spec.MinReplicas != nil && *autoscaler.Spec.MinReplicas == 0 {
			return true
		}
	}
	return false
}

func zeroReplicaNames(workloads []workload) []string {
	names := make([]string, 0)
	for _, item := range workloads {
		if item.replicas == 0 {
			names = append(names, item.kind+" "+item.name)
		}
	}
	sort.Strings(names)
	return names
}

func matchingDeployments(deployments []appsv1.Deployment, namespace string, selector labels.Selector) []appsv1.Deployment {
	matching := make([]appsv1.Deployment, 0)
	for _, deployment := range deployments {
		if deployment.Namespace == namespace && selector.Matches(labels.Set(deployment.Spec.Template.Labels)) {
			matching = append(matching, deployment)
		}
	}
	return matching
}

// nearestDeployments returns the Deployments whose pod templates share the most
// selector labels with the Service, and the selector labels each one lacks.
func nearestDeployments(deployments []appsv1.Deployment, service corev1.Service) ([]appsv1.Deployment, [][]string) {
	best := 0
	var candidates []appsv1.Deployment
	var mismatches [][]string
	keys := make([]string, 0, len(service.Spec.Selector))
	for key := range service.Spec.Selector {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, deployment := range deployments {
		if deployment.Namespace != service.Namespace {
			continue
		}
		matched := 0
		missing := make([]string, 0)
		for _, key := range keys {
			if deployment.Spec.Template.Labels[key] == service.Spec.Selector[key] {
				matched++
				continue
			}
			missing = append(missing, key+"="+service.Spec.Selector[key])
		}
		switch {
		case matched == 0 || matched < best:
			continue
		case matched > best:
			best, candidates, mismatches = matched, nil, nil
		}
		candidates = append(candidates, deployment)
		mismatches = append(mismatches, missing)
	}
	return candidates, mismatches
}

func podReady(pod corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

func renderLabels(values map[string]string) string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+values[key])
	}
	return strings.Join(parts, ", ")
}
