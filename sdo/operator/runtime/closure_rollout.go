package runtime

import (
	"fmt"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sdo.dev/operator/sdk"
)

// Closure must observe the desired workload generation, including after raw
// recorded actions. A Ready replica from the old template is not recovery.
func closureRolloutFindings(snapshot sdk.DetectionContext, namespace string, refs []sdk.ObjectRef) []sdk.Finding {
	if snapshot == nil {
		return nil
	}
	var findings []sdk.Finding
	sets, pods, events, services := snapshot.ReplicaSets(), snapshot.Pods(), snapshot.Events(), snapshot.Services()
	for _, d := range snapshot.Deployments() {
		if d.Namespace != namespace || d.Labels["sdo.dev/responder-helper"] == "true" || !rolloutRelated(d, refs, sets, pods, services) {
			continue
		}
		observation := deploymentRolloutBlocker(&d, sets, pods, events)
		if !observation.pending {
			continue
		}
		blocker := observation.blocker
		evidence := blocker.Reason
		if len(blocker.WarningEvents) > 0 {
			evidence += "; " + strings.Join(blocker.WarningEvents, "; ")
		}
		ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: d.Namespace, Name: d.Name}
		findings = append(findings, sdk.Finding{DetectorID: "controller.rollout", RuleID: "deployment-rollout-pending", Fingerprint: fmt.Sprintf("controller.rollout/deployment-rollout-pending/%s/%s", d.Namespace, d.Name), PrimaryResource: ref, Severity: sdk.SeverityCritical, Status: sdk.FindingActive, Summary: fmt.Sprintf("Deployment %s rollout pending: %s", d.Name, observation.summary), Evidence: evidence})
	}
	sortFindings(findings)
	return findings
}

// Limit this closure prerequisite to findings and recorded writes. An unrelated
// workload cannot expand the health judge's incident objective.
func rolloutRelated(d appsv1.Deployment, refs []sdk.ObjectRef, sets []appsv1.ReplicaSet, pods []corev1.Pod, services []corev1.Service) bool {
	for _, ref := range refs {
		if ref.Namespace != d.Namespace {
			continue
		}
		switch ref.Kind {
		case "*":
			return true
		case "Deployment":
			if ref.Name == d.Name {
				return true
			}
		case "Service":
			for _, service := range services {
				if service.Namespace == ref.Namespace && service.Name == ref.Name && len(service.Spec.Selector) > 0 && labels.SelectorFromSet(service.Spec.Selector).Matches(labels.Set(d.Spec.Template.Labels)) {
					return true
				}
			}
		case "ReplicaSet", "Pod":
			for _, rs := range sets {
				if rs.Namespace != d.Namespace || !rolloutOwnedBy(rs.OwnerReferences, "Deployment", d.UID) {
					continue
				}
				if ref.Kind == "ReplicaSet" && ref.Name == rs.Name {
					return true
				}
				if ref.Kind == "Pod" {
					for _, pod := range pods {
						if pod.Namespace == ref.Namespace && pod.Name == ref.Name && rolloutOwnedBy(pod.OwnerReferences, "ReplicaSet", rs.UID) {
							return true
						}
					}
				}
			}
		}
	}
	return false
}
