// Package crashloopdetector surfaces the crashloop-script-repair playbook for each
// Deployment with a crash-looping Pod, binding the playbook's "deployment" role.
package crashloopdetector

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sdo.dev/operator/sdk"
)

type Detector struct{}

func New() sdk.Detector { return Detector{} }

// Spec must agree with manifest.yaml field for field.
func (Detector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{
		ID:                  "crashloop-script-repair",
		Class:               sdk.DetectorClassIncident,
		Owner:               sdk.DetectorOwnerResponder,
		Description:         "find Deployments whose Pods are crash looping",
		Watches:             []sdk.WatchKind{{APIVersion: "apps/v1", Kind: "Deployment"}, {APIVersion: "v1", Kind: "Pod"}},
		Interval:            30 * time.Second,
		Persistence:         sdk.PersistencePolicy{Firing: 1, Clearing: 2},
		Batching:            sdk.BatchingPolicy{Severity: sdk.SeverityCritical},
		Playbooks:           []string{"crashloop-script-repair"},
		OriginatingIncident: "worked-example",
		OriginatingCommit:   "example-outcome-commit",
	}
}

func (Detector) Detect(_ context.Context, snapshot sdk.DetectionContext) ([]sdk.Finding, error) {
	findings := []sdk.Finding{}
	for _, deployment := range snapshot.Deployments() {
		selector, err := metav1.LabelSelectorAsSelector(deployment.Spec.Selector)
		if err != nil || deployment.Spec.Selector == nil {
			continue
		}
		for _, pod := range snapshot.Pods() {
			if pod.Namespace != deployment.Namespace || !selector.Matches(labels.Set(pod.Labels)) || !crashLooping(pod.Status.ContainerStatuses) {
				continue
			}
			ref := sdk.ObjectRefFrom("Deployment", "apps/v1", &deployment)
			findings = append(findings, sdk.Finding{
				RuleID:            "crashloop-script-repair",
				Status:            sdk.FindingActive,
				Severity:          sdk.SeverityCritical,
				Summary:           "Deployment Pods are crash looping",
				Evidence:          "pod " + pod.Name + " is in CrashLoopBackOff",
				PrimaryResource:   ref,
				Playbooks:         []string{"crashloop-script-repair"},
				ParameterBindings: map[string]sdk.ObjectRef{"deployment": ref}, // the playbook's role name
				Fingerprint:       deployment.Namespace + "/" + deployment.Name,
			})
			break
		}
	}
	return findings, nil
}

func crashLooping(statuses []corev1.ContainerStatus) bool {
	for _, status := range statuses {
		if status.State.Waiting != nil && status.State.Waiting.Reason == "CrashLoopBackOff" {
			return true
		}
	}
	return false
}
