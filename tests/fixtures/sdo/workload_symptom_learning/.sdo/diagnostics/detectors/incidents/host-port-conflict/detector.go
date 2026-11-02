package detector

import (
	"context"
	corev1 "k8s.io/api/core/v1"
	"sdo.dev/operator/sdk"
	"time"
)

type Detector struct{}

func New() sdk.Detector { return Detector{} }
func (Detector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: "host-port-conflict", Class: sdk.DetectorClass("incident"), Owner: sdk.DetectorOwner("responder"), Watches: []sdk.WatchKind{{APIVersion: "apps/v1", Kind: "Deployment"}}, Interval: 30 * time.Second, Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1}, Batching: sdk.BatchingPolicy{Severity: sdk.SeverityWarning}, Playbooks: []string{"workload-host-port"}, OriginatingIncident: "workload-symptom-learning", OriginatingCommit: "synthetic"}
}
func (Detector) Detect(_ context.Context, v sdk.DetectionContext) ([]sdk.Finding, error) {
	var findings []sdk.Finding
	for _, d := range v.Deployments() {
		if hasHostPort(d.Spec.Template.Spec.Containers) {
			ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: d.Namespace, Name: d.Name}
			findings = append(findings, sdk.Finding{Status: sdk.FindingActive, Severity: sdk.SeverityWarning, RuleID: "host-port-conflict", PrimaryResource: ref, Summary: "Unexpected workload listener", Evidence: "Observed extra listener configuration", Playbooks: []string{"workload-host-port"}})
		}
	}
	return findings, nil
}

func hasHostPort(containers []corev1.Container) bool {
	for _, c := range containers {
		for _, p := range c.Ports {
			if p.HostPort != 0 {
				return true
			}
		}
	}
	return false
}
