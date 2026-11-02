package detector

import (
	"context"
	"sdo.dev/operator/sdk"
	"time"
)

type Detector struct{}

func New() sdk.Detector { return Detector{} }
func (Detector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: "listener-conflict", Class: sdk.DetectorClass("incident"), Owner: sdk.DetectorOwner("responder"), Watches: []sdk.WatchKind{{APIVersion: "apps/v1", Kind: "Deployment"}}, Interval: 30 * time.Second, Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1}, Batching: sdk.BatchingPolicy{Severity: sdk.SeverityWarning}, Playbooks: []string{"extra-workload-listener"}, OriginatingIncident: "workload-symptom-learning", OriginatingCommit: "synthetic"}
}
func (Detector) Detect(_ context.Context, v sdk.DetectionContext) ([]sdk.Finding, error) {
	var findings []sdk.Finding
	for _, d := range v.Deployments() {
		if len(d.Spec.Template.Spec.Containers) > 1 {
			ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: d.Namespace, Name: d.Name}
			findings = append(findings, sdk.Finding{Status: sdk.FindingActive, Severity: sdk.SeverityWarning, RuleID: "listener-conflict", PrimaryResource: ref, Summary: "Unexpected workload listener", Evidence: "Observed extra listener configuration", Playbooks: []string{"extra-workload-listener"}})
		}
	}
	return findings, nil
}
