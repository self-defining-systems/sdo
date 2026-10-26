package detector

import (
	"context"
	"time"

	"fixture-diagnostics/lib/mongorelation"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type Detector struct{}

func New() sdk.Detector { return Detector{} }
func (Detector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: "mongo-authentication", Class: sdk.DetectorClassIncident, Owner: sdk.DetectorOwnerResponder,
		Watches: []sdk.WatchKind{{APIVersion: "v1", Kind: "Pod"}}, Interval: 30 * time.Second,
		Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1}, Batching: sdk.BatchingPolicy{Severity: sdk.SeverityWarning},
		Playbooks: []string{"mongo-auth-bootstrap"}, OriginatingIncident: "mongo-auth-upgrade", OriginatingCommit: "synthetic"}
}
func (Detector) Detect(_ context.Context, v sdk.DetectionContext) ([]sdk.Finding, error) {
	var findings []sdk.Finding
	for _, pod := range v.Pods() {
		ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name}
		verdict, err := mongorelation.Diagnose(program.ReadFrom(v), ref)
		if err != nil {
			return nil, err
		}
		if verdict.Result == program.Match {
			findings = append(findings, sdk.Finding{Status: sdk.FindingActive, Severity: sdk.SeverityWarning, RuleID: "mongo-authentication", PrimaryResource: ref, Summary: "Application database account missing or roles differ", Evidence: "current related catalog differs", Playbooks: []string{"mongo-auth-bootstrap"}})
		}
	}
	return findings, nil
}
