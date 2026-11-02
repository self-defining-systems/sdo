package detector

import (
	"context"
	"strings"
	"time"

	"sdo.dev/operator/sdk"
)

type Detector struct{}

func New() sdk.Detector { return Detector{} }
func (Detector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{
		ID: "mongo-authentication", Class: sdk.DetectorClass("incident"), Owner: sdk.DetectorOwner("responder"),
		Watches: []sdk.WatchKind{{APIVersion: "v1", Kind: "Pod"}}, Interval: 30 * time.Second,
		Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1},
		Batching:    sdk.BatchingPolicy{Severity: sdk.FindingSeverity("warn")},
		Playbooks:   []string{"mongo-auth-bootstrap"}, OriginatingIncident: "mongo-auth-upgrade", OriginatingCommit: "synthetic",
	}
}
func (Detector) Detect(_ context.Context, v sdk.DetectionContext) ([]sdk.Finding, error) {
	var findings []sdk.Finding
	for _, pod := range v.Pods() {
		for _, status := range pod.Status.ContainerStatuses {
			waiting := status.State.Waiting
			if waiting == nil || !strings.Contains(waiting.Message, "MongoDB AuthenticationFailed") {
				continue
			}
			ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name}
			findings = append(findings, sdk.Finding{Status: sdk.FindingActive, Severity: sdk.SeverityWarning, RuleID: "mongo-authentication", PrimaryResource: ref, Summary: "Application container reports MongoDB authentication failure", Evidence: waiting.Message, Playbooks: []string{"mongo-auth-bootstrap"}})
		}
	}
	return findings, nil
}
