// Package replay_targetport is a small hand-written detector for builder tests.
package replay_targetport

import (
	"context"
	"sdo.dev/operator/sdk"
	"time"
)

func New() sdk.Detector { return Detector{} }

type Detector struct{}

func (Detector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: "replay-targetport", Class: sdk.DetectorClassIncident, Owner: sdk.DetectorOwnerResponder, Description: "synthetic Service port mismatch", Watches: []sdk.WatchKind{{APIVersion: "v1", Kind: "Service"}, {APIVersion: "v1", Kind: "Pod"}}, Interval: 30 * time.Second, Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1}, Batching: sdk.BatchingPolicy{Severity: sdk.SeverityCritical, Debounce: 500 * time.Millisecond}, Playbooks: []string{"service-targetport-mismatch"}, OriginatingIncident: "synthetic-port", OriginatingCommit: "synthetic"}
}
func (Detector) Detect(_ context.Context, v sdk.DetectionContext) ([]sdk.Finding, error) {
	out := []sdk.Finding{}
	for _, s := range v.Services() {
		for _, pod := range v.PodsForService(s.Namespace, s.Name) {
			if len(pod.Spec.Containers) == 0 || len(pod.Spec.Containers[0].Ports) == 0 {
				continue
			}
			for _, port := range s.Spec.Ports {
				if port.TargetPort.IntVal != pod.Spec.Containers[0].Ports[0].ContainerPort {
					ref := sdk.ObjectRefFrom("Service", "v1", &s)
					out = append(out, sdk.Finding{Status: sdk.FindingActive, RuleID: "targetport", PrimaryResource: ref, Playbooks: []string{"service-targetport-mismatch"}, ParameterBindings: map[string]sdk.ObjectRef{"service": ref}})
				}
			}
		}
	}
	return out, nil
}
