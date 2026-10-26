package driftbound

import (
	"context"
	"sdo.dev/operator/sdk"
	"time"
)

type Detector struct{}

func New() sdk.Detector { return Detector{} }
func (Detector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: "source-absent-binding", Class: sdk.DetectorClassIncident, Owner: sdk.DetectorOwnerResponder, Watches: []sdk.WatchKind{{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRoleBinding"}}, Interval: time.Second, Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1}, Batching: sdk.BatchingPolicy{Severity: sdk.SeverityWarning, Debounce: 100 * time.Millisecond}, Playbooks: []string{"source-absent-binding"}, OriginatingIncident: "binding-origin", OriginatingCommit: "fixture"}
}
func (Detector) Detect(_ context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	// A named authority is intentional fixture configuration, not an app recipe.
	ref := sdk.ObjectRef{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRoleBinding", Name: "toy-binding"}
	diff, err := view.DiffAgainstSource(ref)
	if err != nil {
		return nil, err
	}
	if !diff.Changed || !diff.SourceAbsent {
		return nil, nil
	}
	return []sdk.Finding{{DetectorID: "source-absent-binding", RuleID: "source-absent-binding", Status: sdk.FindingActive, Severity: sdk.SeverityWarning, Summary: "Unexpected authority absent from accepted source", Evidence: diff.Text, PrimaryResource: ref, ParameterBindings: map[string]sdk.ObjectRef{"binding": ref}, Playbooks: []string{"source-absent-binding"}}}, nil
}
