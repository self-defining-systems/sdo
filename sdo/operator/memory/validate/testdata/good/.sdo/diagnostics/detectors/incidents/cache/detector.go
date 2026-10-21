package detector

import (
	"context"
	"sdo.dev/operator/sdk"
	"time"
)

type Detector struct{}

func New() sdk.Detector { return Detector{} }
func (Detector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{
		ID: "cache", Class: sdk.DetectorClass("incident"), Owner: sdk.DetectorOwner("responder"),
		Watches: []sdk.WatchKind{{APIVersion: "v1", Kind: "Pod"}}, Interval: 30 * time.Second,
		Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1},
		Batching:    sdk.BatchingPolicy{Severity: sdk.FindingSeverity("warn"), Debounce: 0},
		Playbooks:   []string{"base"}, OriginatingIncident: "inc-1", OriginatingCommit: "abc",
	}
}
func (Detector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) { return nil, nil }
