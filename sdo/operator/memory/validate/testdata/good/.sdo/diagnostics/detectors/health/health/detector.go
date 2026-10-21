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
		ID: "health", Class: sdk.DetectorClass("health"), Owner: sdk.DetectorOwner("health_judge"),
		Watches: []sdk.WatchKind{{APIVersion: "v1", Kind: "Pod"}}, Interval: 30 * time.Second,
		Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1},
		Batching:    sdk.BatchingPolicy{Severity: sdk.FindingSeverity("critical"), Debounce: 500 * time.Millisecond},
		Playbooks:   []string{"base"}, OriginatingIncident: "", OriginatingCommit: "initial",
	}
}
func (Detector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) { return nil, nil }
