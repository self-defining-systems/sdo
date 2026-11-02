package runtime

import (
	"context"
	"fmt"
	"time"

	"sdo.dev/operator/sdk"
)

type repeatHealthDetector struct{ missingConfigMapDetector }

func (repeatHealthDetector) Spec() sdk.DetectorSpec {
	spec := (missingConfigMapDetector{}).Spec()
	spec.Class, spec.Owner = sdk.DetectorClassHealth, sdk.DetectorOwnerHealthJudge
	spec.OriginatingCommit = "synthetic-health"
	spec.Interval = 20 * time.Millisecond
	spec.Persistence = sdk.PersistencePolicy{Firing: 1, Clearing: 1}
	spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityCritical, Debounce: time.Millisecond}
	return spec
}

// An independent detector may fail to observe a dependency. That error must
// remain visible while the scheduler and unrelated detectors keep progressing.
type repeatUnavailableDetector struct{ accepted string }

func (d repeatUnavailableDetector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: "learned-unavailable", Class: sdk.DetectorClassIncident, Owner: sdk.DetectorOwnerResponder, Watches: []sdk.WatchKind{{APIVersion: "v1", Kind: "Pod"}}, Interval: 20 * time.Millisecond, Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1}, Batching: sdk.BatchingPolicy{Severity: sdk.SeverityCritical}, OriginatingIncident: "synthetic-first-encounter", OriginatingCommit: d.accepted}
}

func (repeatUnavailableDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	return nil, fmt.Errorf("synthetic deployment evidence unavailable")
}

