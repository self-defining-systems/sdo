package synthetic

import (
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/traffic"
	"time"
)

func New() sdk.Detector {
	return traffic.NewDetector(sdk.DetectorSpec{
		ID:                "traffic-health",
		Class:             sdk.DetectorClassHealth,
		Owner:             sdk.DetectorOwnerHealthJudge,
		Persistence:       sdk.PersistencePolicy{Firing: 2, Clearing: 2},
		Batching:          sdk.BatchingPolicy{Severity: sdk.SeverityCritical, Debounce: 500 * time.Millisecond},
		Playbooks:         []string{"health-objective"},
		OriginatingCommit: "lifecycle-bootstrap",
		Watches:           []sdk.WatchKind{traffic.Watch},
		Interval:          10 * time.Second,
	}, "health")
}
