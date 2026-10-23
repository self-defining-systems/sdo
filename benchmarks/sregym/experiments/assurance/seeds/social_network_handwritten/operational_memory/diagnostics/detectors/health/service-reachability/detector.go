package servicereachability

import (
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/servicehealth"
	"time"
)

func New() sdk.Detector {
	return servicehealth.NewReachabilityDetector(sdk.DetectorSpec{
		ID:                "service-reachability",
		Class:             sdk.DetectorClassHealth,
		Owner:             sdk.DetectorOwnerHealthJudge,
		Persistence:       sdk.PersistencePolicy{Firing: 2, Clearing: 2},
		Batching:          sdk.BatchingPolicy{Severity: sdk.SeverityCritical, Debounce: 500 * time.Millisecond},
		Playbooks:         []string{"health-objective"},
		OriginatingCommit: "lifecycle-bootstrap",
		Watches:           servicehealth.ReachabilityWatches(),
		Interval:          10 * time.Second,
	})
}
