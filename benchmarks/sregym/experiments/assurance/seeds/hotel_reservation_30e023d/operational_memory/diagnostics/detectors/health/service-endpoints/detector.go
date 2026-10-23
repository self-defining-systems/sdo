package serviceendpoints

import (
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/servicehealth"
	"time"
)

func New() sdk.Detector {
	return servicehealth.NewReadyEndpointsDetector(sdk.DetectorSpec{
		ID:                "service-endpoints",
		Class:             sdk.DetectorClassHealth,
		Owner:             sdk.DetectorOwnerHealthJudge,
		Persistence:       sdk.PersistencePolicy{Firing: 2, Clearing: 2},
		Batching:          sdk.BatchingPolicy{Severity: sdk.SeverityCritical, Debounce: 500 * time.Millisecond},
		Playbooks:         []string{"health-objective"},
		OriginatingCommit: "lifecycle-bootstrap",
		Watches:           servicehealth.EndpointWatches(),
		Interval:          15 * time.Second,
	})
}
