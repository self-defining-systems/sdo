package endpoints

import (
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/servicehealth"
	"time"
)

func New() sdk.Detector {
	return servicehealth.NewReadyEndpointsDetector(sdk.DetectorSpec{
		ID: "toy-endpoints", Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge,
		Interval: 2 * time.Second, Watches: servicehealth.EndpointWatches(),
		Persistence:       sdk.PersistencePolicy{Firing: 1, Clearing: 1},
		Batching:          sdk.BatchingPolicy{Severity: sdk.SeverityCritical, Debounce: 100 * time.Millisecond},
		Playbooks:         []string{"health-objective"},
		OriginatingCommit: "toy-bootstrap",
	})
}
