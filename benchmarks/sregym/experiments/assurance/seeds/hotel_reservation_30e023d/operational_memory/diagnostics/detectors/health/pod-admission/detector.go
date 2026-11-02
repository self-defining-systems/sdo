package podadmission

import (
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/workloadhealth"
	"time"
)

func New() sdk.Detector {
	return workloadhealth.NewPodAdmissionDetector(sdk.DetectorSpec{
		ID:                "pod-admission",
		Class:             sdk.DetectorClassHealth,
		Owner:             sdk.DetectorOwnerHealthJudge,
		Persistence:       sdk.PersistencePolicy{Firing: 2, Clearing: 2},
		Batching:          sdk.BatchingPolicy{Severity: sdk.SeverityCritical, Debounce: 500 * time.Millisecond},
		Playbooks:         []string{"health-objective"},
		OriginatingCommit: "lifecycle-bootstrap",
		Watches:           []sdk.WatchKind{{APIVersion: "v1", Kind: "Pod"}},
		Interval:          15 * time.Second,
	})
}
