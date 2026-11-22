package bench

import (
	"testing"
	"time"
)

// A release's idle wait may last the whole experiment cap, and the cluster delete comes after it: the step bound must
// outlast both, or the manager kills a release that was only waiting as long as it was allowed (an EKS lane,
// 2026-11-22: a release whose idle wait was the 2 h cap met the 2 h step bound, died, and the lane's cluster billed
// two hours longer).
func TestReleaseBoundOutlastsTheIdleWaitAndTheClusterDelete(t *testing.T) {
	for _, cap := range []time.Duration{0, 10 * time.Minute, 2 * time.Hour, 6 * time.Hour} {
		s := Spec{Cap: cap.String()}
		if cap == 0 {
			s = Spec{}
		}
		const clusterDown = 45 * time.Minute // the step runner's own limit on cluster-down
		if got, need := stepBound(StepRelease, s), cap+clusterDown; got <= need {
			t.Errorf("cap %s: release bound %s does not outlast the idle wait plus the cluster delete (%s)", cap, got, need)
		}
	}
}
