package servicehealth_test

import (
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/util/intstr"
	"sdo.dev/operator/sdk/servicehealth"
)

func TestReachabilityFaultLatencyUnchangedByHealthyReference(t *testing.T) {
	for _, referenced := range []bool{false, true} {
		s := reachabilitySnapshot{Snapshot: geoSnapshot(intstr.FromInt32(8083), true), window: window()}
		served := observedAt.Add(-time.Minute)
		for count := 1; count <= 3; count++ {
			s.window.Targets = []servicehealth.TargetObservations{geoTarget(samples(servicehealth.DialRefused, make([]bool, count)...))}
			s.window.Targets[0].LastSuccess = &served
			if referenced {
				auth := servicehealth.BaselineAuthorization{SourceInputs: strings.Repeat("a", 64), ObjectiveDigest: strings.Repeat("b", 64), Reason: "healthy-baseline", GoalHealthy: true}
				s.window.Baseline = &servicehealth.ReachabilityBaseline{Version: 1, Authorization: auth, CapturedAt: served}
				s.window.SourceInputs = auth.SourceInputs
				s.window.ObjectiveDigest = auth.ObjectiveDigest
			}
			got := detectReachability(t, s)
			if (len(got) == 1) != (count == 3) {
				t.Fatalf("reference=%v samples=%d findings=%+v", referenced, count, got)
			}
			if count == 3 {
				t.Logf("reference=%v detection after %s / %d failed samples", referenced, s.window.Targets[0].Samples[2].At.Sub(s.window.Targets[0].Samples[0].At), count)
			}
		}
	}
}
