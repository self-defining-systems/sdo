package runtime

import (
	"context"
	"testing"
	"time"

	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memoryhost"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/servicehealth"
	"sdo.dev/operator/sdk/traffic"
)

// A detector served by the memory host must report the same settling window as
// the compiled detector it was built from. The deterministic stage waits for a
// windowed detector to catch up with a repair only when the window is visible;
// a host proxy that hides it hands a correct automatic repair to the responder
// whenever the verification sample lands before the detector's dial history
// turns over.
func TestMemoryHostDetectorKeepsItsEvaluationWindow(t *testing.T) {
	spec := sdk.DetectorSpec{ID: "windowed", Class: sdk.DetectorClassHealth}
	view := sdktest.Snapshot{Reachability: &servicehealth.ReachabilityWindow{Interval: time.Second, Timeout: 2 * time.Second}}
	cases := map[string]sdk.Detector{
		"service reachability": servicehealth.NewReachabilityDetector(spec),
		"traffic SLO":          traffic.NewDetector(spec, "frontend"),
		"plain":                specOnly{spec},
	}
	for name, local := range cases {
		t.Run(name, func(t *testing.T) {
			server, err := memoryhost.NewServer("synthetic-commit", "synthetic-digest", []sdk.Detector{local})
			if err != nil {
				t.Fatal(err)
			}
			ready, err := server.Ready(context.Background(), &pb.ReadyRequest{})
			if err != nil {
				t.Fatal(err)
			}
			remote, err := NewRemoteMemory(ready, unusedMemoryRPC{})
			if err != nil {
				t.Fatal(err)
			}
			_, localWindowed := local.(sdk.WindowedDetector)
			_, remoteWindowed := remote.Detectors[0].(sdk.WindowedDetector)
			if localWindowed != remoteWindowed {
				t.Fatalf("windowed: compiled %v, memory host %v", localWindowed, remoteWindowed)
			}
			if got, want := detectorWindow(remote.Detectors[0], view), detectorWindow(local, view); got != want {
				t.Fatalf("window: memory host %v, compiled %v", got, want)
			}
			if got, want := detectorClearingHorizon(remote.Detectors[0], view), detectorClearingHorizon(local, view); got != want {
				t.Fatalf("clearing horizon: memory host %v, compiled %v", got, want)
			}
		})
	}
}
