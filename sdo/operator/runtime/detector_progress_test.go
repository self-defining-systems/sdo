package runtime

import (
	"context"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

// The controller counts each detector's judgements and how many came at least
// its own horizon after its first, so a reader can end an observation window by
// what the detectors judged rather than by a fixed length.
func TestControllerReportsDetectorProgressAgainstItsHorizon(t *testing.T) {
	detector := controllerDetector("steady", 30*time.Second, sdk.Finding{})
	controller, err := NewController(testControllerConfig(), []sdk.Detector{detector}, staticProvider{snapshot: sdktest.Snapshot{}}, &fixedResultDispatcher{}, time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		if err := controller.Step(context.Background(), time.Unix(0, 0).Add(time.Duration(i)*30*time.Second), nil); err != nil {
			t.Fatal(err)
		}
	}
	progress := controller.DetectorProgress()
	if len(progress) != 1 || progress[0].DetectorID != "steady" {
		t.Fatalf("progress %+v", progress)
	}
	p := progress[0]
	if p.Evaluations != 6 || p.Interval != 30*time.Second || p.Horizon < 30*time.Second {
		t.Fatalf("progress %+v", p)
	}
	if p.PastHorizon == 0 || p.PastHorizon >= p.Evaluations {
		t.Fatalf("only judgements a horizon after the first count: %+v", p)
	}
}
