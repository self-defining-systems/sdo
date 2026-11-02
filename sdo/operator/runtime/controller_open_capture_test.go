package runtime

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/program"
)

// gatedProber blocks every probe until released, so a test can prove that the
// routing sweep is off the incident-open path.
type gatedProber struct {
	release chan struct{}
	once    sync.Once
}

func newGatedProber() *gatedProber { return &gatedProber{release: make(chan struct{})} }

func (p *gatedProber) Release() { p.once.Do(func() { close(p.release) }) }

func (p *gatedProber) Probe(ctx context.Context, _ sdk.ObjectRef, _ int32, _ *program.HTTPProbe) (program.ProbeResult, error) {
	select {
	case <-p.release:
		return program.ProbeResult{Success: false, Detail: "refused"}, nil
	case <-ctx.Done():
		return program.ProbeResult{}, ctx.Err()
	}
}

func (p *gatedProber) ProbePod(ctx context.Context, ref sdk.ObjectRef, _ string, port int32) (program.ProbeResult, error) {
	return p.Probe(ctx, ref, port, nil)
}

func openCaptureController(t *testing.T, root string, view func(sdk.DetectionContext) program.ReadView) (*Controller, *recordingDispatcher) {
	t.Helper()
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	finding := sdk.Finding{DetectorID: "health", RuleID: "routing", Status: sdk.FindingActive, Severity: sdk.SeverityWarning, PrimaryResource: ref, Summary: "routing unavailable", Evidence: "no Service connection"}
	detector := controllerDetector("health", time.Second, finding, finding, sdk.Finding{}, sdk.Finding{})
	detector.spec.Class, detector.spec.Owner = sdk.DetectorClassHealth, sdk.DetectorOwnerHealthJudge
	detector.spec.Persistence = sdk.PersistencePolicy{Firing: 2, Clearing: 2}
	detector.spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityWarning}
	detector.spec.OriginatingCommit = "health-objective"
	config := testControllerConfig()
	config.Namespace = "app"
	config.CorpusSessionRoot = root
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	controller, err := NewController(config, []sdk.Detector{detector}, &changingProvider{snapshot: routingCaptureSnapshot()}, dispatcher, time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	controller.ConfigureDrift(DriftControllerConfig{View: view})
	t.Cleanup(controller.Wait)
	return controller, dispatcher
}

func openIncident(t *testing.T, controller *Controller) {
	t.Helper()
	for i := 0; i < 2; i++ {
		if err := controller.Step(context.Background(), time.Unix(int64(i), 0), nil); err != nil {
			t.Fatal(err)
		}
	}
	executePendingEffect(t, controller)
}

func TestOpenDetectCaptureFailureNeverBlocksIncident(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(blocker, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	controller, dispatcher := openCaptureController(t, filepath.Join(blocker, "corpus"), func(s sdk.DetectionContext) program.ReadView {
		return LiveReadView{Carrier: inputs.New(s), Prober: &routingTestProber{}}
	})
	var mu sync.Mutex
	var reported []error
	controller.OnError = func(err error) {
		mu.Lock()
		defer mu.Unlock()
		reported = append(reported, err)
	}
	openIncident(t, controller)
	awaitRequest(t, dispatcher.requests)
	mu.Lock()
	defer mu.Unlock()
	if len(reported) == 0 {
		t.Fatal("capture failure was not reported")
	}
}

// Between draining the batcher and opening the incident the controller does file I/O for the corpus.
// Nothing else then marks the work, so the incident must already be open: an observer exporting state
// in that window must read busy, not idle.
func TestIncidentIsOpenBeforeTheCorpusCaptureIO(t *testing.T) {
	var controller *Controller
	var busyDuringCapture *timestamppb.Timestamp
	var openDuringCapture bool
	controller, _ = openCaptureController(t, t.TempDir(), func(s sdk.DetectionContext) program.ReadView {
		state := exportState(t, controller)
		busyDuringCapture, openDuringCapture = state.GetBusyUntil(), state.GetIncidentOpen()
		return LiveReadView{Carrier: inputs.New(s), Prober: &routingTestProber{}}
	})
	openIncident(t, controller)
	if !openDuringCapture || busyDuringCapture == nil {
		t.Fatalf("corpus capture ran with the batcher drained and no incident open (open=%v busy_until=%v)", openDuringCapture, busyDuringCapture)
	}
}

