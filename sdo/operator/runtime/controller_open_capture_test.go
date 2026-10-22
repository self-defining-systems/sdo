package runtime

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
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

// awaitRoutingSweep waits for the background open@detect routing sweep of an
// incident to finish (its marker is the last file it writes).
func awaitRoutingSweep(t *testing.T, root, incidentID string) {
	t.Helper()
	dir, err := memory.IncidentCorpusDir(root, incidentID)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(filepath.Join(dir, "open@detect-routing.done")); err == nil {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("routing sweep never completed")
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
	t.Cleanup(controller.routingSweeps.Wait)
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

func TestOpenDetectRoutingSweepNeverDelaysDispatch(t *testing.T) {
	root := t.TempDir()
	prober := newGatedProber()
	t.Cleanup(prober.Release)
	controller, dispatcher := openCaptureController(t, root, func(s sdk.DetectionContext) program.ReadView {
		return LiveReadView{DetectionContext: s, Prober: prober}
	})
	openIncident(t, controller)
	// The responder is dispatched while the sweep is still blocked on its first probe.
	request := awaitRequest(t, dispatcher.requests)
	dir, err := memory.IncidentCorpusDir(root, request.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "open@detect.json")); err != nil {
		t.Fatalf("open@detect snapshot must exist when the responder starts: %v", err)
	}
	marker := filepath.Join(dir, "open@detect-routing.done")
	if _, err := os.Stat(marker); err == nil {
		t.Fatal("routing sweep finished although every probe was blocked")
	}
	prober.Release()
	awaitRoutingSweep(t, root, request.IncidentID)
}

func TestOpenDetectCaptureFailureNeverBlocksIncident(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(blocker, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	controller, dispatcher := openCaptureController(t, filepath.Join(blocker, "corpus"), func(s sdk.DetectionContext) program.ReadView {
		return LiveReadView{DetectionContext: s, Prober: &routingTestProber{}}
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
