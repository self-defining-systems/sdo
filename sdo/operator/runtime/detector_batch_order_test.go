package runtime

import (
	"context"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
)

// parkedSlowDetector stands in for a 30 s store-state or link detector: it
// answers only when released.
type parkedSlowDetector struct {
	entered chan struct{}
	release chan struct{}
}

func (parkedSlowDetector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: "store-state", Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge,
		Interval: 30 * time.Second, Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1},
		Batching: sdk.BatchingPolicy{Severity: sdk.SeverityWarning}, OriginatingCommit: "seed"}
}

func (d parkedSlowDetector) Detect(ctx context.Context, _ sdk.DetectionContext) ([]sdk.Finding, error) {
	close(d.entered)
	select {
	case <-d.release:
	case <-ctx.Done():
	}
	return []sdk.Finding{}, nil
}

// A fast detector's finding is applied the moment that detector returns: an
// incident opens while a slow detector of the same step is still running.
func TestFastDetectorFindingOpensIncidentWhileSlowDetectorIsRunning(t *testing.T) {
	opened := dataRiskOpened
	world := &sameWriteWorld{}
	cfg := testControllerConfig()
	cfg.Namespace, cfg.SourceCommit, cfg.RepositoryWorktree = "app", "synthetic-accepted", "synthetic-worktree"
	slow := parkedSlowDetector{entered: make(chan struct{}), release: make(chan struct{})}
	controller, err := NewController(cfg, []sdk.Detector{sameWriteDetector{world: world}, slow}, newRestartedStore(opened), &blockingDispatcher{stopAfter: opened}, opened)
	if err != nil {
		t.Fatal(err)
	}
	controller.now = func() time.Time { return opened }
	controller.gitRead = fakeArchitecture("")
	released := false
	release := func() {
		if !released {
			released = true
			close(slow.release)
		}
	}
	defer release()
	stepped := make(chan error, 1)
	go func() { stepped <- controller.StepEvents(context.Background(), opened, nil) }()
	timeout, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	select {
	case <-slow.entered:
	case <-timeout.Done():
		t.Fatal("slow detector never started")
	}
	for !controller.IncidentOpen() {
		select {
		case err := <-stepped:
			t.Fatalf("step ended without opening the incident: %v", err)
		case <-timeout.Done():
			t.Fatal("incident did not open while the slow detector was still running")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	release()
	if err := <-stepped; err != nil {
		t.Fatal(err)
	}
}
