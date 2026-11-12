package runtime

import (
	"context"
	"fmt"
	goruntime "runtime"
	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"sync"
	"testing"
	"time"
)

type sequenceDetector struct {
	spec    sdk.DetectorSpec
	samples [][]sdk.Finding
	errors  map[int]error
	calls   int
}

func (d *sequenceDetector) Spec() sdk.DetectorSpec { return d.spec }

func (d *sequenceDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	index := d.calls
	d.calls++
	if err := d.errors[index]; err != nil {
		return nil, err
	}
	if index >= len(d.samples) {
		return d.samples[len(d.samples)-1], nil
	}
	return d.samples[index], nil
}

type staticProvider struct{ snapshot sdk.DetectionContext }

func (p staticProvider) Snapshot(context.Context) (sdk.DetectionContext, error) {
	if p.snapshot == nil {
		return sdktest.Snapshot{}, nil
	}
	return p.snapshot, nil
}

type recordingDispatcher struct {
	requests chan IncidentRequest
	mu       sync.Mutex
	calls    int
}

func (d *recordingDispatcher) Dispatch(_ context.Context, request IncidentRequest) (IncidentResult, error) {
	d.mu.Lock()
	d.calls++
	d.mu.Unlock()
	d.requests <- request
	return completedResult(request.IncidentID), nil
}

func (d *recordingDispatcher) callCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.calls
}

func controllerDetector(id string, interval time.Duration, samples ...sdk.Finding) *sequenceDetector {
	sequences := make([][]sdk.Finding, 0, len(samples))
	for _, finding := range samples {
		if finding.RuleID == "" {
			sequences = append(sequences, nil)
			continue
		}
		finding.DetectorID = id
		sequences = append(sequences, []sdk.Finding{finding})
	}
	return &sequenceDetector{
		spec: sdk.DetectorSpec{ID: id, Interval: interval}, samples: sequences, errors: make(map[int]error),
	}
}

// testControllerConfig takes its timing and policy from the production
// defaults, as every NewController caller does, with a one-minute response.
func testControllerConfig() ControllerConfig {
	cfg := ControllerConfig{
		Application: "demo", Namespace: "demo", SourceCommit: "source", DeployedCommit: "deployed",
		RepositoryWorktree: "/tmp/demo",
		FiringThreshold:    2, ClearThreshold: 2,
	}.WithSettings(settings.Default())
	cfg.ResponseTimeout = time.Minute
	return cfg
}

func completedResult(incidentID string) IncidentResult {
	now := time.Unix(100, 0).UTC()
	return IncidentResult{
		SchemaVersion: ProtocolSchemaVersion, IncidentID: incidentID, Status: IncidentCompleted,
		Usage: UsageMetrics{}, Timing: TimingMetrics{StartedAt: now, CompletedAt: now},
	}
}

func awaitRequest(t *testing.T, requests <-chan IncidentRequest) IncidentRequest {
	t.Helper()
	return <-requests
}

func awaitDispatchCompletionQueued(t *testing.T, controller *Controller) {
	t.Helper()
	// Observe and restore the queued message: downstream controller steps still
	// own consumption, and the channel receive synchronizes with the worker.
	result := <-controller.results
	controller.results <- result
}

// awaitEffectGoroutines joins the goroutines an executed effect started. The
// closure handoff calls the broker on one, so a stepping loop that does not
// wait can finish all its steps before the broker is ever called. Results stay
// queued in their channels for the next step to consume.
func awaitEffectGoroutines(controller *Controller) { controller.Wait() }

// runOnOneProcessor makes a stepping loop's scheduling worst-case: a goroutine
// the loop does not wait for runs only when the loop blocks. A loop that races
// a controller goroutine fails here every time instead of once under CI load.
func runOnOneProcessor(t *testing.T) {
	t.Helper()
	previous := goruntime.GOMAXPROCS(1)
	t.Cleanup(func() { goruntime.GOMAXPROCS(previous) })
}

func executePendingEffect(t *testing.T, controller *Controller) {
	t.Helper()
	effect, ok := controller.PendingDispatchEffect()
	if !ok {
		t.Fatal("expected pending dispatch effect")
	}
	if err := controller.ExecuteDispatchEffect(context.Background(), effect); err != nil {
		t.Fatalf("execute dispatch effect: %v", err)
	}
}

func (d *sequenceDetector) String() string { return fmt.Sprintf("detector(%s)", d.spec.ID) }
