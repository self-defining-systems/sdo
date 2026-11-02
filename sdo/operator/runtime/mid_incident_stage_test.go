package runtime

import (
	"context"
	"sync/atomic"

	"sdo.dev/operator/sdk"
)

// delayedPortDetector reports the wrong-port fault only once released, like a
// learned detector that activates after the responder was dispatched.
type delayedPortDetector struct {
	pipelineDetector
	released *atomic.Bool
}

func (d delayedPortDetector) Detect(ctx context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	findings, err := d.pipelineDetector.Detect(ctx, view)
	if d.released.Load() {
		return findings, err
	}
	kept := findings[:0]
	for _, finding := range findings {
		if finding.RuleID != "wrong-port" {
			kept = append(kept, finding)
		}
	}
	return kept, err
}

// blockingAgent holds the agent stage open, as a responder at work does.
type blockingAgent struct {
	pipelineScenarioDispatcher
	requests chan IncidentRequest
	release  chan struct{}
}

func (d *blockingAgent) Dispatch(_ context.Context, request IncidentRequest) (IncidentResult, error) {
	d.requests <- request
	<-d.release
	return completedResult(request.IncidentID), nil
}

// stagedPortDetector reports the wrong-port findings of the first n Services
// in order, like learned detectors activating one by one mid-incident.
type stagedPortDetector struct {
	pipelineDetector
	order    []string
	released *atomic.Int32
}

func (d stagedPortDetector) Detect(ctx context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	findings, err := d.pipelineDetector.Detect(ctx, view)
	shown := map[string]bool{}
	for _, name := range d.order[:d.released.Load()] {
		shown[name] = true
	}
	kept := findings[:0]
	for _, finding := range findings {
		if finding.RuleID != "wrong-port" || shown[finding.PrimaryResource.Name] {
			kept = append(kept, finding)
		}
	}
	return kept, err
}

