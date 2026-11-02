package runtime

import (
	"context"
	"time"
)

type pipelineScenarioDispatcher struct {
	loop               *ResponderLoop
	agentCalls         int
	observationCeiling time.Duration
}

func (d *pipelineScenarioDispatcher) DeterministicResponseStage() ResponseStage {
	return &DeterministicResponseStage{NewLoop: func(request IncidentRequest) *ResponderLoop { d.loop.Request = request; return d.loop }, ObservationCeiling: d.observationCeiling}
}

func (d *pipelineScenarioDispatcher) Dispatch(_ context.Context, request IncidentRequest) (IncidentResult, error) {
	d.agentCalls++
	return completedResult(request.IncidentID), nil
}

// recordingScenarioDispatcher hands each agent request to the test over a
// channel, because Dispatch runs on the controller's dispatch goroutine.
type recordingScenarioDispatcher struct {
	pipelineScenarioDispatcher
	requests chan IncidentRequest
}

func (d *recordingScenarioDispatcher) Dispatch(ctx context.Context, request IncidentRequest) (IncidentResult, error) {
	d.requests <- request
	return completedResult(request.IncidentID), nil
}
