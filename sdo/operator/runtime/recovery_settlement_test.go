package runtime

import (
	"time"
)

type settlementControllerDispatcher struct {
	pipelineScenarioDispatcher
	controller *Controller
}

func (d *settlementControllerDispatcher) DeterministicResponseStage() ResponseStage {
	return &DeterministicResponseStage{ObservationCeiling: time.Millisecond, NewLoop: func(request IncidentRequest) *ResponderLoop {
		d.loop.Request = request
		d.controller.mu.Lock()
		d.controller.responderLoop = d.loop
		d.controller.mu.Unlock()
		return d.loop
	}}
}
