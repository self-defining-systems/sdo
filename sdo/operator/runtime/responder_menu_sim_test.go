package runtime

import (
	"context"
	"sync"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk/program"
)

type menuSimTask struct {
	request  IncidentRequest
	result   IncidentResult
	done     chan struct{}
	sessions map[string]bool
	reports  []*contracts.MitigationReport
}

// Rejoining a controller dispatch reconnects to the existing scripted worker,
// exactly as a surviving responder Job does in the restart simulation harness.
type menuSimDispatcher struct {
	mu      sync.Mutex
	tasks   map[string]*menuSimTask
	started chan *menuSimTask
}

func (d *menuSimDispatcher) Dispatch(ctx context.Context, r IncidentRequest) (IncidentResult, error) {
	d.mu.Lock()
	task := d.tasks[r.IncidentID]
	if task == nil {
		task = &menuSimTask{request: r, done: make(chan struct{}), sessions: map[string]bool{}}
		d.tasks[r.IncidentID] = task
		d.started <- task
	}
	d.mu.Unlock()
	select {
	case <-task.done:
		return task.result, nil
	case <-ctx.Done():
		return IncidentResult{}, ctx.Err()
	}
}

type menuSimProgram struct {
	id   string
	port int
}

func (p menuSimProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: p.id, Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}, {Name: "port", ValueKind: program.IntValue, Default: p.port}}}
}

func (p menuSimProgram) Diagnose(_ context.Context, v program.ReadView, b program.Bindings) (program.Verdict, error) {
	for _, service := range v.Services() {
		if service.Name == b.Objects["service"].Name && service.Spec.Ports[0].TargetPort.IntVal != 8080 {
			return program.Verdict{Result: program.Match}, nil
		}
	}
	return program.Verdict{Result: program.NoMatch}, nil
}

func (p menuSimProgram) Mitigate(_ context.Context, _ program.ReadView, b program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: []program.Action{program.SetField{Ref: b.Objects["service"], Path: "spec.ports[0].targetPort", Value: program.FromParam("port")}}}, nil
}
