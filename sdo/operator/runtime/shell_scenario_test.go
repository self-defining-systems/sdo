package runtime

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/shellpath"
)

type shellScenarioClock struct{ nanos atomic.Int64 }

func (c *shellScenarioClock) Now() time.Time   { return time.Unix(0, c.nanos.Load()).UTC() }

func (c *shellScenarioClock) Set(at time.Time) { c.nanos.Store(at.UnixNano()) }

type shellScenarioReader struct {
	service *shellpath.Service
	clock   *shellScenarioClock
	reports *shellpath.ReportStore
}

func (r shellScenarioReader) Read(ctx context.Context) (shellpath.Report, error) {
	if err := r.service.Step(ctx, r.clock.Now(), false); err != nil {
		return shellpath.Report{}, err
	}
	return r.reports.Get(), nil
}

type shellScenarioDispatcher struct {
	preparer KubernetesJobDispatcher
	fastloopDispatch
}

func (d shellScenarioDispatcher) PrepareDispatch(ctx context.Context, r IncidentRequest) (IncidentRequest, error) {
	return d.preparer.PrepareDispatch(ctx, r)
}

func (d shellScenarioDispatcher) Dispatch(ctx context.Context, r IncidentRequest) (IncidentResult, error) {
	if r.PreparedAgentTask == "" {
		var err error
		r, err = d.preparer.PrepareDispatch(ctx, r)
		if err != nil {
			return IncidentResult{}, err
		}
	}

	task := &contracts.AgentTask{}
	if err := wire.Unmarshal([]byte(r.PreparedAgentTask), task); err != nil {
		return IncidentResult{}, err
	}
	request := &contracts.IncidentRequest{}
	if err := task.Request.UnmarshalTo(request); err != nil {
		return IncidentResult{}, err
	}
	if len(request.GetMemorySnapshot().GetDetectors()) == 0 || !strings.Contains(request.GetMemorySnapshot().GetDetectors()[0].GetDescription(), "check.sh") {
		return IncidentResult{}, fmt.Errorf("shell responder did not receive accepted scripts")
	}
	return d.fastloopDispatch.Dispatch(ctx, r)
}

