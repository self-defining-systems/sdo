package runtime

import (
	"context"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// closureReplayProbeTimeout bounds each live probe replayed while an incident closes. The controller
// evaluates no detector meanwhile, and a repaired Service no longer exposes the port that the open
// probe failed on, so that replay can only wait out its whole budget.
const closureReplayProbeTimeout = time.Second

// boundedProbeView limits the live probes of a closure replay; every other read is unchanged.
type boundedProbeView struct{ program.ReadView }

func (v boundedProbeView) ProbeTCP(ref sdk.ObjectRef, port int32) (program.ProbeResult, error) {
	live, ok := routingLiveView(v.ReadView)
	if !ok {
		return v.ReadView.ProbeTCP(ref, port)
	}
	ctx, cancel := context.WithTimeout(live.context(), closureReplayProbeTimeout)
	defer cancel()
	live.Context = ctx
	return live.ProbeTCP(ref, port)
}

func (v boundedProbeView) ProbeHTTP(ref sdk.ObjectRef, request program.HTTPProbe) (program.ProbeResult, error) {
	live, ok := routingLiveView(v.ReadView)
	if !ok {
		return v.ReadView.ProbeHTTP(ref, request)
	}
	ctx, cancel := context.WithTimeout(live.context(), closureReplayProbeTimeout)
	defer cancel()
	live.Context = ctx
	return live.ProbeHTTP(ref, request)
}
