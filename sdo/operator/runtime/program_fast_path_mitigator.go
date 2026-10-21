package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// Non-cooperative authored code cannot be killed by Go. Quarantine at most four
// executions for the lifetime of the process, including calls that timed out.
// Timed-out calls retain their slot until they actually return. No mutable loop
// state is passed to them and subsequent work fails closed when slots are spent.
var mitigationSlots = make(chan struct{}, 4)

// runMitigator is called with mu held; authored code always runs without it.
// operationMu serializes tool/diagnosis operations while readers can still
// inspect the loop during a slow or non-cooperative authored call.
func (l *ResponderLoop) runMitigator(ctx context.Context, id string, p program.Mitigator, view program.ReadView, bindings program.Bindings) (plan program.Plan, err error) {
	deadline := l.deadline()
	bindings = cloneProgramBindings(bindings)
	l.mu.Unlock()
	defer func() {
		l.mu.Lock()
		if err != nil {
			l.addFailureSignal("mitigator_failure:" + id)
		}
	}()
	return runBoundedMitigator(ctx, p, view, bindings, deadline)
}
func runBoundedMitigator(ctx context.Context, p program.Mitigator, view program.ReadView, bindings program.Bindings, deadline time.Duration) (program.Plan, error) {
	ctx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()
	select {
	case mitigationSlots <- struct{}{}:
	default:
		return program.Plan{}, fmt.Errorf("mitigator execution capacity exhausted")
	}
	type outcome struct {
		plan program.Plan
		err  error
	}
	results := make(chan outcome, 1)
	go func() {
		defer func() { <-mitigationSlots }()
		defer func() {
			if r := recover(); r != nil {
				results <- outcome{err: fmt.Errorf("mitigator panic: %v", r)}
			}
		}()
		if err := program.ValidateBindings(p.Spec(), bindings); err != nil {
			results <- outcome{err: err}
			return
		}
		// Each accessor returns defensive copies. Timed-out executions keep only this
		// private snapshot and read capabilities, never maps owned by ResponderLoop.
		snapshot := sdktest.Snapshot{NamespaceName: view.Namespace(), ConfigMapList: view.ConfigMaps(), ServiceList: view.Services(), PodList: view.Pods(), DeploymentList: view.Deployments(), ReplicaSetList: view.ReplicaSets(), EndpointList: view.Endpoints(), EndpointSliceList: view.EndpointSlices(), NetworkPolicyList: view.NetworkPolicies(), EventList: view.Events()}
		isolated := mitigationReadView{Snapshot: snapshot, ctx: ctx, live: view}
		plan, err := p.Mitigate(ctx, isolated, bindings)
		if err == nil {
			err = plan.Validate()
		}
		if err == nil && len(plan.Actions) == 0 {
			err = fmt.Errorf("program returned an empty plan")
		}
		if err == nil {
			// Reject unsupported actions and detach the returned values from authored memory.
			payload, encodeErr := json.Marshal(PlanApplicationResult{Plan: plan})
			err = encodeErr
			if err == nil {
				var decoded PlanApplicationResult
				err = json.Unmarshal(payload, &decoded)
				plan = decoded.Plan
			}
		}
		results <- outcome{plan: plan, err: err}
	}()
	select {
	case <-ctx.Done():
		return program.Plan{}, fmt.Errorf("mitigator deadline: %w", ctx.Err())
	case result := <-results:
		return result.plan, result.err
	}
}

type mitigationReadView struct {
	sdktest.Snapshot
	ctx  context.Context
	live program.ReadView
}

func (v mitigationReadView) Logs(ref sdk.ObjectRef, container string, since time.Duration) (string, error) {
	if err := v.ctx.Err(); err != nil {
		return "", err
	}
	return v.live.Logs(ref, container, since)
}
func (v mitigationReadView) ProbeTCP(ref sdk.ObjectRef, port int32) (program.ProbeResult, error) {
	if err := v.ctx.Err(); err != nil {
		return program.ProbeResult{}, err
	}
	return v.live.ProbeTCP(ref, port)
}
func (v mitigationReadView) ProbeHTTP(ref sdk.ObjectRef, request program.HTTPProbe) (program.ProbeResult, error) {
	if err := v.ctx.Err(); err != nil {
		return program.ProbeResult{}, err
	}
	return v.live.ProbeHTTP(ref, request)
}
func (v mitigationReadView) DiffAgainstSource(ref sdk.ObjectRef) (program.Diff, error) {
	if err := v.ctx.Err(); err != nil {
		return program.Diff{}, err
	}
	return v.live.DiffAgainstSource(ref)
}
func (v mitigationReadView) DiffAgainstBaseline(ref sdk.ObjectRef) (program.Diff, error) {
	if err := v.ctx.Err(); err != nil {
		return program.Diff{}, err
	}
	return v.live.DiffAgainstBaseline(ref)
}
