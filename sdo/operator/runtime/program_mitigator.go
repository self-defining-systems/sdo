package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// Non-cooperative authored code cannot be killed by Go. Quarantine at most four
// executions for the lifetime of the process, including calls that timed out.
// Timed-out calls retain their slot until they actually return. No mutable loop
// state is passed to them and subsequent work fails closed when slots are spent.
var errEmptyMitigationPlan = errors.New("program returned an empty plan")

// NonExecutablePlanError is a structural program defect, not unavailable live
// evidence. The automatic stage hands it off without retrying the program.
type NonExecutablePlanError struct{ Cause error }

func (e *NonExecutablePlanError) Error() string { return e.Cause.Error() }
func (e *NonExecutablePlanError) Unwrap() error { return e.Cause }

func (l *ResponderLoop) terminalPlanError(id string) *NonExecutablePlanError {
	return l.terminalPlanErrors[id+"\x00"+l.programRevision(id)]
}
func (l *ResponderLoop) rememberTerminalPlanError(id, revision string, err *NonExecutablePlanError) {
	if l.terminalPlanErrors == nil {
		l.terminalPlanErrors = map[string]*NonExecutablePlanError{}
	}
	l.terminalPlanErrors[id+"\x00"+revision] = err
}

var mitigationSlots = make(chan struct{}, 4)

// runMitigator is called with mu held; authored code always runs without it.
// operationMu serializes tool/diagnosis operations while readers can still
// inspect the loop during a slow or non-cooperative authored call.
func (l *ResponderLoop) runMitigator(ctx context.Context, id string, p program.Mitigator, view program.ReadView, bindings program.Bindings) (plan program.Plan, err error) {
	if prior := l.terminalPlanError(id); prior != nil {
		return program.Plan{}, prior
	}
	deadline := l.deadline()
	bindings = cloneProgramBindings(bindings)
	if stepErr := withoutResponderLocks(l, "mitigator "+id, false, func() {
		plan, err = BuildMitigationPlan(ctx, p, view, bindings, deadline)
	}); stepErr != nil {
		err = stepErr
	}
	if err != nil {
		var terminal *NonExecutablePlanError
		if errors.As(err, &terminal) {
			l.rememberTerminalPlanError(id, l.programRevision(id), terminal)
		}
		l.addFailureSignal("mitigator_failure:" + id)
	}
	return plan, err
}

// BuildMitigationPlan is the one way a learned mitigation's plan is built: the
// automatic stage, the responder's plan command, follow-ups and replay
// validation all come here, so a plan one of them would refuse fails them all.
func BuildMitigationPlan(ctx context.Context, p program.Mitigator, view program.ReadView, bindings program.Bindings, deadline time.Duration) (program.Plan, error) {
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
		isolated := newMitigationReadView(ctx, view)
		observed, readFailure := program.CheckReads(isolated)
		plan, err := p.Mitigate(ctx, program.ReadFrom(observed), bindings)
		if readErr := readFailure(); readErr != nil {
			err = readErr
		}
		if err == nil {
			if validation := plan.Validate(); validation != nil {
				err = &NonExecutablePlanError{Cause: validation}
			}
		}
		if err == nil && len(plan.Actions) == 0 {
			err = &NonExecutablePlanError{Cause: errEmptyMitigationPlan}
		}
		if err == nil {
			// Reject unsupported actions and detach the returned values from authored memory.
			payload, encodeErr := json.Marshal(PlanApplicationResult{Plan: plan})
			if encodeErr != nil {
				err = &NonExecutablePlanError{Cause: encodeErr}
			}
			if err == nil {
				var decoded PlanApplicationResult
				if decodeErr := json.Unmarshal(payload, &decoded); decodeErr != nil {
					err = &NonExecutablePlanError{Cause: decodeErr}
				}
				plan = decoded.Plan
			}
		}
		if err == nil {
			plan, err = bindMitigationValues(plan, p.Spec(), bindings, isolated)
		}
		if err == nil {
			err = program.CheckReplicaSetScales(plan, isolated, bindings)
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

// mitigationReadView is the view a mitigator runs against. The typed object
// lists are a private frozen copy, so a call that outlives its deadline never
// touches maps owned by ResponderLoop. Every other read is the live view's own,
// by embedding: a read the live view offers cannot be silently dropped here, which
// is how a learned program saw no pod admission and returned an empty plan.
type mitigationReadView struct {
	program.ReadView
	frozen sdktest.Snapshot
	ctx    context.Context
}

func newMitigationReadView(ctx context.Context, live program.ReadView) mitigationReadView {
	frozen := sdktest.Snapshot{NamespaceName: live.Namespace(), ConfigMapList: live.ConfigMaps(), ServiceList: live.Services(), PodList: live.Pods(), DeploymentList: live.Deployments(), ReplicaSetList: live.ReplicaSets(), EndpointList: live.Endpoints(), EndpointSliceList: live.EndpointSlices(), NetworkPolicyList: live.NetworkPolicies(), EventList: live.Events(), StatefulSetList: live.StatefulSets(), HPAList: live.HorizontalPodAutoscalers(), PDBList: live.PodDisruptionBudgets()}
	return mitigationReadView{ReadView: live, frozen: frozen, ctx: ctx}
}

func (v mitigationReadView) Namespace() string                { return v.frozen.Namespace() }
func (v mitigationReadView) ConfigMaps() []corev1.ConfigMap   { return v.frozen.ConfigMaps() }
func (v mitigationReadView) Services() []corev1.Service       { return v.frozen.Services() }
func (v mitigationReadView) Pods() []corev1.Pod               { return v.frozen.Pods() }
func (v mitigationReadView) Deployments() []appsv1.Deployment { return v.frozen.Deployments() }
func (v mitigationReadView) ReplicaSets() []appsv1.ReplicaSet { return v.frozen.ReplicaSets() }
func (v mitigationReadView) Endpoints() []corev1.Endpoints    { return v.frozen.Endpoints() }
func (v mitigationReadView) EndpointSlices() []discoveryv1.EndpointSlice {
	return v.frozen.EndpointSlices()
}
func (v mitigationReadView) NetworkPolicies() []networkingv1.NetworkPolicy {
	return v.frozen.NetworkPolicies()
}
func (v mitigationReadView) Events() []corev1.Event             { return v.frozen.Events() }
func (v mitigationReadView) StatefulSets() []appsv1.StatefulSet { return v.frozen.StatefulSets() }
func (v mitigationReadView) HorizontalPodAutoscalers() []autoscalingv2.HorizontalPodAutoscaler {
	return v.frozen.HorizontalPodAutoscalers()
}
func (v mitigationReadView) PodDisruptionBudgets() []policyv1.PodDisruptionBudget {
	return v.frozen.PodDisruptionBudgets()
}
func (v mitigationReadView) ReadyEndpointCountForService(namespace, service string) int {
	return v.frozen.ReadyEndpointCountForService(namespace, service)
}
func (v mitigationReadView) PodsForService(namespace, service string) []corev1.Pod {
	return v.frozen.PodsForService(namespace, service)
}
func (v mitigationReadView) RecentEventsFor(namespace, kind, name string) []corev1.Event {
	return v.frozen.RecentEventsFor(namespace, kind, name)
}

// Object prefers the frozen typed copy so values pinned into the plan match the
// lists the mitigator read, and falls back to the live view for other kinds.
func (v mitigationReadView) Object(ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	object, err := v.frozen.Object(ref)
	if err == nil || !errors.Is(err, sdk.ErrObjectNotCaptured) {
		return object, err
	}
	if err := v.ctx.Err(); err != nil {
		return nil, err
	}
	return v.ReadView.Object(ref)
}

func (v mitigationReadView) Logs(ref sdk.ObjectRef, container string, since time.Duration) (string, error) {
	if err := v.ctx.Err(); err != nil {
		return "", err
	}
	return v.ReadView.Logs(ref, container, since)
}
func (v mitigationReadView) ProbeTCP(ref sdk.ObjectRef, port int32) (program.ProbeResult, error) {
	if err := v.ctx.Err(); err != nil {
		return program.ProbeResult{}, err
	}
	return v.ReadView.ProbeTCP(ref, port)
}
func (v mitigationReadView) ProbeHTTP(ref sdk.ObjectRef, request program.HTTPProbe) (program.ProbeResult, error) {
	if err := v.ctx.Err(); err != nil {
		return program.ProbeResult{}, err
	}
	return v.ReadView.ProbeHTTP(ref, request)
}
func (v mitigationReadView) DiffAgainstSource(ref sdk.ObjectRef) (program.Diff, error) {
	if err := v.ctx.Err(); err != nil {
		return program.Diff{}, err
	}
	return v.ReadView.DiffAgainstSource(ref)
}
func (v mitigationReadView) DiffAgainstBaseline(ref sdk.ObjectRef) (program.Diff, error) {
	if err := v.ctx.Err(); err != nil {
		return program.Diff{}, err
	}
	return v.ReadView.DiffAgainstBaseline(ref)
}

func (v mitigationReadView) DatabaseUsers(t program.DatabaseTarget) ([]program.DatabaseUser, error) {
	if err := v.ctx.Err(); err != nil {
		return nil, err
	}
	return v.ReadView.DatabaseUsers(t)
}
