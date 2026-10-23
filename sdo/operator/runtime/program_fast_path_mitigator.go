package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	admissionv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"sdo.dev/operator/sdk/servicehealth"
	"sdo.dev/operator/sdk/traffic"
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
		if extended, ok := view.(interface{ StatefulSets() []appsv1.StatefulSet }); ok {
			snapshot.StatefulSetList = extended.StatefulSets()
		}
		if extended, ok := view.(interface {
			HorizontalPodAutoscalers() []autoscalingv2.HorizontalPodAutoscaler
		}); ok {
			snapshot.HPAList = extended.HorizontalPodAutoscalers()
		}
		if extended, ok := view.(interface {
			PodDisruptionBudgets() []policyv1.PodDisruptionBudget
		}); ok {
			snapshot.PDBList = extended.PodDisruptionBudgets()
		}
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
		if err == nil {
			plan, err = bindMitigationValues(plan, p.Spec(), bindings, isolated)
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

func (v mitigationReadView) ScopedDeployments(namespace string) ([]appsv1.Deployment, error) {
	return sdk.ScopedSource(v.live).ScopedDeployments(namespace)
}

func (v mitigationReadView) ScopedServices(namespace string) ([]corev1.Service, error) {
	return sdk.ScopedSource(v.live).ScopedServices(namespace)
}

func (v mitigationReadView) ScopedEndpoints(namespace string) ([]corev1.Endpoints, error) {
	return sdk.ScopedSource(v.live).ScopedEndpoints(namespace)
}

func (v mitigationReadView) MutatingWebhookConfigurations() ([]admissionv1.MutatingWebhookConfiguration, error) {
	return sdk.ScopedSource(v.live).MutatingWebhookConfigurations()
}

func (v mitigationReadView) ValidatingWebhookConfigurations() ([]admissionv1.ValidatingWebhookConfiguration, error) {
	return sdk.ScopedSource(v.live).ValidatingWebhookConfigurations()
}

func (v mitigationReadView) ObservationStatuses() []sdk.ObservationStatus {
	return sdk.ScopedSource(v.live).ObservationStatuses()
}
func (v mitigationReadView) ObservationGeneration() string {
	return sdk.ScopedSource(v.live).ObservationGeneration()
}

func (v mitigationReadView) ObservedAt() time.Time {
	if clock, ok := v.live.(sdk.ObservationClock); ok {
		return clock.ObservedAt()
	}
	return time.Time{}
}

func (v mitigationReadView) TrafficWindow(name string) (traffic.Window, bool) {
	if source, ok := v.live.(traffic.Source); ok {
		return source.TrafficWindow(name)
	}
	return traffic.Window{}, false
}

func (v mitigationReadView) ServiceReachability() (servicehealth.ReachabilityWindow, bool) {
	if source, ok := v.live.(servicehealth.ReachabilitySource); ok {
		return source.ServiceReachability()
	}
	return servicehealth.ReachabilityWindow{}, false
}

func (v mitigationReadView) MutatingWebhookConfiguration(name string) (*admissionv1.MutatingWebhookConfiguration, error) {
	result, err := sdk.ScopedSource(v.live).MutatingWebhookConfiguration(name)
	return result, err
}

func (v mitigationReadView) ValidatingWebhookConfiguration(name string) (*admissionv1.ValidatingWebhookConfiguration, error) {
	result, err := sdk.ScopedSource(v.live).ValidatingWebhookConfiguration(name)
	return result, err
}
