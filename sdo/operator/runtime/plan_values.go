package runtime

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type PlanActionReport struct {
	ActionIndex int                       `json:"action_index"`
	Status      string                    `json:"status"`
	Values      []program.ValueProvenance `json:"values,omitempty"`
	ExitCode    *int32                    `json:"exit_code,omitempty"`
}

func newPlanReports(plan program.Plan) []PlanActionReport {
	out := make([]PlanActionReport, len(plan.Actions))
	for i := range out {
		out[i] = PlanActionReport{ActionIndex: i, Status: "prepared"}
	}
	return out
}
func settlePlanReports(result *PlanApplicationResult, err error) {
	for i := range result.ActionReports {
		if i < len(result.ActionsApplied) {
			result.ActionReports[i].Status = "applied"
		} else if err != nil && result.ActionReports[i].Status != "unknown" {
			result.ActionReports[i].Status = "refused"
			if result.PendingAction != nil && *result.PendingAction == i {
				result.ActionReports[i].Status = "unknown"
			}
		}
	}
}
func (e PlanExecutor) valueInputs(ctx context.Context, incident IncidentRequest) program.ValueInputs {
	in := e.Values
	in.SourceCommit = incident.SourceCommit
	in.Source = func(ref sdk.ObjectRef) (map[string]any, error) {
		if err := program.FromSource(ref, "metadata.name").Validate(); err != nil {
			return nil, err
		}
		if incident.Namespace != "" && ref.Namespace != incident.Namespace {
			return nil, fmt.Errorf("value source is outside incident namespace")
		}
		if e.Source == nil {
			return nil, fmt.Errorf("accepted source is required")
		}

		object, err := e.Source.AcceptedManifest(ctx, incident.SourceCommit, ref)
		if err != nil || object == nil || !samePlanRef(object, ref) {
			return nil, fmt.Errorf("accepted value object unavailable")
		}

		return object.Object, nil
	}
	return in
}

// verifyValueWrite proves the exact changed fields with a direct server read.
// Health clearance and informer freshness cannot stand in for write evidence.
func (e PlanExecutor) verifyValueWrite(ctx context.Context, action program.Action, patch PlanPatch) error {
	path := ""
	switch a := action.(type) {
	case program.SetField:
		path = a.Path
	case *program.SetField:
		if a != nil {
			path = a.Path
		}
	case program.ScaleWorkload, *program.ScaleWorkload:
		path = "spec.replicas"
	default:
		return nil
	}
	live, err := e.Client.Get(ctx, patch.Ref)
	if err != nil || live == nil || !samePlanRef(live, patch.Ref) {
		return fmt.Errorf("value write observation unknown: current read failed")
	}
	if patch.Before.GetUID() != "" && live.GetUID() != patch.Before.GetUID() {
		return fmt.Errorf("value write observation unknown: UID changed")
	}
	live = live.DeepCopy()
	canonicalizePlanObject(live)
	expected, expectedErr := program.FieldValue(patch.After.Object, path)
	actual, actualErr := program.FieldValue(live.Object, path)
	if expectedErr != nil || actualErr != nil {
		return fmt.Errorf("value write observation unknown: requested path unavailable")
	}
	// Requested leaves must match even for no-op writes. Extra server defaults
	// remain allowed; the delta check below also proves requested removals.
	if !planRequestedValueObserved(expected, actual) {
		return fmt.Errorf("value write observation unknown: requested value changed")
	}

	for _, field := range []string{"spec", "data", "binaryData"} {
		b, bok := patch.Before.Object[field]
		a, aok := patch.After.Object[field]
		v, vok := live.Object[field]
		if !planFieldsMatch(b, bok, a, aok, v, vok, false) {
			return fmt.Errorf("value write observation unknown: intended field value not observed")
		}
	}
	return nil
}

// planRequestedValueObserved compares every requested leaf while allowing the
// API server to add defaulted object fields.
func planRequestedValueObserved(expected, actual any) bool {
	if want, ok := expected.(map[string]any); ok {
		got, ok := actual.(map[string]any)
		if !ok {
			return false
		}
		for key, value := range want {
			observed, exists := got[key]
			if !exists || !planRequestedValueObserved(value, observed) {
				return false
			}
		}
		return true
	}
	if want, ok := expected.([]any); ok {
		got, ok := actual.([]any)
		if !ok || len(want) != len(got) {
			return false
		}
		for i := range want {
			if !planRequestedValueObserved(want[i], got[i]) {
				return false
			}
		}
		return true
	}
	return planValuesEqual(expected, actual)
}

func actionUsesSourceValue(action program.Action) bool {
	for _, value := range planActionValues(action) {
		if value.Origin() == program.SourceValue {
			return true
		}
	}
	return false
}

func planActionValues(action program.Action) []program.Value {
	switch a := action.(type) {
	case program.SetField:
		return []program.Value{a.Value}
	case program.ScaleWorkload:
		return []program.Value{a.Replicas}
	case program.Exec:
		return a.Command
	case *program.SetField:
		if a != nil {
			return planActionValues(*a)
		}
	case *program.ScaleWorkload:
		if a != nil {
			return planActionValues(*a)
		}
	case *program.Exec:
		if a != nil {
			return planActionValues(*a)
		}
	}
	return nil
}

// planObservedObject reads only the program's supplied view. No API read or
// Secret lookup can be substituted for an unavailable snapshot observation.
func planObservedObject(view program.ReadView, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	versions := map[string]string{"Pod": "v1", "Service": "v1", "ConfigMap": "v1", "Endpoints": "v1", "Event": "v1", "Deployment": "apps/v1", "ReplicaSet": "apps/v1", "StatefulSet": "apps/v1", "HorizontalPodAutoscaler": "autoscaling/v2", "PodDisruptionBudget": "policy/v1", "NetworkPolicy": "networking.k8s.io/v1", "EndpointSlice": "discovery.k8s.io/v1", "MutatingWebhookConfiguration": "admissionregistration.k8s.io/v1", "ValidatingWebhookConfiguration": "admissionregistration.k8s.io/v1"}
	if versions[ref.Kind] == "" || versions[ref.Kind] != ref.APIVersion {
		return nil, fmt.Errorf("observed value kind/version unavailable")
	}
	if ref.Namespace != view.Namespace() {
		return (LiveReadView{DetectionContext: view}).snapshotObject(ref)
	}
	var object any
	switch ref.Kind {
	case "StatefulSet":
		if extended, ok := view.(interface{ StatefulSets() []appsv1.StatefulSet }); ok {
			for _, item := range extended.StatefulSets() {
				if item.Namespace == ref.Namespace && item.Name == ref.Name {
					object = &item
					break
				}
			}
		}
	case "HorizontalPodAutoscaler":
		if extended, ok := view.(interface {
			HorizontalPodAutoscalers() []autoscalingv2.HorizontalPodAutoscaler
		}); ok {
			for _, item := range extended.HorizontalPodAutoscalers() {
				if item.Namespace == ref.Namespace && item.Name == ref.Name {
					object = &item
					break
				}
			}
		}
	case "PodDisruptionBudget":
		if extended, ok := view.(interface {
			PodDisruptionBudgets() []policyv1.PodDisruptionBudget
		}); ok {
			for _, item := range extended.PodDisruptionBudgets() {
				if item.Namespace == ref.Namespace && item.Name == ref.Name {
					object = &item
					break
				}
			}
		}
	case "Event":
		for _, item := range view.Events() {
			if item.Namespace == ref.Namespace && item.Name == ref.Name {
				object = &item
				break
			}
		}
	default:
		return (LiveReadView{DetectionContext: view}).snapshotObject(ref)
	}
	if object == nil {
		return nil, fmt.Errorf("observed value object unavailable")
	}
	return planUnstructured(object, ref)
}

func bindMitigationValues(plan program.Plan, spec program.ProgramSpec, bindings program.Bindings, view program.ReadView) (program.Plan, error) {
	observations := map[sdk.ObjectRef]*unstructured.Unstructured{}
	for _, action := range plan.Actions {
		for _, value := range planActionValues(action) {
			if value.Origin() != program.ObservedValue {
				continue
			}
			ref := value.Ref()
			if _, ok := observations[ref]; ok {
				continue
			}
			object, err := planObservedObject(view, ref)
			if err != nil {
				return program.Plan{}, err
			}
			observations[ref] = object.DeepCopy()
		}
	}
	return program.BindPlanValues(plan, program.ValueInputs{Spec: spec, Bindings: bindings.Clone(), Observed: func(ref sdk.ObjectRef) (map[string]any, error) {
		object := observations[ref]
		if object == nil {
			return nil, fmt.Errorf("program snapshot value object unavailable")
		}
		return object.DeepCopy().Object, nil
	}}), nil
}
