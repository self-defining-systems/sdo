package runtime

import (
	"context"
	"fmt"
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
		if err := e.validateAcceptedRevision(incident); err != nil {
			return nil, err
		}
		object, err := e.Source.AcceptedManifest(ctx, incident.SourceCommit, ref)
		if err != nil || object == nil || !samePlanRef(object, ref) {
			return nil, fmt.Errorf("accepted value object unavailable")
		}
		if err := planSecretPredicate(object); err != nil {
			return nil, err
		}
		return object.Object, nil
	}
	return in
}

// verifyValueWrite proves the exact changed fields with a direct server read.
// Health clearance and informer freshness cannot stand in for write evidence.
func (e PlanExecutor) verifyValueWrite(ctx context.Context, action program.Action, patch PlanPatch) error {
	switch action.(type) {
	case program.SetField, *program.SetField, program.ScaleWorkload, *program.ScaleWorkload:
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

func actionUsesSourceValue(action program.Action) bool {
	switch a := action.(type) {
	case program.SetField:
		return a.Value.Origin() == program.SourceValue
	case program.ScaleWorkload:
		return a.Replicas.Origin() == program.SourceValue
	case program.Exec:
		for _, v := range a.Command {
			if v.Origin() == program.SourceValue {
				return true
			}
		}
	case *program.SetField:
		if a != nil {
			return actionUsesSourceValue(*a)
		}
	case *program.ScaleWorkload:
		if a != nil {
			return actionUsesSourceValue(*a)
		}
	case *program.Exec:
		if a != nil {
			return actionUsesSourceValue(*a)
		}
	}
	return false
}
