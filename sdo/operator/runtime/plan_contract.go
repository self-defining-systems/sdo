package runtime

import (
	"fmt"

	"google.golang.org/protobuf/types/known/structpb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// Proto projects an application onto the current PlanApplication contract.
// Duration and individual applied actions remain on the runtime result because
// the current protobuf message has no fields for them.
func (r PlanApplicationResult) Proto() (*contracts.PlanApplication, error) {
	if err := r.Plan.Validate(); err != nil {
		return nil, err
	}
	plan := &contracts.Plan{Rationale: r.Plan.Rationale}
	for _, action := range r.Plan.Actions {
		converted, err := planActionProto(action)
		if err != nil {
			return nil, err
		}
		plan.Actions = append(plan.Actions, converted)
	}
	application := &contracts.PlanApplication{Plan: plan, Applied: r.Applied, DryRun: r.DryRun}
	if r.Applied {
		verdict := contracts.Verdict_RESULT_UNKNOWN
		if r.VerificationCompleted {
			verdict = contracts.Verdict_RESULT_MATCH
			if r.VerificationPassed {
				verdict = contracts.Verdict_RESULT_NO_MATCH
			}
		}
		application.Verification = &contracts.Verdict{Result: verdict}
	}
	return application, nil
}

func planActionProto(action program.Action) (*contracts.Action, error) {
	switch a := action.(type) {
	case program.ReconcileFromSource:
		return &contracts.Action{Kind: &contracts.Action_ReconcileFromSource{ReconcileFromSource: &contracts.ReconcileFromSource{Ref: planRefProto(a.Ref)}}}, nil
	case program.Rollback:
		return &contracts.Action{Kind: &contracts.Action_Rollback{Rollback: &contracts.Rollback{Ref: planRefProto(a.Ref), ToRevision: a.ToRevision}}}, nil
	case program.SetField:
		value, err := structpb.NewValue(a.Value)
		if err != nil {
			return nil, fmt.Errorf("encode SetField value: %w", err)
		}
		return &contracts.Action{Kind: &contracts.Action_SetField{SetField: &contracts.SetField{Ref: planRefProto(a.Ref), Path: a.Path, Value: value, Reason: a.Reason}}}, nil
	case program.RestartRollout:
		return &contracts.Action{Kind: &contracts.Action_RestartRollout{RestartRollout: &contracts.RestartRollout{Ref: planRefProto(a.Ref), Reason: a.Reason}}}, nil
	case *program.ReconcileFromSource:
		if a != nil {
			return planActionProto(*a)
		}
	case *program.Rollback:
		if a != nil {
			return planActionProto(*a)
		}
	case *program.SetField:
		if a != nil {
			return planActionProto(*a)
		}
	case *program.RestartRollout:
		if a != nil {
			return planActionProto(*a)
		}
	}
	return nil, fmt.Errorf("unsupported plan action %T", action)
}

func planRefProto(ref sdk.ObjectRef) *contracts.ObjectRef {
	return &contracts.ObjectRef{ApiVersion: ref.APIVersion, Kind: ref.Kind, Namespace: ref.Namespace, Name: ref.Name}
}
