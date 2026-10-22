package runtime

import (
	"fmt"

	"google.golang.org/protobuf/types/known/structpb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// Proto projects an application onto the current PlanApplication contract.
// Only completed action and verification evidence crosses the durable boundary.
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
	application := &contracts.PlanApplication{Plan: plan, Applied: r.Applied, DryRun: r.DryRun, VerificationCompleted: r.VerificationCompleted, VerificationPassed: r.VerificationPassed}
	for _, action := range r.ActionsApplied {
		converted, err := planActionProto(action)
		if err != nil {
			return nil, err
		}
		application.ActionsApplied = append(application.ActionsApplied, converted)
	}
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

func planApplicationState(application *contracts.PlanApplication) RecordedPlanApplication {
	result := PlanApplicationResult{Applied: application.GetApplied(), DryRun: application.GetDryRun()}
	if application.GetPlan() != nil {
		result.Plan.Rationale = application.GetPlan().GetRationale()
		for _, action := range application.GetPlan().GetActions() {
			if decoded := planActionState(action); decoded != nil {
				result.Plan.Actions = append(result.Plan.Actions, decoded)
			}
		}
	}
	for _, action := range application.GetActionsApplied() {
		if decoded := planActionState(action); decoded != nil {
			result.ActionsApplied = append(result.ActionsApplied, decoded)
		}
	}
	result.VerificationCompleted = application.GetVerificationCompleted() || application.GetVerification() != nil
	result.VerificationPassed = application.GetVerificationPassed() || application.GetVerification().GetResult() == contracts.Verdict_RESULT_NO_MATCH
	if application.GetDuration() != nil {
		result.Duration = application.GetDuration().AsDuration()
	}
	return RecordedPlanApplication{ID: application.GetApplicationId(), ProgramIDs: append([]string(nil), application.GetProgramIds()...), Result: result,
		StartedAt: controllerTime(application.GetStartedAt()), CompletedAt: controllerTime(application.GetCompletedAt()), Error: application.GetError(), RolledBack: application.GetRolledBack(), RollbackError: application.GetRollbackError()}
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

func planActionState(action *contracts.Action) program.Action {
	var decoded program.Action
	switch kind := action.GetKind().(type) {
	case *contracts.Action_ReconcileFromSource:
		decoded = program.ReconcileFromSource{Ref: objectRefState(kind.ReconcileFromSource.GetRef())}
	case *contracts.Action_Rollback:
		decoded = program.Rollback{Ref: objectRefState(kind.Rollback.GetRef()), ToRevision: kind.Rollback.GetToRevision()}
	case *contracts.Action_SetField:
		decoded = program.SetField{Ref: objectRefState(kind.SetField.GetRef()), Path: kind.SetField.GetPath(), Value: kind.SetField.GetValue().AsInterface(), Reason: kind.SetField.GetReason()}
	case *contracts.Action_RestartRollout:
		decoded = program.RestartRollout{Ref: objectRefState(kind.RestartRollout.GetRef()), Reason: kind.RestartRollout.GetReason()}
	}
	return decoded
}
