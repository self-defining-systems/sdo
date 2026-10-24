package runtime

import (
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

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
	application := &contracts.PlanApplication{RolledBack: r.RolledBack, RollbackError: r.RollbackError, Plan: plan, Applied: r.Applied, DryRun: r.DryRun, VerificationCompleted: r.VerificationCompleted, VerificationPassed: r.VerificationPassed, FinalEquivalenceVerified: r.FinalEquivalenceVerified}
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
	for _, effect := range r.ObjectEffects {
		encoded := &contracts.PlanObjectEffect{Operation: effect.Operation, Ref: planRefProto(effect.Ref), Uid: effect.UID, State: effect.State, Confirmed: effect.Confirmed, Irreversible: effect.Irreversible}
		if effect.DeletedPreImage != nil {

			image, err := structpb.NewStruct(effect.DeletedPreImage.Object)
			if err != nil {
				return nil, fmt.Errorf("encode nonsecret delete receipt")
			}
			encoded.DeletedPreImage = image
		}
		application.ObjectEffects = append(application.ObjectEffects, encoded)
	}
	for _, report := range r.ActionReports {
		encoded := &contracts.ActionReport{ActionIndex: uint32(report.ActionIndex), Status: report.Status, ExitCode: report.ExitCode}
		for _, v := range report.Values {
			var ref *contracts.ObjectRef
			if v.Origin != program.ParameterValue {
				ref = planRefProto(v.Ref)
			}
			encoded.Values = append(encoded.Values, &contracts.ValueProvenance{Origin: string(v.Origin), Ref: ref, Path: v.Path, SourceCommit: v.SourceCommit, ResourceVersion: v.ResourceVersion, Parameter: v.Parameter})
		}
		application.ActionReports = append(application.ActionReports, encoded)
	}
	return application, nil
}

func planApplicationState(application *contracts.PlanApplication) RecordedPlanApplication {
	result := PlanApplicationResult{RolledBack: application.GetRolledBack(), RollbackError: application.GetRollbackError(), Applied: application.GetApplied(), DryRun: application.GetDryRun(), FinalEquivalenceVerified: application.FinalEquivalenceVerified}
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
	for _, effect := range application.GetObjectEffects() {
		decoded := PlanObjectEffect{Operation: effect.GetOperation(), Ref: objectRefState(effect.GetRef()), UID: effect.GetUid(), State: effect.GetState(), Confirmed: effect.GetConfirmed(), Irreversible: effect.GetIrreversible()}
		if effect.GetDeletedPreImage() != nil {
			decoded.DeletedPreImage = &unstructured.Unstructured{Object: effect.GetDeletedPreImage().AsMap()}
		}
		result.ObjectEffects = append(result.ObjectEffects, decoded)
	}
	for _, report := range application.GetActionReports() {
		decoded := PlanActionReport{ActionIndex: int(report.GetActionIndex()), Status: report.GetStatus(), ExitCode: report.ExitCode}
		for _, v := range report.GetValues() {
			decoded.Values = append(decoded.Values, program.ValueProvenance{Origin: program.ValueOrigin(v.GetOrigin()), Ref: objectRefState(v.GetRef()), Path: v.GetPath(), SourceCommit: v.GetSourceCommit(), ResourceVersion: v.GetResourceVersion(), Parameter: v.GetParameter()})
		}
		result.ActionReports = append(result.ActionReports, decoded)
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
	case program.CreateFromSource:
		return &contracts.Action{Kind: &contracts.Action_CreateFromSource{CreateFromSource: &contracts.CreateFromSource{Ref: planRefProto(a.Ref), Reason: a.Reason}}}, nil
	case program.DeleteObject:
		return &contracts.Action{Kind: &contracts.Action_DeleteObject{DeleteObject: &contracts.DeleteObject{Ref: planRefProto(a.Ref), Reason: a.Reason}}}, nil
	case *program.CreateFromSource:
		if a != nil {
			return planActionProto(*a)
		}
	case *program.DeleteObject:
		if a != nil {
			return planActionProto(*a)
		}
	case program.ReconcileFromSource:
		return &contracts.Action{Kind: &contracts.Action_ReconcileFromSource{ReconcileFromSource: &contracts.ReconcileFromSource{Ref: planRefProto(a.Ref)}}}, nil
	case program.Rollback:
		return &contracts.Action{Kind: &contracts.Action_Rollback{Rollback: &contracts.Rollback{Ref: planRefProto(a.Ref), ToRevision: a.ToRevision}}}, nil
	case program.SetField:
		value, err := planValueProto(a.Value)
		if err != nil {
			return nil, err
		}
		return &contracts.Action{Kind: &contracts.Action_SetField{SetField: &contracts.SetField{Ref: planRefProto(a.Ref), Path: a.Path, Value: value, Reason: a.Reason, DocumentFormat: a.DocumentFormat, DocumentPath: a.DocumentPath}}}, nil
	case program.DeletePod:
		return &contracts.Action{Kind: &contracts.Action_DeletePod{DeletePod: &contracts.DeletePod{Ref: planRefProto(a.Ref)}}}, nil
	case program.ScaleWorkload:
		value, err := planValueProto(a.Replicas)
		if err != nil {
			return nil, err
		}
		return &contracts.Action{Kind: &contracts.Action_ScaleWorkload{ScaleWorkload: &contracts.ScaleWorkload{Ref: planRefProto(a.Ref), Replicas: value}}}, nil
	case program.Exec:
		command := make([]*contracts.PlanValue, 0, len(a.Command))
		for _, v := range a.Command {
			value, err := planValueProto(v)
			if err != nil {
				return nil, err
			}
			command = append(command, value)
		}
		return &contracts.Action{Kind: &contracts.Action_Exec{Exec: &contracts.Exec{Pod: planRefProto(a.Pod), Container: a.Container, Command: command}}}, nil
	case *program.DeletePod:
		if a != nil {
			return planActionProto(*a)
		}
	case *program.ScaleWorkload:
		if a != nil {
			return planActionProto(*a)
		}
	case *program.Exec:
		if a != nil {
			return planActionProto(*a)
		}

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
	case *contracts.Action_CreateFromSource:
		decoded = program.CreateFromSource{Ref: objectRefState(kind.CreateFromSource.GetRef()), Reason: kind.CreateFromSource.GetReason()}
	case *contracts.Action_DeleteObject:
		decoded = program.DeleteObject{Ref: objectRefState(kind.DeleteObject.GetRef()), Reason: kind.DeleteObject.GetReason()}
	case *contracts.Action_ReconcileFromSource:
		decoded = program.ReconcileFromSource{Ref: objectRefState(kind.ReconcileFromSource.GetRef())}
	case *contracts.Action_Rollback:
		decoded = program.Rollback{Ref: objectRefState(kind.Rollback.GetRef()), ToRevision: kind.Rollback.GetToRevision()}
	case *contracts.Action_SetField:
		decoded = program.SetField{Ref: objectRefState(kind.SetField.GetRef()), Path: kind.SetField.GetPath(), Value: planValueState(kind.SetField.GetValue()), Reason: kind.SetField.GetReason(), DocumentFormat: kind.SetField.GetDocumentFormat(), DocumentPath: kind.SetField.GetDocumentPath()}
	case *contracts.Action_DeletePod:
		decoded = program.DeletePod{Ref: objectRefState(kind.DeletePod.GetRef())}
	case *contracts.Action_ScaleWorkload:
		decoded = program.ScaleWorkload{Ref: objectRefState(kind.ScaleWorkload.GetRef()), Replicas: planValueState(kind.ScaleWorkload.GetReplicas())}
	case *contracts.Action_Exec:
		a := program.Exec{Pod: objectRefState(kind.Exec.GetPod()), Container: kind.Exec.GetContainer()}
		for _, v := range kind.Exec.GetCommand() {
			a.Command = append(a.Command, planValueState(v))
		}
		decoded = a
	case *contracts.Action_RestartRollout:
		decoded = program.RestartRollout{Ref: objectRefState(kind.RestartRollout.GetRef()), Reason: kind.RestartRollout.GetReason()}
	}
	return decoded
}

func planValueProto(v program.Value) (*contracts.PlanValue, error) {
	if err := v.Validate(); err != nil {
		return nil, err
	}
	field := &contracts.ValueField{Ref: planRefProto(v.Ref()), Path: v.Path()}
	switch v.Origin() {
	case program.SourceValue:
		return &contracts.PlanValue{Origin: &contracts.PlanValue_Source{Source: field}}, nil
	case program.ObservedValue:
		return &contracts.PlanValue{Origin: &contracts.PlanValue_Observed{Observed: field}}, nil
	case program.ParameterValue:
		return &contracts.PlanValue{Origin: &contracts.PlanValue_Parameter{Parameter: v.Param()}}, nil
	}
	return nil, fmt.Errorf("invalid value origin")
}
func planValueState(v *contracts.PlanValue) program.Value {
	switch origin := v.GetOrigin().(type) {
	case *contracts.PlanValue_Source:
		return program.FromSource(objectRefState(origin.Source.GetRef()), origin.Source.GetPath())
	case *contracts.PlanValue_Observed:
		return program.FromObserved(objectRefState(origin.Observed.GetRef()), origin.Observed.GetPath())
	case *contracts.PlanValue_Parameter:
		return program.FromParam(origin.Parameter)
	}
	return program.Value{}
}
