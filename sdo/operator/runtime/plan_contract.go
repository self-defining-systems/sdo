package runtime

import (
	"encoding/json"
	"fmt"
	utiljson "k8s.io/apimachinery/pkg/util/json"
	"sdo.dev/operator/planwire"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"google.golang.org/protobuf/types/known/structpb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// Proto projects an application onto the current PlanApplication contract.
// Applied actions, pending-rollout observations and verification evidence cross
// the durable boundary; pending observations never become verification credit.
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
	application := &contracts.PlanApplication{RolledBack: r.RolledBack, RollbackError: r.RollbackError, Plan: plan, Applied: r.Applied, DryRun: r.DryRun, VerificationCompleted: r.VerificationCompleted, VerificationPassed: r.VerificationPassed, FinalEquivalenceVerified: r.FinalEquivalenceVerified, FinalVerificationBasis: r.FinalVerificationBasis, SettlementRejectionReason: r.SettlementRejectionReason, RolloutPending: r.RolloutPending, RecoveryWait: r.RecoveryWait}
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
	for _, effect := range r.observedObjectEffects() {
		encoded := &contracts.PlanObjectEffect{Operation: effect.Operation, Ref: planRefProto(effect.Ref), Uid: effect.UID, State: effect.State, Confirmed: effect.Confirmed, Irreversible: effect.Irreversible}
		if effect.DeletedPreImage != nil {
			object := effect.DeletedPreImage.DeepCopy().Object
			sdk.ProjectWebhookCABundles(object)
			image, err := structpb.NewStruct(object)
			if err != nil {
				return nil, fmt.Errorf("encode delete receipt")
			}
			encoded.DeletedPreImage = image
		}
		application.ObjectEffects = append(application.ObjectEffects, encoded)
	}
	for _, report := range r.ActionReports {
		encoded := &contracts.ActionReport{ActionIndex: uint32(report.ActionIndex), Status: report.Status, ExitCode: report.ExitCode}
		encoded.SourceReconcile = report.SourceReconcile
		encoded.WebhookCaDigest = report.WebhookCADigest
		encoded.FailureReason = report.FailureReason
		encoded.RecordingError = report.RecordingError
		encoded.JobStatusJson = report.JobStatus
		if report.Script != nil {
			encoded.ScriptResultJson, _ = json.Marshal(report.Script)
		}
		if report.ExternalEffect != nil {
			encoded.ExternalEffectJson, _ = json.Marshal(report.ExternalEffect)
		}
		if report.KubernetesEffect != nil {
			encoded.KubernetesEffectJson, _ = json.Marshal(report.KubernetesEffect)
		}
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
	result := PlanApplicationResult{RolledBack: application.GetRolledBack(), RollbackError: application.GetRollbackError(), Applied: application.GetApplied(), DryRun: application.GetDryRun(), FinalEquivalenceVerified: application.FinalEquivalenceVerified, FinalVerificationBasis: application.GetFinalVerificationBasis(), SettlementRejectionReason: application.GetSettlementRejectionReason(), RolloutPending: application.GetRolloutPending(), RecoveryWait: application.GetRecoveryWait()}
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
		decoded.SourceReconcile = report.GetSourceReconcile()
		decoded.WebhookCADigest = report.GetWebhookCaDigest()
		decoded.FailureReason = report.GetFailureReason()
		decoded.RecordingError = report.GetRecordingError()
		decoded.JobStatus = report.GetJobStatusJson()
		if len(report.GetScriptResultJson()) > 0 {
			var script ScriptExecutionResult
			if json.Unmarshal(report.GetScriptResultJson(), &script) == nil {
				decoded.Script = &script
			}
		}
		if len(report.GetExternalEffectJson()) > 0 {
			var effect program.ExternalEffect
			if json.Unmarshal(report.GetExternalEffectJson(), &effect) == nil {
				decoded.ExternalEffect = &effect
			}
		}
		if len(report.GetKubernetesEffectJson()) > 0 {
			var effect program.KubernetesEffect
			if utiljson.Unmarshal(report.GetKubernetesEffectJson(), &effect) == nil {
				decoded.KubernetesEffect = &effect
			}
		}
		for _, v := range report.GetValues() {
			decoded.Values = append(decoded.Values, program.ValueProvenance{Origin: program.ValueOrigin(v.GetOrigin()), Ref: objectRefState(v.GetRef()), Path: v.GetPath(), SourceCommit: v.GetSourceCommit(), ResourceVersion: v.GetResourceVersion(), Parameter: v.GetParameter()})
		}
		result.ActionReports = append(result.ActionReports, decoded)
	}
	result.VerificationCompleted = application.GetVerificationCompleted() || (application.GetVerification() != nil && application.GetRolloutPending() == nil)
	result.VerificationPassed = application.GetVerificationPassed() || application.GetVerification().GetResult() == contracts.Verdict_RESULT_NO_MATCH
	if application.GetDuration() != nil {
		result.Duration = application.GetDuration().AsDuration()
	}
	return RecordedPlanApplication{ID: application.GetApplicationId(), ProgramIDs: append([]string(nil), application.GetProgramIds()...), Result: result,
		StartedAt: controllerTime(application.GetStartedAt()), CompletedAt: controllerTime(application.GetCompletedAt()), Error: application.GetError(), RolledBack: application.GetRolledBack(), RollbackError: application.GetRollbackError()}
}

// observedObjectEffects carries UID-bound direct write evidence across the
// receipt boundary without serializing mutable patch preimages. Successful
// verification already required the executor's identity/value readback.
func (r PlanApplicationResult) observedObjectEffects() []PlanObjectEffect {
	effects := append([]PlanObjectEffect(nil), r.ObjectEffects...)
	if !r.Applied || r.DryRun || r.RolledBack || r.RollbackError != "" || !r.VerificationCompleted || !r.VerificationPassed || r.RolloutPending != nil {
		return effects
	}
	seen := map[sdk.ObjectRef]bool{}
	for _, effect := range effects {
		seen[effect.Ref] = true
	}
	for _, patch := range r.Patches {
		if seen[patch.Ref] || patch.Operation != "" || patch.Before == nil || patch.Before.GetUID() == "" || patch.ObservedUID != string(patch.Before.GetUID()) {
			continue
		}
		effects = append(effects, PlanObjectEffect{Operation: "patch", Ref: patch.Ref, UID: string(patch.Before.GetUID()), State: "applied", Confirmed: true})
		seen[patch.Ref] = true
	}
	return effects
}

// Action conversion lives in planwire so the memory host shares it.
func planActionProto(action program.Action) (*contracts.Action, error) {
	return planwire.ActionProto(action)
}
func planActionState(action *contracts.Action) program.Action { return planwire.ActionState(action) }
func planRefProto(ref sdk.ObjectRef) *contracts.ObjectRef     { return planwire.RefProto(ref) }
func generalAction(a program.Action) program.Action           { return planwire.GeneralAction(a) }
func isGeneralAction(a program.Action) bool                   { return planwire.IsGeneralAction(a) }
func concreteExternalAction(a program.Action) program.Action {
	return planwire.ConcreteExternalAction(a)
}
func isExternalAction(a program.Action) bool { return planwire.IsExternalAction(a) }
