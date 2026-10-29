package validate

import (
	"slices"
	"strings"

	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// stageObservationSettled recognizes a completed lifecycle write whose cache
// observation failed, not a failed repair. The direct effect, bound diagnosis
// and later independent closure must all agree. Health alone proves nothing;
// later hand repairs and unresolved own findings are still checked separately.
func stageObservationSettled(stage *contracts.StageMitigation, outcome *contracts.OutcomeRecord) bool {
	initial, diagnosis := stage.GetApplyReceipt(), stage.GetPostApplyDiagnose()
	if stage.GetResult() != "rollout_failed" || outcome.GetClassification() != "success" ||
		len(stage.GetClearedFindingFingerprints()) == 0 || len(stage.GetRemainingFindingFingerprints()) != 0 ||
		diagnosis.GetVerdict().GetResult() != contracts.Verdict_RESULT_NO_MATCH || diagnosis.GetError() != "" ||
		diagnosis.GetProgramId() != stage.GetPlaybookId() || stage.GetProgramRevision() == "" || diagnosis.GetProgramRevision() != stage.GetProgramRevision() ||
		!slices.EqualFunc(diagnosis.GetBindings(), stage.GetBindings(), func(a, b *contracts.Binding) bool { return proto.Equal(a, b) }) ||
		initial.GetApplicationId() == "" || initial.GetStartedAt() == nil || initial.GetCompletedAt() == nil {
		return false
	}
	verified := outcome.GetTimestamps().GetVerifiedAt()
	if verified == nil || !verified.IsValid() || !initial.GetCompletedAt().IsValid() || verified.AsTime().Before(initial.GetCompletedAt().AsTime()) ||
		len(outcome.GetFinalHealthDetectorState()) == 0 || slices.ContainsFunc(outcome.GetFinalHealthDetectorState(), func(state *contracts.DetectorEvaluation) bool { return state.GetStatus() != "clear" }) {
		return false
	}
	var final *contracts.PlanApplication
	for _, application := range outcome.GetPlanApplications() {
		if application.GetApplicationId() == initial.GetApplicationId() {
			if final != nil {
				return false
			}
			final = application
		}
	}
	if final == nil || !final.GetApplied() || final.GetDryRun() || final.GetRolledBack() || final.GetRollbackError() != "" || final.GetRolloutPending() != nil ||
		!strings.HasPrefix(final.GetError(), "wait for the cache to observe plan writes: write observation unknown:") ||
		final.GetError() != initial.GetError() || !initial.GetApplied() || initial.GetDryRun() || initial.GetRolledBack() ||
		!slices.Contains(final.GetProgramIds(), stage.GetPlaybookId()) || !slices.Equal(final.GetProgramIds(), initial.GetProgramIds()) ||
		!proto.Equal(final.GetPlan(), initial.GetPlan()) || !proto.Equal(final.GetStartedAt(), initial.GetStartedAt()) || !proto.Equal(final.GetCompletedAt(), initial.GetCompletedAt()) {
		return false
	}
	actions := final.GetPlan().GetActions()
	if len(actions) == 0 || len(final.GetActionReports()) != len(actions) || len(final.GetObjectEffects()) != len(actions) ||
		!slices.EqualFunc(actions, final.GetActionsApplied(), func(a, b *contracts.Action) bool { return proto.Equal(a, b) }) {
		return false
	}
	for index, action := range actions {
		report := final.GetActionReports()[index]
		if report.GetActionIndex() != uint32(index) || report.GetStatus() != "applied" || report.GetFailureReason() != "" {
			return false
		}
		var ref *contracts.ObjectRef
		operation := "delete"
		switch {
		case action.GetDeleteObject() != nil:
			ref = action.GetDeleteObject().GetRef()
		case action.GetDeletePod() != nil:
			ref = action.GetDeletePod().GetRef()
		case action.GetCreateFromSource() != nil:
			ref, operation = action.GetCreateFromSource().GetRef(), "create"
		default:
			return false // Other effects need their own execution proof, not this lifecycle receipt.
		}
		matches := 0
		for _, effect := range final.GetObjectEffects() {
			if effect.GetConfirmed() && effect.GetState() != "unknown" && effect.GetState() != "not_applied" && effect.GetOperation() == operation && proto.Equal(effect.GetRef(), ref) {
				matches++
			}
		}
		if ref.GetName() == "" || matches != 1 {
			return false
		}
	}
	return true
}
