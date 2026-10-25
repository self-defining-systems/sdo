package runtime

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

// Unverified causes remain questions, never positive repair evidence. Keep one
// typed gap per original cause even when the executable-learning projection has
// no causes at all. No benchmark recovery signal can supply this proof.
func unverifiedCauseWork(outcome *contracts.OutcomeRecord, indices []uint32) []*contracts.ReflectionWorkItem {
	verified := map[uint32]bool{}
	for _, index := range indices {
		verified[index] = true
	}
	var work []*contracts.ReflectionWorkItem
	for index, cause := range outcome.GetConfirmedRootCauses() {
		if verified[uint32(index)] {
			continue
		}
		item := &contracts.ReflectionWorkItem{Disposition: "question", Reason: "evidence_gap", Question: fmt.Sprintf("Cause %d (%s) has no independently verified repair. Record the missing evidence without treating reported command success as repair proof.", index, cause.GetSummary())}
		for _, ref := range cause.GetResources() {
			item.Resources = append(item.Resources, proto.Clone(ref).(*contracts.ObjectRef))
		}
		work = append(work, item)
	}
	return work
}

func reflectionResource(ref *contracts.ObjectRef) sdk.ObjectRef {
	r := objectRefState(ref)
	r.APIVersion = ""
	return r
}

// partialRepairProof uses controller receipts, not an agent's success claim or
// aggregate health. Missing bound post-apply/final equivalence proof abstains.
// This includes controller-verified equivalents of manual recorded repairs.
func partialRepairProof(outcome *contracts.OutcomeRecord) (map[sdk.ObjectRef]time.Time, map[*contracts.PlanApplication]bool) {
	proof := map[sdk.ObjectRef]time.Time{}
	applications := map[*contracts.PlanApplication]bool{}
	revisions := programRunRevisions(outcome)
	for _, app := range outcome.GetPlanApplications() {
		if !app.GetApplied() || app.GetDryRun() || app.GetRolledBack() || app.GetError() != "" || app.GetRollbackError() != "" || !app.GetVerificationCompleted() || !app.GetVerificationPassed() || !finalApplicationVerified(app) || app.GetVerification().GetResult() != contracts.Verdict_RESULT_NO_MATCH || len(app.GetProgramIds()) == 0 {
			continue
		}
		bound := map[sdk.ObjectRef]bool{}
		verified := true
		for _, id := range app.GetProgramIds() {
			if !boundPostApplyNoMatch(outcome, app, id, revisions[id]) {
				verified = false
				break
			}
			for _, binding := range appliedPlanBindings(outcome, app, id) {
				for _, ref := range binding.Objects {
					ref.APIVersion = ""
					bound[ref] = true
				}
			}
		}
		if !verified {
			continue
		}
		refs := []sdk.ObjectRef{}
		for index, action := range app.GetPlan().GetActions() {
			scoped, err := applicationActionResources(app, index, action)
			if err != nil || len(scoped) == 0 {
				verified = false
				break
			}
			for _, ref := range scoped {
				ref.APIVersion = ""
				if !bound[ref] {
					verified = false
					break
				}
				refs = append(refs, ref)
			}
		}
		if !verified || len(refs) == 0 {
			continue
		}
		for _, call := range outcome.GetProgramToolCalls() {
			_, response, ok := parseProgramToolCall(call)
			if !ok || call.GetCommand() != "apply" || response.ApplicationID != app.GetApplicationId() || call.GetAt() == nil || call.GetAt().CheckValid() != nil {
				continue
			}
			at := call.GetAt().AsTime()
			if at.Before(app.GetCompletedAt().AsTime()) {
				continue
			}
			applications[app] = true
			for _, ref := range refs {
				if at.After(proof[ref]) {
					proof[ref] = at
				}
			}
			break
		}
	}
	return proof, applications
}

func verifiedPartialAction(action *contracts.RepairActionReceipt, proof map[sdk.ObjectRef]time.Time, applications map[*contracts.PlanApplication]bool) bool {
	if !action.GetSuccess() || len(action.GetResources()) == 0 || action.GetStartedAt() == nil || action.GetStartedAt().CheckValid() != nil || action.GetCompletedAt() == nil || action.GetCompletedAt().CheckValid() != nil || action.GetCompletedAt().AsTime().Before(action.GetStartedAt().AsTime()) {
		return false
	}
	for _, ref := range action.GetResources() {
		at, ok := proof[reflectionResource(ref)]
		if !ok || action.GetCompletedAt().AsTime().After(at) {
			return false
		}
	}
	for app := range applications {
		// Normal controller receipts name their application. Manual receipts
		// require the controller's distinct exact-equivalence application.
		if action.GetActionId() != "sdo-program-"+app.GetApplicationId() && !(action.GetKind() != "program-plan" && strings.HasPrefix(app.GetApplicationId(), "equivalent-repair-")) {
			continue
		}
		planned := map[sdk.ObjectRef]bool{}
		for index, item := range app.GetPlan().GetActions() {
			scoped, err := applicationActionResources(app, index, item)
			if err == nil {
				for _, ref := range scoped {
					ref.APIVersion = ""
					planned[ref] = true
				}
			}
		}
		covered := true
		for _, ref := range action.GetResources() {
			covered = covered && planned[reflectionResource(ref)]
		}
		if covered {
			return true
		}
	}
	return false
}

func partialRepairCauses(outcome *contracts.OutcomeRecord) []uint32 {
	proof, applications := partialRepairProof(outcome)
	repaired := map[sdk.ObjectRef]bool{}
	for _, action := range outcome.GetRepairActions() {
		if partialActionVerified(outcome, action, proof, applications) {
			for _, ref := range action.GetResources() {
				repaired[reflectionResource(ref)] = true
			}
		}
	}
	// Resource-only receipts cannot distinguish two claimed causes on the
	// same object. Abstain rather than teach an unrepaired peer cause.
	owners := map[sdk.ObjectRef]map[int]bool{}
	for index, cause := range outcome.GetConfirmedRootCauses() {
		for _, resource := range cause.GetResources() {
			ref := reflectionResource(resource)
			if owners[ref] == nil {
				owners[ref] = map[int]bool{}
			}
			owners[ref][index] = true
		}
	}
	var indices []uint32
	for index, cause := range outcome.GetConfirmedRootCauses() {
		verified := len(cause.GetResources()) > 0
		for _, ref := range cause.GetResources() {
			verified = verified && repaired[reflectionResource(ref)] && len(owners[reflectionResource(ref)]) == 1
		}
		if verified {
			indices = append(indices, uint32(index))
		}
	}
	return indices
}

// partialReflectionEligibility keeps original cause indices in durable memory;
// only the worker projection reindexes them to its compact cause list.
func partialReflectionEligibility(eligibility *contracts.ReflectionEligibility, indices []uint32, reindex bool) *contracts.ReflectionEligibility {
	result := &contracts.ReflectionEligibility{Version: 1}
	gaps := map[[4]string]bool{}
	for next, index := range indices {
		for _, cause := range eligibility.GetCauses() {
			if cause.GetCauseIndex() != index {
				continue
			}
			work := proto.Clone(cause).(*contracts.ReflectionCauseWork)
			if reindex {
				work.CauseIndex = uint32(next)
			}
			inside := func(ref *contracts.ObjectRef) bool {
				for _, resource := range work.GetResources() {
					if sameResource(objectRefState(ref), objectRefState(resource)) {
						return true
					}
				}
				return false
			}
			work.Items = nil
			for _, item := range cause.GetItems() {
				bounded := true
				for _, resource := range item.GetResources() {
					bounded = bounded && inside(resource)
				}
				for _, binding := range item.GetBindings() {
					if binding.GetObject() != nil {
						bounded = bounded && inside(binding.GetObject())
					}
				}
				for _, binding := range item.GetEvaluation().GetBindings() {
					if binding.GetObject() != nil {
						bounded = bounded && inside(binding.GetObject())
					}
				}
				if !bounded {
					continue
				}
				work.Items = append(work.Items, proto.Clone(item).(*contracts.ReflectionWorkItem))
				if item.GetReason() == "evidence_gap" {
					for _, ref := range item.GetResources() {
						gaps[[4]string{ref.GetKind(), ref.GetNamespace(), ref.GetName(), item.GetEvidencePhase()}] = true
					}
				}
			}
			if len(work.Items) == 0 {
				work.Items = []*contracts.ReflectionWorkItem{{Disposition: "question", Reason: "uncertain", Question: "Learn only this independently verified repaired cause; the whole incident remains failed."}}
			}
			result.Causes = append(result.Causes, work)
		}
	}
	count := uint32(len(gaps))
	result.EvidenceGapCount = &count
	return result
}

// partialReflectionOutcome is a learning view. The broker retains the complete
// failed outcome and its original classification as the authoritative record.
func partialReflectionOutcome(outcome *contracts.OutcomeRecord) *contracts.OutcomeRecord {
	current := proto.Clone(outcome).(*contracts.OutcomeRecord)
	indices := partialRepairCauses(outcome)
	current.ConfirmedRootCauses = nil
	allowed := map[sdk.ObjectRef]bool{}
	for _, index := range indices {
		cause := proto.Clone(outcome.ConfirmedRootCauses[index]).(*contracts.ConfirmedRootCause)
		current.ConfirmedRootCauses = append(current.ConfirmedRootCauses, cause)
		for _, ref := range cause.GetResources() {
			allowed[reflectionResource(ref)] = true
		}
	}
	onlyAllowed := func(refs []*contracts.ObjectRef) bool {
		if len(refs) == 0 {
			return false
		}
		for _, ref := range refs {
			if !allowed[reflectionResource(ref)] {
				return false
			}
		}
		return true
	}
	proof, applications := partialRepairProof(outcome)
	current.RepairActions = nil
	for _, action := range outcome.GetRepairActions() {
		if (partialActionVerified(outcome, action, proof, applications)) && onlyAllowed(action.GetResources()) {
			current.RepairActions = append(current.RepairActions, proto.Clone(action).(*contracts.RepairActionReceipt))
		}
	}
	current.PlanApplications = nil
	for _, app := range outcome.GetPlanApplications() {
		refs := []*contracts.ObjectRef{}
		for index, action := range app.GetPlan().GetActions() {
			scoped, err := applicationActionResources(app, index, action)
			if err == nil {
				for _, ref := range scoped {
					refs = append(refs, objectRefContract(ref))
				}
			}
		}
		if applications[app] && onlyAllowed(refs) {
			current.PlanApplications = append(current.PlanApplications, proto.Clone(app).(*contracts.PlanApplication))
		}
	}
	current.ProgramRuns = nil
	for _, run := range outcome.GetProgramRuns() {
		refs := []*contracts.ObjectRef{}
		for _, binding := range run.GetBindings() {
			if binding.GetObject() != nil {
				refs = append(refs, binding.GetObject())
			}
		}
		if onlyAllowed(refs) {
			current.ProgramRuns = append(current.ProgramRuns, proto.Clone(run).(*contracts.ProgramRun))
		}
	}
	current.Findings = nil
	for _, finding := range outcome.GetFindings() {
		if allowed[reflectionResource(finding.GetPrimaryResource())] {
			current.Findings = append(current.Findings, proto.Clone(finding).(*contracts.Finding))
		}
	}
	// Opaque tool JSON can contain unrelated bindings/actions. Do not present it
	// as learning evidence; retain the verified typed applications and diagnoses.
	current.ProgramToolCalls, current.ProgramFailureSignals, current.DriftEvents = nil, nil, nil
	for _, action := range current.RepairActions {
		if coldPartialAction(outcome, action) {
			summary, call := latestPartialVerification(outcome)
			evidence, _ := json.Marshal(struct {
				Command           string                   `json:"command"`
				VerifiedResources []*contracts.ObjectRef   `json:"verified_repaired_resources"`
				At                time.Time                `json:"verified_at"`
				Evaluations       []VerificationEvaluation `json:"detector_evaluations"`
				OtherOpenFindings int                      `json:"other_open_findings"`
			}{"verify", action.GetResources(), call.GetAt().AsTime(), summary.Evaluations, summary.FindingsCount})
			current.ProgramToolCalls = append(current.ProgramToolCalls, &contracts.ProgramToolCall{Command: "verify", At: call.At, RequestJson: call.RequestJson, ResponseJson: string(evidence)})
		}
	}
	current.SurfacedPlaybooks, current.InspectedPlaybooks, current.ConfirmedPlaybooks, current.RejectedPlaybooks, current.AppliedPlaybooks = nil, nil, nil, nil, nil
	current.ReflectionEligibility = partialReflectionEligibility(outcome.GetReflectionEligibility(), indices, true)
	current.ReflectionEligibility.Unassigned = unverifiedCauseWork(outcome, indices)
	return current
}

func trimPartialReflectionClosure(closed *contracts.IncidentClosure, current *contracts.OutcomeRecord) {
	if result := closed.GetResult(); result != nil {
		result.ConfirmedRootCauses = current.ConfirmedRootCauses
		result.RepairActions, result.PlanApplications, result.ProgramRuns = current.RepairActions, current.PlanApplications, current.ProgramRuns
		result.ProgramToolCalls, result.ProgramFailureSignals, result.DriftEvents = current.ProgramToolCalls, nil, nil
		result.AppliedPlaybooks, result.RepairChanges, result.VerificationEvidence = nil, nil, nil
		result.FinalDetectorStates = nil
	}
	if request := closed.GetRequest(); request != nil {
		if len(current.Findings) > 0 {
			request.Findings = current.Findings
		}
		request.InitialProgramRuns, request.DriftEvents, request.StateChanges = nil, nil, nil
		request.SurfacedPlaybooks = nil
	}
	closed.FinalStateChanges = nil
}
