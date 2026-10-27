package runtime

import (
	"context"
	"sort"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
)

func cloneResponseProgress(items []*contracts.ResponseProgress) []*contracts.ResponseProgress {
	result := make([]*contracts.ResponseProgress, 0, len(items))
	for _, item := range items {
		if item != nil {
			result = append(result, proto.Clone(item).(*contracts.ResponseProgress))
		}
	}
	return result
}

func priorResponseProgress(repository string, findings []sdk.Finding) []*contracts.ResponseProgress {
	stored, _, err := memory.ListOutcomes(context.Background(), memory.OpenReader(repository), "open response progress")
	if err != nil {
		return nil
	}
	var records []*contracts.OutcomeRecord
	for _, item := range stored {
		records = append(records, item.Record)
	}
	return responseProgress(records, findings, time.Time{})
}

// Failed episodes retain factual writes and blockers, without granting repair,
// rollback or learning authority. A verified healthy closure ends the open
// problem; similar later incidents must not inherit its already-done work.
func responseProgress(records []*contracts.OutcomeRecord, findings []sdk.Finding, healthyAfter time.Time) []*contracts.ResponseProgress {
	for _, record := range records {
		healthy := record.GetClassification() == "success" || record.GetClassification() == "cleared_without_action"
		at := outcomeCompletedAt(record)
		// A failed worker can still have an independently healthy closure.
		// Classification alone must not turn that later recurrence into a retry.
		if verified := record.GetTimestamps().GetVerifiedAt(); verified != nil && verified.IsValid() {
			healthy = true
			if verified.AsTime().After(at) {
				at = verified.AsTime()
			}
		}
		if healthy && at.After(healthyAfter) {
			healthyAfter = at
		}
	}
	current := map[string]sdk.ObjectRef{}
	for _, finding := range findings {
		current[finding.DetectorID+"\x00"+FindingFingerprint(finding)] = finding.PrimaryResource
	}
	ordered := append([]*contracts.OutcomeRecord(nil), records...)
	sort.SliceStable(ordered, func(i, j int) bool {
		a, b := outcomeCompletedAt(ordered[i]), outcomeCompletedAt(ordered[j])
		if a.Equal(b) {
			return ordered[i].GetIncidentId() < ordered[j].GetIncidentId()
		}
		return a.Before(b)
	})
	var result []*contracts.ResponseProgress
	for _, record := range ordered {
		if record.GetClassification() != "failed" || !healthyAfter.IsZero() && !outcomeCompletedAt(record).After(healthyAfter) {
			continue
		}
		sameProblem := false
		for _, finding := range record.GetFindings() {
			ref, present := current[finding.GetDetectorId()+"\x00"+finding.GetFingerprint()]
			if finding.GetFingerprint() != "" && present && sameObject(finding.GetPrimaryResource(), ref) {
				sameProblem = true
				break
			}
		}
		if !sameProblem {
			continue
		}
		progress := &contracts.ResponseProgress{IncidentId: record.GetIncidentId()}
		errors := append([]string(nil), record.GetProgramFailureSignals()...)
		for _, action := range record.GetRepairActions() {
			if !action.GetSuccess() {
				reason := action.GetFailureReason()
				if reason == "" {
					reason = action.GetSummary()
				}
				if details := action.GetDetails(); details != "" {
					if reason != "" {
						reason += ": "
					}
					reason += details
				}
				if reason != "" {
					errors = append(errors, reason)
				}
			}
			if action.GetSuccess() {
				progress.SuccessfulActions = append(progress.SuccessfulActions, proto.Clone(action).(*contracts.RepairActionReceipt))
			}
		}
		for _, application := range record.GetPlanApplications() {
			if application.GetError() != "" {
				errors = append(errors, application.GetError())
			}
			if application.GetApplied() && !application.GetRolledBack() && application.GetRolloutPending() != nil {
				progress.Blockers = append(progress.Blockers, proto.Clone(application.RolloutPending).(*contracts.RolloutPending))
			}
		}
		// Stage reports may have survived a failed worker without its final result.
		for _, stage := range record.GetPriorStages() {
			for _, attempt := range stage.GetMitigations() {
				application := attempt.GetApplyReceipt()
				if application.GetApplied() && !application.GetRolledBack() && application.GetRolloutPending() != nil {
					duplicate := false
					for _, blocker := range progress.Blockers {
						if proto.Equal(blocker, application.RolloutPending) {
							duplicate = true
							break
						}
					}
					if !duplicate {
						progress.Blockers = append(progress.Blockers, proto.Clone(application.RolloutPending).(*contracts.RolloutPending))
					}
				}
			}
		}
		progress.Error = strings.Join(errors, "; ")
		if len(progress.SuccessfulActions) > 0 || len(progress.Blockers) > 0 || progress.Error != "" {
			result = append(result, progress)
		}
	}
	return result
}
