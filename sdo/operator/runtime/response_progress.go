package runtime

import (
	"context"
	"encoding/json"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

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
	return boundResponseProgress(result, maxResponseProgressBytes)
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
			result = boundResponseProgress(append(result, progress), maxResponseProgressBytes)
		}
	}
	return result
}

// These are projection budgets, never durable-history or repair-credit limits.
// Keep chronological order for consumers, evicting the oldest episodes first.
const maxResponseProgressBytes = 32 << 10
const maxResponseProgressEpisodes = 8

func boundResponseProgress(items []*contracts.ResponseProgress, budget int) []*contracts.ResponseProgress {
	var kept []*contracts.ResponseProgress
	var omitted uint32
	truncated := false
	for i := len(items) - 1; i >= 0; i-- {
		item := items[i]
		if item == nil {
			continue
		}
		omitted += item.GetOmittedEpisodes()
		truncated = truncated || item.GetTruncated()
		if len(kept) >= maxResponseProgressEpisodes {
			omitted++
			truncated = true
			continue
		}
		projected := proto.Clone(item).(*contracts.ResponseProgress)
		projected.OmittedEpisodes = 0
		projected.Truncated = false
		// Bound every nested string and inventory, including receipt owner chains
		// and webhook references. Retain the newest entries within an episode.
		clipped := clipProgressMessage(projected.ProtoReflect(), 256, 4)
		if progressWireBytes([]*contracts.ResponseProgress{projected}) > budget/2 {
			clipped = clipProgressMessage(projected.ProtoReflect(), 64, 1) || clipped
		}
		candidate := append([]*contracts.ResponseProgress{projected}, kept...)
		if progressWireBytes(candidate) > budget-128 {
			omitted++
			truncated = true
			continue
		}
		projected.Truncated = clipped
		truncated = truncated || clipped
		kept = candidate
	}
	if len(kept) == 0 && (truncated || omitted > 0) {
		kept = []*contracts.ResponseProgress{{}}
	}
	if len(kept) > 0 {
		kept[len(kept)-1].Truncated = truncated
		kept[len(kept)-1].OmittedEpisodes = omitted
	}
	return kept
}

func progressWireBytes(items []*contracts.ResponseProgress) int {
	data, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(&contracts.IncidentRequest{PriorResponseProgress: items})
	if err != nil {
		return maxResponseProgressBytes + 1
	}
	var document any
	if json.Unmarshal(data, &document) != nil {
		return maxResponseProgressBytes + 1
	}
	canonical, err := json.Marshal(document)
	if err != nil {
		return maxResponseProgressBytes + 1
	}
	return len(canonical)
}

func clipProgressMessage(message protoreflect.Message, textBytes, count int) bool {
	changed := len(message.GetUnknown()) > 0
	message.SetUnknown(nil)
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		clip := func(v protoreflect.Value) protoreflect.Value {
			if field.Kind() == protoreflect.StringKind && len(v.String()) > textBytes {
				text := v.String()[:textBytes]
				for !utf8.ValidString(text) {
					text = text[:len(text)-1]
				}
				changed = true
				return protoreflect.ValueOfString(text + " [truncated]")
			}
			if field.Kind() == protoreflect.MessageKind {
				changed = clipProgressMessage(v.Message(), textBytes, count) || changed
			}
			return v
		}
		if field.IsList() {
			list := value.List()
			if list.Len() > count {
				start := list.Len() - count
				for i := 0; i < count; i++ {
					list.Set(i, list.Get(start+i))
				}
				list.Truncate(count)
				changed = true
			}
			for i := 0; i < list.Len(); i++ {
				list.Set(i, clip(list.Get(i)))
			}
		} else {
			message.Set(field, clip(value))
		}
		return true
	})
	return changed
}
