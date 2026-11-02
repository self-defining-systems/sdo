package runtime

import (
	"context"
	"log/slog"
	outcomeclass "sdo.dev/operator/contracts/outcome"
	"sort"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/sdk"
)

const maxRelevantOutcomes = 3

// relevantOutcomeEvidence returns up to maxRelevantOutcomes prior successes
// related to findings, newest first. origins maps a learned incident
// detector to the incident it was learned from.
func relevantOutcomeEvidence(
	repository string, findings []sdk.Finding, sourceCommit string, origins map[string]string,
) []PriorOutcomeEvidence {
	stored, _, err := memory.ListOutcomes(context.Background(), memory.OpenReader(repository), "relevant outcome recall")
	if err != nil {
		slog.Warn("relevant outcome recall unavailable", "repository", repository, "error", err)
		return nil
	}

	records := make([]*contracts.OutcomeRecord, 0)
	for _, outcome := range stored {
		if outcome.Record.GetClassification() == outcomeclass.Success {
			records = append(records, outcome.Record)
		}
	}
	sort.SliceStable(records, func(i, j int) bool {
		left, right := outcomeCompletedAt(records[i]), outcomeCompletedAt(records[j])
		if left.Equal(right) {
			return records[i].GetIncidentId() < records[j].GetIncidentId()
		}
		return left.Before(right)
	})

	evidence := make([]PriorOutcomeEvidence, 0, maxRelevantOutcomes)
	for index := len(records) - 1; index >= 0 && len(evidence) < maxRelevantOutcomes; index-- {
		record := records[index]
		match := outcomeMatch(record, findings, origins)
		if match == "" {
			continue
		}
		playbookIDs, err := layout.PlaybookIDs(record.GetAppliedPlaybooks())
		if err != nil {
			continue
		}
		rootCauses := make([]string, 0, len(record.GetConfirmedRootCauses()))
		for _, cause := range record.GetConfirmedRootCauses() {
			rootCauses = append(rootCauses, cause.GetSummary())
		}
		actions := make([]string, 0, len(record.GetRepairActions()))
		for _, action := range record.GetRepairActions() {
			if action.GetSuccess() {
				actions = append(actions, action.GetSummary())
			}
		}
		evidence = append(evidence, PriorOutcomeEvidence{
			IncidentID: record.GetIncidentId(), MatchReason: match, RootCauseSummaries: rootCauses,
			RepairActionSummaries: actions, AppliedPlaybookIDs: playbookIDs,
			SourceCommit: record.GetSourceCommit(), ExactSourceMatch: record.GetSourceCommit() == sourceCommit,
		})
	}
	return evidence
}

func outcomeCompletedAt(record *contracts.OutcomeRecord) time.Time {
	if record.GetTimestamps() == nil || record.GetTimestamps().GetCompletedAt() == nil {
		return time.Time{}
	}
	return record.GetTimestamps().GetCompletedAt().AsTime()
}

func outcomeMatch(record *contracts.OutcomeRecord, current []sdk.Finding, origins map[string]string) string {
	previous := record.GetFindings()
	for _, left := range previous {
		for _, right := range current {
			if left.GetDetectorId() == right.DetectorID && left.GetFingerprint() != "" && left.GetFingerprint() == right.Fingerprint {
				return "exact-fingerprint"
			}
		}
	}
	// A detector learned from this outcome fires on its first match, often
	// before the health detectors that opened the original incident, so the
	// repeat shares no fingerprint with it. Same learned detector, same
	// resource: the same fault again.
	for _, right := range current {
		if origin := origins[right.DetectorID]; origin == "" || origin != record.GetIncidentId() {
			continue
		}
		for _, left := range previous {
			if sameObject(left.GetPrimaryResource(), right.PrimaryResource) {
				return "learned-detector-origin"
			}
		}
	}
	for _, left := range previous {
		for _, right := range current {
			if left.GetDetectorId() == right.DetectorID && left.GetRuleId() == right.RuleID &&
				left.GetPrimaryResource().GetKind() == right.PrimaryResource.Kind {
				return "detector-rule-resource-kind"
			}
		}
	}
	return ""
}

func sameObject(left *contracts.ObjectRef, right sdk.ObjectRef) bool {
	return left.GetKind() != "" && left.GetName() != "" && left.GetKind() == right.Kind &&
		left.GetNamespace() == right.Namespace && left.GetName() == right.Name
}
