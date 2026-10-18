package runtime

import (
	"context"
	"encoding/json"
	"sort"
	"time"

	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
)

const maxRelevantOutcomes = 3

type outcomeMemoryRecord struct {
	IncidentID          string                `json:"incident_id"`
	SourceCommit        string                `json:"source_commit"`
	Classification      string                `json:"classification"`
	Findings            []sdk.Finding         `json:"findings"`
	ConfirmedRootCauses []ConfirmedRootCause  `json:"confirmed_root_causes"`
	AppliedPlaybooks    []string              `json:"applied_playbooks"`
	RepairActions       []RepairActionReceipt `json:"repair_actions"`
	// DiagnosisVerification is SDO's deterministic verdict per root cause,
	// aligned by index with ConfirmedRootCauses. Absent in older records.
	DiagnosisVerification []struct {
		Verdict string `json:"verdict"`
	} `json:"diagnosis_verification"`
	Timestamps struct {
		CompletedAt time.Time `json:"completed_at"`
	} `json:"timestamps"`
}

// discreditedVerdicts mark a root cause SDO found false (contradicted) or not
// backed by the responder's own repair (unattributed). Such a cause is
// never surfaced to a later responder as a known root cause.
var discreditedVerdicts = map[string]struct{}{"contradicted": {}, "unattributed": {}}

// relevantOutcomeEvidence returns up to maxRelevantOutcomes prior successes
// related to findings, newest first. origins maps a learned incident
// detector to the incident it was learned from.
func relevantOutcomeEvidence(
	repository string, findings []sdk.Finding, sourceCommit string, origins map[string]string,
) []PriorOutcomeEvidence {
	stored, err := memory.OpenReader(repository).List(context.Background())
	if err != nil {
		return nil
	}

	records := make([]outcomeMemoryRecord, 0)
	for _, outcome := range stored {
		var record outcomeMemoryRecord
		if json.Unmarshal(outcome.Document, &record) == nil && record.Classification == "success" {
			records = append(records, record)
		}
	}
	sort.SliceStable(records, func(i, j int) bool {
		if records[i].Timestamps.CompletedAt.Equal(records[j].Timestamps.CompletedAt) {
			return records[i].IncidentID < records[j].IncidentID
		}
		return records[i].Timestamps.CompletedAt.Before(records[j].Timestamps.CompletedAt)
	})

	evidence := make([]PriorOutcomeEvidence, 0, maxRelevantOutcomes)
	for index := len(records) - 1; index >= 0 && len(evidence) < maxRelevantOutcomes; index-- {
		record := records[index]
		match := outcomeMatch(record, findings, origins)
		if match == "" {
			continue
		}
		playbookIDs, err := memory.PlaybookIDs(record.AppliedPlaybooks)
		if err != nil {
			continue
		}
		rootCauses := make([]string, 0, len(record.ConfirmedRootCauses))
		aligned := len(record.DiagnosisVerification) == len(record.ConfirmedRootCauses)
		for index, cause := range record.ConfirmedRootCauses {
			if aligned {
				if _, discredited := discreditedVerdicts[record.DiagnosisVerification[index].Verdict]; discredited {
					continue
				}
			}
			rootCauses = append(rootCauses, cause.Summary)
		}
		actions := make([]string, 0, len(record.RepairActions))
		for _, action := range record.RepairActions {
			if action.Success {
				actions = append(actions, action.Summary)
			}
		}
		evidence = append(evidence, PriorOutcomeEvidence{
			IncidentID: record.IncidentID, MatchReason: match, RootCauseSummaries: rootCauses,
			RepairActionSummaries: actions, AppliedPlaybookIDs: playbookIDs,
			SourceCommit: record.SourceCommit, ExactSourceMatch: record.SourceCommit == sourceCommit,
		})
	}
	return evidence
}

func outcomeMatch(record outcomeMemoryRecord, current []sdk.Finding, origins map[string]string) string {
	previous := record.Findings
	for _, left := range previous {
		for _, right := range current {
			if left.DetectorID == right.DetectorID && left.Fingerprint != "" && left.Fingerprint == right.Fingerprint {
				return "exact-fingerprint"
			}
		}
	}
	// A detector learned from this outcome fires on its first match, often
	// before the health detectors that opened the original incident, so the
	// repeat shares no fingerprint with it. Same learned detector, same
	// resource: the same fault again.
	for _, right := range current {
		if origin := origins[right.DetectorID]; origin == "" || origin != record.IncidentID {
			continue
		}
		for _, left := range previous {
			if sameObject(left.PrimaryResource, right.PrimaryResource) {
				return "learned-detector-origin"
			}
		}
	}
	for _, left := range previous {
		for _, right := range current {
			if left.DetectorID == right.DetectorID && left.RuleID == right.RuleID &&
				left.PrimaryResource.Kind == right.PrimaryResource.Kind {
				return "detector-rule-resource-kind"
			}
		}
	}
	return ""
}

func sameObject(left sdk.ObjectRef, right sdk.ObjectRef) bool {
	return left.Kind != "" && left.Name != "" && left.Kind == right.Kind &&
		left.Namespace == right.Namespace && left.Name == right.Name
}
