package runtime

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
)

const (
	maxEarlierAttempts = 3
	// defaultEarlierAttemptAge keeps a stale attempt out of a new incident's task.
	defaultEarlierAttemptAge = 7 * 24 * time.Hour
	// defaultEarlierAttemptBytes bounds the block the agent reads in its first message.
	defaultEarlierAttemptBytes = 4500
)

// earlierAttempts are the earlier incidents' own notes for a new incident that opens on the same keys:
// failed, escalated, unconfirmed or playbook-less closes with findings, newest first, at most three, none
// older than maxAge, as large as byteCap allows (zero values use the defaults). The outcome class and the
// changes are Go's (classification and repair receipts); the findings are the agent's, copied as stored.
func earlierAttempts(outcomes []*contracts.OutcomeRecord, openingKeys []string, now time.Time, maxAge time.Duration, byteCap int) []*contracts.EarlierAttempt {
	if maxAge <= 0 {
		maxAge = defaultEarlierAttemptAge
	}
	if byteCap <= 0 {
		byteCap = defaultEarlierAttemptBytes
	}
	var matched []*contracts.OutcomeRecord
	for _, outcome := range outcomes {
		completed := outcome.GetTimestamps().GetCompletedAt()
		if len(outcome.GetAgentFindings()) == 0 || completed == nil || now.Sub(completed.AsTime()) > maxAge {
			continue
		}
		switch outcome.GetClassification() {
		case "failed", "escalated", "unconfirmed":
		case "success":
			if len(outcome.GetAppliedPlaybooks()) > 0 {
				continue
			}
		default:
			continue
		}
		if !slices.ContainsFunc(outcome.GetRecoveryVerification().GetOpeningKeys(), func(key string) bool { return slices.Contains(openingKeys, key) }) {
			continue
		}
		matched = append(matched, outcome)
	}
	sort.SliceStable(matched, func(i, j int) bool {
		return matched[i].GetTimestamps().GetCompletedAt().AsTime().After(matched[j].GetTimestamps().GetCompletedAt().AsTime())
	})
	var attempts []*contracts.EarlierAttempt
	remaining := byteCap
	for _, outcome := range matched {
		if len(attempts) == maxEarlierAttempts {
			break
		}
		attempt := &contracts.EarlierAttempt{
			IncidentId: outcome.GetIncidentId(), CompletedAt: timestamppb.New(outcome.GetTimestamps().GetCompletedAt().AsTime()),
			Outcome: outcome.GetClassification(), Changes: attemptChanges(outcome),
		}
		for _, finding := range outcome.GetAgentFindings() {
			attempt.Findings = append(attempt.Findings, proto.Clone(finding).(*contracts.IncidentFinding)) //assertcheck:ok proto.Clone of a T returns a T
		}
		for proto.Size(attempt) > remaining && len(attempt.Findings) > 1 && len(attempts) == 0 {
			attempt.Findings = attempt.Findings[:len(attempt.Findings)-1]
		}
		if proto.Size(attempt) > remaining {
			break
		}
		remaining -= proto.Size(attempt)
		attempts = append(attempts, attempt)
	}
	return attempts
}

// attemptChanges lists what the attempt did, from the repair receipts Go recorded, never from the agent's words.
func attemptChanges(outcome *contracts.OutcomeRecord) []string {
	var changes []string
	for _, action := range outcome.GetRepairActions() {
		if action.GetSuccess() {
			changes = append(changes, action.GetSummary())
			continue
		}
		changes = append(changes, fmt.Sprintf("%s (failed: %s)", action.GetSummary(), cmpOr(action.GetFailureReason(), "not verified")))
	}
	return changes
}

func cmpOr(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

// withEarlierAttempts returns the snapshot with the earlier attempts for this incident's opening keys.
// The incident task is the only caller; the stored outcomes are read once, as for program trust.
func (d KubernetesJobDispatcher) withEarlierAttempts(ctx context.Context, snapshot *contracts.OperationalMemorySnapshot, request IncidentRequest) (*contracts.OperationalMemorySnapshot, error) {
	if d.Outcomes == nil || request.ExternalMessage != "" || snapshot == nil {
		return snapshot, nil
	}
	stored, _, err := memory.ListOutcomes(ctx, d.Outcomes, "earlier attempts")
	if err != nil {
		return nil, fmt.Errorf("read earlier attempts: %w", err)
	}
	records := make([]*contracts.OutcomeRecord, 0, len(stored))
	for _, outcome := range stored {
		if outcome.Record != nil && outcome.Record.GetIncidentId() != request.IncidentID {
			records = append(records, outcome.Record)
		}
	}
	attempts := earlierAttempts(records, openingKeys(&request), request.DetectedAt, d.EarlierAttemptMaxAge, 0)
	if len(attempts) == 0 {
		return snapshot, nil
	}
	cloned := proto.Clone(snapshot).(*contracts.OperationalMemorySnapshot) //assertcheck:ok proto.Clone of a T returns a T
	cloned.EarlierAttempts = attempts
	return cloned, nil
}
