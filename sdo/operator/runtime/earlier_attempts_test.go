package runtime

import (
	"context"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

var attemptNow = time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)

func attemptOutcome(id, class string, age time.Duration, keys []string, findings ...*contracts.IncidentFinding) *contracts.OutcomeRecord {
	return &contracts.OutcomeRecord{
		IncidentId: id, Classification: class, AgentFindings: findings,
		Timestamps:           &contracts.OutcomeTimestamps{CompletedAt: timestamppb.New(attemptNow.Add(-age))},
		RecoveryVerification: &contracts.RecoveryVerification{OpeningKeys: keys},
	}
}

func seen(statement string) *contracts.IncidentFinding {
	return &contracts.IncidentFinding{Kind: "observation", Statement: statement, SeenIn: "kubectl get pods"}
}

func TestEarlierAttemptsMatchOnOpeningKeysOnly(t *testing.T) {
	outcomes := []*contracts.OutcomeRecord{
		attemptOutcome("same", "failed", time.Hour, []string{"a/b", "c/d"}, seen("one")),
		attemptOutcome("other", "failed", time.Hour, []string{"x/y"}, seen("two")),
		attemptOutcome("none", "failed", time.Hour, []string{"a/b"}),
	}
	got := earlierAttempts(outcomes, []string{"c/d"}, attemptNow, 0, 0)
	if len(got) != 1 || got[0].GetIncidentId() != "same" || got[0].GetOutcome() != "failed" {
		t.Fatalf("attempts = %v", got)
	}
}

func TestEarlierAttemptsNewestFirstCappedAndAged(t *testing.T) {
	keys := []string{"a/b"}
	var outcomes []*contracts.OutcomeRecord
	for i, age := range []time.Duration{5 * time.Hour, time.Hour, 3 * time.Hour, 2 * time.Hour, 200 * time.Hour} {
		outcomes = append(outcomes, attemptOutcome(string(rune('a'+i)), "escalated", age, keys, seen("f")))
	}
	got := earlierAttempts(outcomes, keys, attemptNow, 0, 0)
	var ids []string
	for _, a := range got {
		ids = append(ids, a.GetIncidentId())
	}
	if strings.Join(ids, "") != "bdc" {
		t.Fatalf("ids = %v, want newest three within the age cap (b d c)", ids)
	}
}

// A success that applied a playbook is already learned memory; a success without one, and every
// close that did not recover users, can carry notes.
func TestEarlierAttemptsSkipLearnedSuccess(t *testing.T) {
	learned := attemptOutcome("learned", "success", time.Hour, []string{"k"}, seen("f"))
	learned.AppliedPlaybooks = []string{"fix"}
	bare := attemptOutcome("bare", "success", time.Hour, []string{"k"}, seen("f"))
	cleared := attemptOutcome("cleared", "cleared_without_action", time.Hour, []string{"k"}, seen("f"))
	got := earlierAttempts([]*contracts.OutcomeRecord{learned, bare, cleared}, []string{"k"}, attemptNow, 0, 0)
	if len(got) != 1 || got[0].GetIncidentId() != "bare" {
		t.Fatalf("attempts = %v", got)
	}
}

func TestEarlierAttemptChangesComeFromReceipts(t *testing.T) {
	o := attemptOutcome("i", "failed", time.Hour, []string{"k"}, seen("f"))
	o.RepairActions = []*contracts.RepairActionReceipt{
		{Summary: "restart deployment web", Success: true},
		{Summary: "scale web to 0", Success: false, FailureReason: "execution_failed"},
	}
	got := earlierAttempts([]*contracts.OutcomeRecord{o}, []string{"k"}, attemptNow, 0, 0)
	want := []string{"restart deployment web", "scale web to 0 (failed: execution_failed)"}
	if strings.Join(got[0].GetChanges(), "|") != strings.Join(want, "|") {
		t.Fatalf("changes = %v", got[0].GetChanges())
	}
}

func TestEarlierAttemptsDropOldestToFitByteCap(t *testing.T) {
	big := &contracts.IncidentFinding{Kind: "observation", Statement: strings.Repeat("s", 300), SeenIn: strings.Repeat("w", 200)}
	findings := make([]*contracts.IncidentFinding, 12)
	for i := range findings {
		findings[i] = big
	}
	var outcomes []*contracts.OutcomeRecord
	for i := 0; i < 3; i++ {
		outcomes = append(outcomes, attemptOutcome(string(rune('a'+i)), "failed", time.Duration(i+1)*time.Hour, []string{"k"}, findings...))
	}
	got := earlierAttempts(outcomes, []string{"k"}, attemptNow, 0, 4500)
	if len(got) != 1 || got[0].GetIncidentId() != "a" {
		t.Fatalf("want only the newest attempt under the cap, got %d", len(got))
	}
}

func TestDispatcherFillsEarlierAttemptsOnlyForMatchingOpeningKeys(t *testing.T) {
	record := attemptOutcome("earlier", "failed", time.Hour, []string{"a/b"}, seen("f"))
	dispatcher := KubernetesJobDispatcher{Outcomes: pathsOutcomes{record: record}}
	base := &contracts.OperationalMemorySnapshot{AcceptedCommit: "c"}
	match, err := dispatcher.withEarlierAttempts(context.Background(), base, IncidentRequest{IncidentID: "now", DetectedAt: attemptNow, OpeningKeys: []string{"a/b"}})
	if err != nil || len(match.GetEarlierAttempts()) != 1 || len(base.GetEarlierAttempts()) != 0 {
		t.Fatalf("matching: %v err=%v (shared snapshot must stay unchanged)", match.GetEarlierAttempts(), err)
	}
	other, _ := dispatcher.withEarlierAttempts(context.Background(), base, IncidentRequest{IncidentID: "now", DetectedAt: attemptNow, OpeningKeys: []string{"x/y"}})
	external, _ := dispatcher.withEarlierAttempts(context.Background(), base, IncidentRequest{IncidentID: "now", DetectedAt: attemptNow, OpeningKeys: []string{"a/b"}, ExternalMessage: "m"})
	if len(other.GetEarlierAttempts()) != 0 || len(external.GetEarlierAttempts()) != 0 {
		t.Fatal("different keys and external alerts get no earlier attempts")
	}
}
