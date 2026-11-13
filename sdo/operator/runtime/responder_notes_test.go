package runtime

import (
	"context"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
)

type noteClock struct{ now time.Time }

func (c *noteClock) Now() time.Time { return c.now }

func noteFinding(fingerprint string) sdk.Finding {
	return sdk.Finding{Fingerprint: fingerprint, Status: sdk.FindingActive}
}

// noteLoop returns a loop whose verify reports the findings in *current and whose recorded writes are *writes.
func noteLoop(current *[]sdk.Finding, writes *[]RepairActionReceipt) (*ResponderLoop, *noteClock) {
	clock := &noteClock{now: time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC)}
	loop := NewResponderLoop(IncidentRequest{IncidentID: "incident", Findings: []sdk.Finding{noteFinding("a")}}, nil, nil, nil, PlanExecutor{}, 3)
	loop.Now = clock.Now
	loop.VerifyClosure = func(context.Context) ToolResponse {
		return ToolResponse{Command: "verify", Summary: "not recovered", Findings: append([]sdk.Finding(nil), *current...)}
	}
	if writes != nil {
		loop.ExternalReceipts = func() ([]RepairActionReceipt, bool, string, error) { return *writes, false, "r", nil }
	}
	return loop, clock
}

func verifyOnce(loop *ResponderLoop, clock *noteClock, advance time.Duration) string {
	clock.now = clock.now.Add(advance)
	return loop.Handle(context.Background(), ToolRequest{Command: "verify"}).Summary
}

func TestVerifyNotesUnchangedAlertsSinceLastWrite(t *testing.T) {
	current := []sdk.Finding{noteFinding("a")}
	var writes []RepairActionReceipt
	loop, clock := noteLoop(&current, &writes)
	verifyOnce(loop, clock, time.Minute)
	writes = append(writes, RepairActionReceipt{CompletedAt: clock.now.Add(time.Minute)})
	got := verifyOnce(loop, clock, 2*time.Minute)
	want := "opening alerts unchanged since your last write at 12:02:00Z (1 write since the alerts last changed)"
	if !strings.Contains(got, want) || !strings.HasPrefix(got, "not recovered") {
		t.Fatalf("summary = %q, want it to contain %q", got, want)
	}
	writes = append(writes, RepairActionReceipt{CompletedAt: clock.now.Add(time.Minute)})
	got = verifyOnce(loop, clock, 2*time.Minute)
	if !strings.Contains(got, "last write at 12:04:00Z (2 writes since the alerts last changed)") {
		t.Fatalf("summary = %q", got)
	}
}

func TestVerifyNoteIsAbsentWhenTheAlertsChanged(t *testing.T) {
	current := []sdk.Finding{noteFinding("a")}
	writes := []RepairActionReceipt{}
	loop, clock := noteLoop(&current, &writes)
	verifyOnce(loop, clock, time.Minute)
	writes = append(writes, RepairActionReceipt{CompletedAt: clock.now.Add(time.Second)})
	current = []sdk.Finding{noteFinding("b")}
	if got := verifyOnce(loop, clock, time.Minute); strings.Contains(got, "unchanged") {
		t.Fatalf("alerts changed but summary = %q", got)
	}
}

func TestVerifyNotesNoWriteAndVerifyCalls(t *testing.T) {
	current := []sdk.Finding{noteFinding("a")}
	writes := []RepairActionReceipt{}
	loop, clock := noteLoop(&current, &writes)
	if got := verifyOnce(loop, clock, time.Minute); strings.Contains(got, "no write") {
		t.Fatalf("the first verify has nothing to contradict: %q", got)
	}
	verifyOnce(loop, clock, time.Minute)
	got := verifyOnce(loop, clock, time.Minute)
	if !strings.Contains(got, "no write since 12:01:00Z; 2 verify calls since") {
		t.Fatalf("summary = %q", got)
	}
}

func TestVerifyOmitsTheWriteClauseWhenNoWriteRecordingExists(t *testing.T) {
	current := []sdk.Finding{noteFinding("a")}
	loop, clock := noteLoop(&current, nil)
	verifyOnce(loop, clock, time.Minute)
	got := verifyOnce(loop, clock, time.Minute)
	if strings.Contains(got, "write") || !strings.Contains(got, "opening alerts unchanged since your last verify at 12:01:00Z") {
		t.Fatalf("summary = %q", got)
	}
}

func TestVerifyUsesRecordedApplicationsWhenTheRecordingIsOff(t *testing.T) {
	current := []sdk.Finding{noteFinding("a")}
	loop, clock := noteLoop(&current, nil)
	verifyOnce(loop, clock, time.Minute)
	loop.applicationsLog = append(loop.applicationsLog, RecordedPlanApplication{ID: "app", CompletedAt: clock.now.Add(time.Second)})
	got := verifyOnce(loop, clock, time.Minute)
	if !strings.Contains(got, "opening alerts unchanged since your last write at 12:01:01Z") {
		t.Fatalf("summary = %q", got)
	}
}

func TestDetectNotesUnchangedFindingsSinceLastCall(t *testing.T) {
	loop, _ := onDemandLoop(t, detectApp("create"), &staticAppState{})
	first := loop.Handle(context.Background(), ToolRequest{Command: "detect"})
	if strings.Contains(first.Text, "unchanged") {
		t.Fatalf("first detect has no earlier call:\n%s", first.Text)
	}
	second := loop.Handle(context.Background(), ToolRequest{Command: "detect"})
	if !strings.Contains(second.Text, "findings unchanged since your last call at ") || !strings.Contains(second.Summary, "findings unchanged since your last call") {
		t.Fatalf("text:\n%s\nsummary: %s", second.Text, second.Summary)
	}
}

// The wiring hands the loop a nil source with the recording off; a non-nil one returning no rows would read as
// "no write happened" and make verify claim a contradiction that is not known.
func TestRecordingOffLeavesExternalReceiptsNilAndVerifyNotesUseApplicationsOnly(t *testing.T) {
	clock := func() time.Time { return time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC) }
	if source := externalReceiptsSource(false, t.TempDir(), "incident", clock); source != nil {
		t.Fatal("recording off must leave ExternalReceipts nil")
	}
	if source := externalReceiptsSource(true, t.TempDir(), "incident", clock); source == nil {
		t.Fatal("recording on must read the recording")
	}
	current := []sdk.Finding{noteFinding("a")}
	loop, c := noteLoop(&current, nil)
	loop.ExternalReceipts = externalReceiptsSource(false, t.TempDir(), "incident", clock)
	verifyOnce(loop, c, time.Minute)
	got := verifyOnce(loop, c, time.Minute)
	if strings.Contains(got, "no write since") || strings.Contains(got, "last write") || !strings.Contains(got, "unchanged since your last verify") {
		t.Fatalf("summary = %q, want the unchanged-since-last-verify form with no write claim", got)
	}
	loop.applicationsLog = append(loop.applicationsLog, RecordedPlanApplication{ID: "app", CompletedAt: c.now.Add(time.Second)})
	if got := verifyOnce(loop, c, time.Minute); !strings.Contains(got, "since your last write at") {
		t.Fatalf("summary = %q, want the plan application as the write signal", got)
	}
}
