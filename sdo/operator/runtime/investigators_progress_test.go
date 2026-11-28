package runtime

import (
	"context"
	"testing"
	"time"

	"sdo.dev/operator/apihistory"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
)

// The investigator gate sees the responder's progress without the agent recording: an alert of the set that
// cleared (or whose fingerprint stopped firing) is progress, the same alert still firing is not.
func TestAnAlertOfTheSetThatClearedIsProgress(t *testing.T) {
	c := openersController(t, &recordingDispatcher{})
	opener := failingOperation("requestmetrics/errors/browse-items")
	opener.DetectorID = memory.RequestMetricsDetectorID
	detected := time.Unix(100, 0).UTC()
	request := IncidentRequest{IncidentID: "i", DetectedAt: detected, Findings: []sdk.Finding{opener}, OpeningKeys: findingKeys([]sdk.Finding{opener})}
	c.incidentOpen, c.currentIncidentRequest = true, &request
	firing := DetectorEvaluation{DetectorID: memory.RequestMetricsDetectorID, EvaluatedAt: detected.Add(time.Second), Status: DetectorEvaluationFiring, Fingerprints: []string{FindingFingerprint(opener)}}

	c.history = []DetectorEvaluation{firing}
	if c.alertSetProgressed("i") {
		t.Fatal("the opening alert still fires: no progress")
	}
	if c.alertSetProgressed("other") {
		t.Fatal("another incident's id is never progress")
	}
	other := firing
	other.EvaluatedAt, other.Fingerprints = detected.Add(2*time.Second), []string{"requestmetrics/errors/checkout"}
	c.history = append(c.history, other)
	if !c.alertSetProgressed("i") {
		t.Fatal("the opening fingerprint stopped firing: progress")
	}
	c.history = []DetectorEvaluation{firing, {DetectorID: memory.RequestMetricsDetectorID, EvaluatedAt: detected.Add(3 * time.Second), Status: DetectorEvaluationClear}}
	if !c.alertSetProgressed("i") {
		t.Fatal("the opening alert's detector is clear: progress")
	}
	c.history = []DetectorEvaluation{{DetectorID: memory.RequestMetricsDetectorID, EvaluatedAt: detected.Add(-time.Second), Status: DetectorEvaluationClear}}
	if c.alertSetProgressed("i") {
		t.Fatal("a clear evaluation from before detection is not progress")
	}
}

// Investigators start only for a stuck responder: a known write, a cleared alert or a verified recovery each
// mean progress Go can see, so none starts (the hotel and social rows started them after the fix, even after
// verification, because only Go-known writes counted).
func TestTheInvestigatorGateCountsClearedAlertsAndVerification(t *testing.T) {
	loop := &ResponderLoop{}
	if loop.responderProgressed() {
		t.Fatal("no write, no alert cleared, not verified: stuck")
	}
	loop.AlertProgress = func() bool { return true }
	if !loop.responderProgressed() {
		t.Fatal("a cleared alert is progress")
	}
	loop.AlertProgress = func() bool { return false }
	loop.IncidentStatus = func() *apihistory.IncidentVerified { return &apihistory.IncidentVerified{VerifiedAt: time.Unix(1, 0)} }
	if !loop.responderProgressed() {
		t.Fatal("a verified recovery is progress")
	}
	loop.IncidentStatus = func() *apihistory.IncidentVerified { return nil }
	loop.applicationsLog = []RecordedPlanApplication{{StartedAt: time.Unix(2, 0)}}
	if !loop.responderProgressed() {
		t.Fatal("a known write is progress")
	}
}

func TestProgressAfterTheDelayStartsNoInvestigator(t *testing.T) {
	record := &launchRecord{}
	board := &InvestigationBoard{}
	investigatorPlan{
		IncidentID: "inc-1", Count: 2, Board: board, Wait: immediate,
		Progressed: func() bool { return true },
		Briefs: func(context.Context, int) []investigatorBrief {
			t.Fatal("no brief is rendered after progress")
			return nil
		},
		Launch: record.launch,
	}.run(context.Background())
	if len(record.ids) != 0 || board.DetectLine() != "" {
		t.Fatalf("started %v", record.ids)
	}
}

// The delay counts from the responder's first tool call, not from the session start: an agent dispatched early
// (while the stage's apply settles) that has not started working starts no investigator.
func TestTheDelayStartsAtTheResponderFirstToolCall(t *testing.T) {
	record := &launchRecord{}
	called := make(chan struct{})
	waits := 0
	done := make(chan struct{})
	go func() {
		defer close(done)
		investigatorPlan{
			IncidentID: "inc-1", Count: 1, Board: &InvestigationBoard{}, FirstToolCall: called,
			Wait: func(context.Context, time.Duration) error {
				select {
				case <-called:
				default:
					t.Error("the delay started before the responder's first tool call")
				}
				waits++
				return nil
			},
			Progressed: func() bool { return false },
			Briefs:     func(_ context.Context, max int) []investigatorBrief { return briefFixture(t)[:max] },
			Launch:     record.launch,
		}.run(context.Background())
	}()
	close(called)
	<-done
	if waits != 1 || len(record.ids) != 1 {
		t.Fatalf("after the first call the delay runs and one starts: waits %d, started %v", waits, record.ids)
	}
}

func TestNoToolCallBeforeTheEndStartsNoInvestigator(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	record := &launchRecord{}
	investigatorPlan{
		IncidentID: "inc-1", Count: 1, Board: &InvestigationBoard{}, FirstToolCall: make(chan struct{}),
		Wait: immediate, Progressed: func() bool { return false },
		Briefs: func(_ context.Context, max int) []investigatorBrief { return briefFixture(t)[:max] },
		Launch: record.launch,
	}.run(ctx)
	if len(record.ids) != 0 {
		t.Fatalf("started %v", record.ids)
	}
}

// Verification that lands while the briefs are rendered still stops the start.
func TestVerificationDuringBriefRenderingStartsNoInvestigator(t *testing.T) {
	record := &launchRecord{}
	verified := false
	investigatorPlan{
		IncidentID: "inc-1", Count: 1, Board: &InvestigationBoard{}, Wait: immediate,
		Progressed: func() bool { return verified },
		Briefs: func(_ context.Context, max int) []investigatorBrief {
			verified = true
			return briefFixture(t)[:max]
		},
		Launch: record.launch,
	}.run(context.Background())
	if len(record.ids) != 0 {
		t.Fatalf("started %v after verification", record.ids)
	}
}

// The responder's own commands mark its first tool call; an investigator's read on the loop does not.
func TestOnlyTheResponderToolCallMarksTheFirstCall(t *testing.T) {
	loop, _ := detectWorldLoop(&detectWorld{sendOK: true})
	loop.HandleInvestigatorRead(context.Background(), ToolRequest{Command: "detect", Name: "list"})
	select {
	case <-loop.firstToolCall():
		t.Fatal("an investigator read marked the responder's first call")
	default:
	}
	loop.Handle(context.Background(), ToolRequest{Command: "detect", Name: "list"})
	select {
	case <-loop.firstToolCall():
	default:
		t.Fatal("the responder's detect did not mark its first call")
	}
}
