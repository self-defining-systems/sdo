package runtime

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

// resumeWorld is an open alert-opened incident whose agent session is working
// it. Alert-set members fail or clear by evaluations the test records, so
// every decision answerFinish makes is deterministic.
type resumeWorld struct {
	t   *testing.T
	c   *Controller
	now time.Time
}

const resumeOpening = "checkout"

func resumeFinding(test string) sdk.Finding {
	finding := sdk.Finding{
		DetectorID: "traffic-" + test, RuleID: "traffic." + test, Status: sdk.FindingActive, Severity: sdk.SeverityCritical,
		Summary: test + " failed: HTTP 500 from the order service", Evidence: "synthetic",
		PrimaryResource: sdk.ObjectRef{Kind: "Service", Namespace: "demo", Name: test},
	}
	finding.Fingerprint = FindingFingerprint(finding)
	return finding
}

func newResumeWorld(t *testing.T) *resumeWorld {
	t.Helper()
	now := time.Unix(10_000, 0).UTC()
	cfg := testControllerConfig()
	cfg.ClosureStabilityWindow = 10 * time.Second
	cfg.VerificationTimeout = 5 * time.Minute
	c, err := NewController(cfg, nil, staticProvider{}, &recordingDispatcher{requests: make(chan IncidentRequest, 8)}, now)
	if err != nil {
		t.Fatal(err)
	}
	w := &resumeWorld{t: t, c: c, now: now}
	c.now = func() time.Time { return w.now }
	opening := resumeFinding(resumeOpening)
	c.incidentOpen = true
	c.dispatchState = contracts.DispatchState_DISPATCH_STATE_RUNNING
	c.currentIncidentRequest = &IncidentRequest{IncidentID: "demo-1", Application: "demo", Findings: []sdk.Finding{opening},
		OpeningKeys: findingKeys([]sdk.Finding{opening}), ResponseDeadline: now.Add(30 * time.Minute)}
	c.incidentDetectedAt, c.incidentDispatchedAt = now.Add(-time.Minute), now.Add(-time.Minute)
	c.writesSettled = true
	w.evaluate(resumeOpening, true)
	return w
}

// evaluate records an evaluation of a test now: firing on it, or clear.
func (w *resumeWorld) evaluate(test string, failing bool) {
	w.c.mu.Lock()
	defer w.c.mu.Unlock()
	finding := resumeFinding(test)
	evaluation := DetectorEvaluation{DetectorID: finding.DetectorID, EvaluatedAt: w.now, Status: DetectorEvaluationClear}
	if failing {
		evaluation.Status, evaluation.Fingerprints = DetectorEvaluationFiring, []string{finding.Fingerprint}
	}
	w.c.history = append(w.c.history, evaluation)
}

// join adds a user test to the alert set, as the controller does while the
// incident is worked.
func (w *resumeWorld) join(test string) {
	w.t.Helper()
	finding := resumeFinding(test)
	w.c.mu.Lock()
	request := w.c.currentIncidentRequest
	request.JoinedKeys = append(request.JoinedKeys, findingKey(finding))
	request.Findings = append(request.Findings, finding)
	w.c.mu.Unlock()
	w.evaluate(test, true)
}

func (w *resumeWorld) finish(inTurn bool) FinishAnswer {
	return w.c.answerFinish("demo-1", FinishRequest{InTurn: inTurn, Status: "completed", Resumable: true})
}

// returned asks after the turn until SDO resumes or ends the session, moving
// the clock by each wait and re-evaluating the failing tests. It reports the
// answer and the time it took.
func (w *resumeWorld) returned(failing ...string) (FinishAnswer, time.Duration) {
	w.t.Helper()
	started := w.now
	for range 2000 {
		for _, test := range failing {
			w.evaluate(test, true)
		}
		answer := w.finish(false)
		if answer.Action != FinishWait {
			return answer, w.now.Sub(started)
		}
		w.now = w.now.Add(time.Duration(answer.WaitSeconds * float64(time.Second)))
	}
	w.t.Fatal("SDO never resumed or ended the returned session")
	return FinishAnswer{}, 0
}

// The fixed reflective ask, pinned byte for byte.
const goldenNotRecoveredAsk = `Not recovered. Consider why before acting. Common causes (not exhaustive):
- an earlier conclusion was wrong (fixed a symptom or the wrong component);
- another independent root cause, masked by the first or arrived later;
- the fix didn't take effect or didn't persist (rollout pending, reverted from source, config not reloaded, stale cache or derived data);
- the fix caused new harm;
- slow or transient recovery (warm-up, retries, flaky check).`

func requirePrinciplesOnly(t *testing.T, message string) {
	t.Helper()
	for _, command := range []string{"`", "sdo ", "kubectl"} {
		if strings.Contains(message, command) {
			t.Fatalf("the message names a command (%q): %s", command, message)
		}
	}
}

// The resume message and the in-turn reply both end with the fixed ask, after
// Go's facts: the verdict, each failing member with its newest sample, and
// what is new since the session was last told.
func TestTheResumeMessageAndTheInTurnReplyEndWithTheFixedAsk(t *testing.T) {
	if NotRecoveredAsk != goldenNotRecoveredAsk {
		t.Fatalf("the reflective ask changed:\n%s", NotRecoveredAsk)
	}
	w := newResumeWorld(t)
	inTurn := w.finish(true)
	if inTurn.Action != FinishDeny || !strings.HasSuffix(inTurn.Message, "\n\n"+goldenNotRecoveredAsk) {
		t.Fatalf("in-turn reply = %+v", inTurn)
	}
	w.join("write-post")
	resumed, _ := w.returned(resumeOpening, "write-post")
	if resumed.Action != FinishResume || !strings.HasSuffix(resumed.Message, "\n\n"+goldenNotRecoveredAsk) {
		t.Fatalf("resume = %+v", resumed)
	}
	facts := strings.TrimSuffix(resumed.Message, goldenNotRecoveredAsk)
	post, checkout := alertName(resumeFinding("write-post")), alertName(resumeFinding(resumeOpening))
	for _, want := range []string{"not recovered", post + " (joined alert): write-post failed: HTTP 500", checkout + " (opening alert)", "New since you were last told: " + post + "\n"} {
		if !strings.Contains(facts, want) {
			t.Fatalf("the facts miss %q:\n%s", want, facts)
		}
	}
	requirePrinciplesOnly(t, inTurn.Message)
	requirePrinciplesOnly(t, resumed.Message)
}

// A return before Recovered keeps the incident open, waits for SDO's last
// known write to settle and the stability window, and resumes the same
// session: the session is prompted twice and never sealed or closed.
func TestAReturnBeforeRecoveredResumesTheSameSession(t *testing.T) {
	w := newResumeWorld(t)
	w.join("write-post")
	w.evaluate(resumeOpening, false) // the session fixed the opening alert
	w.c.mu.Lock()
	w.c.writesSettled = false // its last write is still rolling out
	w.c.mu.Unlock()
	if answer := w.finish(false); answer.Action != FinishWait {
		t.Fatalf("resumed before the write settled: %+v", answer)
	}
	w.c.mu.Lock()
	w.c.writesSettled = true
	w.c.mu.Unlock()
	answer, waited := w.returned("write-post")
	if answer.Action != FinishResume || waited < w.c.config.ClosureStabilityWindow || waited > w.c.config.ClosureStabilityWindow+2*finishPoll {
		t.Fatalf("answer %+v after %s; want a resume one stability window after the return", answer, waited)
	}
	if strings.Contains(answer.Message, "checkout") {
		t.Fatalf("a cleared alert is listed as failing: %s", answer.Message)
	}
	if !w.c.IncidentOpen() || w.c.currentIncidentRequest.SessionGeneration != 0 {
		t.Fatal("the resume closed the incident or started another session")
	}
}

// A session that cannot resume (lost, context full) ends; the controller then
// starts a fresh session on the same incident with a Go-rendered summary.
func TestAnUnresumableSessionEndsAndAFreshOneStarts(t *testing.T) {
	w := newResumeWorld(t)
	answer := w.c.answerFinish("demo-1", FinishRequest{Status: "completed", Resumable: false, Reason: "resume failed: context full"})
	if answer.Action != FinishEnd {
		t.Fatalf("an unresumable session was not ended: %+v", answer)
	}
	w.c.mu.Lock()
	result := completedResult("demo-1")
	w.c.currentIncidentResult, w.c.responderDone, w.c.responderCompletedAt = &result, true, w.now
	w.c.restartSessionLocked(w.now)
	request := *cloneIncidentRequest(w.c.currentIncidentRequest)
	w.c.mu.Unlock()
	if request.SessionGeneration != 1 || request.PreparedAgentTask != "" || !strings.Contains(request.PriorSession, alertName(resumeFinding(resumeOpening))+" (opening alert)") {
		t.Fatalf("fresh session request = generation %d, summary %q", request.SessionGeneration, request.PriorSession)
	}
	requirePrinciplesOnly(t, request.PriorSession)
}

// Two resumed turns that clear nothing end the session (a fresh one follows);
// a turn that clears a member resets the count.
func TestTwoResumesWithoutProgressEndTheSession(t *testing.T) {
	w := newResumeWorld(t)
	w.join("write-post")
	if answer, _ := w.returned(resumeOpening, "write-post"); answer.Action != FinishResume {
		t.Fatalf("first return: %+v", answer)
	}
	// The resumed turn clears the opening alert: progress, the count resets.
	w.evaluate(resumeOpening, false)
	if answer, _ := w.returned("write-post"); answer.Action != FinishResume {
		t.Fatalf("after progress: %+v", answer)
	}
	if answer, _ := w.returned("write-post"); answer.Action != FinishResume {
		t.Fatalf("first turn without progress: %+v", answer)
	}
	if answer, _ := w.returned("write-post"); answer.Action != FinishEnd {
		t.Fatalf("second resumed turn without progress was resumed again: %+v", answer)
	}
}

// Starts after a turn without progress back off from two minutes, doubling up
// to thirty, and never stop while an alert fails; a newly failing member resets
// the backoff at once.
func TestStartsAfterNoProgressBackOffAndNeverStop(t *testing.T) {
	w := newResumeWorld(t)
	if answer, _ := w.returned(resumeOpening); answer.Action != FinishResume {
		t.Fatalf("first return: %+v", answer)
	}
	var delays []time.Duration
	for range 6 {
		// A fresh session each time the old one ends: the controller's
		// restart, then the new session's own turns.
		answer, waited := w.returned(resumeOpening)
		if answer.Action == FinishEnd {
			w.c.mu.Lock()
			w.c.restartSessionLocked(w.now)
			delays = append(delays, w.c.dispatchNextRetryAt.Sub(w.now))
			w.now = w.c.dispatchNextRetryAt
			w.c.session.prompt(factKeys(w.c.failingAlertFactsLocked(w.c.incidentDetectedAt, true)), false)
			w.c.mu.Unlock()
			continue
		}
		delays = append(delays, waited-w.c.config.ClosureStabilityWindow)
	}
	want := []time.Duration{2 * time.Minute, 4 * time.Minute, 8 * time.Minute, 16 * time.Minute, 30 * time.Minute, 30 * time.Minute}
	for i, delay := range delays {
		if delay < want[i]-2*finishPoll || delay > want[i]+2*finishPoll {
			t.Fatalf("start delays %v, want about %v", delays, want)
		}
	}
	// A new failing member gets prompt service: the backoff resets.
	fresh := newResumeWorld(t)
	if answer, _ := fresh.returned(resumeOpening); answer.Action != FinishResume {
		t.Fatalf("first return: %+v", answer)
	}
	if answer := fresh.finish(false); answer.Action != FinishWait { // the turn returned
		t.Fatalf("first ask after the return: %+v", answer)
	}
	fresh.now = fresh.now.Add(fresh.c.config.ClosureStabilityWindow)
	fresh.evaluate(resumeOpening, true)
	if answer := fresh.finish(false); answer.Action != FinishWait {
		t.Fatalf("resumed a turn without progress before the backoff: %+v", answer)
	}
	fresh.now = fresh.now.Add(time.Second)
	fresh.join("write-post")
	if answer := fresh.finish(false); answer.Action != FinishResume || !strings.Contains(answer.Message, "write-post") {
		t.Fatalf("a newly failing member did not reset the backoff: %+v", answer)
	}
}

// In-turn replies are bounded by the same no-progress rule: after a reply
// that changed nothing, the finish goes through and the after-turn path paces.
func TestInTurnRepliesStopAfterATurnWithoutProgress(t *testing.T) {
	w := newResumeWorld(t)
	if answer := w.finish(true); answer.Action != FinishDeny {
		t.Fatalf("first finish: %+v", answer)
	}
	w.now = w.now.Add(time.Minute)
	w.evaluate(resumeOpening, true)
	if answer := w.finish(true); answer.Action != FinishEnd {
		t.Fatalf("a second finish without progress was denied again: %+v", answer)
	}
	if answer := w.finish(true); answer.Action != FinishEnd {
		t.Fatalf("finishes after no progress must go through: %+v", answer)
	}
	// Nothing failing: the finish goes through at once.
	other := newResumeWorld(t)
	other.evaluate(resumeOpening, false)
	if answer := other.finish(true); answer.Action != FinishEnd {
		t.Fatalf("a finish with nothing failing was denied: %+v", answer)
	}
}

// A page is a bounded response: its finish is never denied or resumed.
func TestAPageFinishIsNeverDenied(t *testing.T) {
	w := newResumeWorld(t)
	w.c.mu.Lock()
	w.c.currentIncidentRequest.Page, w.c.currentIncidentRequest.OpeningKeys = "synthetic page", nil
	w.c.mu.Unlock()
	if answer := w.finish(true); answer.Action != FinishEnd {
		t.Fatalf("page finish = %+v", answer)
	}
}

// The finish is the session's lifecycle, not a tool call: the loop answers it
// without recording a call, and without a controller every finish ends.
func TestTheLoopAnswersAFinishWithoutRecordingACall(t *testing.T) {
	loop := &ResponderLoop{}
	response := loop.Handle(context.Background(), ToolRequest{Command: "finish", Finish: &FinishRequest{InTurn: true}})
	if response.Finish == nil || response.Finish.Action != FinishEnd || len(loop.calls) != 0 {
		t.Fatalf("finish without a controller = %+v, calls %d", response.Finish, len(loop.calls))
	}
	loop.Finish = func(request FinishRequest) FinishAnswer {
		return FinishAnswer{Action: FinishDeny, Message: "synthetic"}
	}
	response = loop.Handle(context.Background(), ToolRequest{Command: "finish", Finish: &FinishRequest{InTurn: true}})
	if response.Finish.Action != FinishDeny || len(loop.calls) != 0 {
		t.Fatalf("finish = %+v, calls %d", response.Finish, len(loop.calls))
	}
}

// A user test that starts failing while a returned session waits for its
// resume joins the alert set and is in the resume message.
func TestAJoinWhileTheResumeIsPendingIsInTheResumeMessage(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newLateWorld(t, true)
		w.c.mu.Lock()
		w.c.responderDone, w.c.responderCompletedAt = false, time.Time{}
		w.c.dispatchState = contracts.DispatchState_DISPATCH_STATE_RUNNING
		w.c.mu.Unlock()
		// The session returned: its worker asks SDO and is told to wait.
		if answer := w.c.answerFinish(w.incidentID(), FinishRequest{Status: "completed", Resumable: true}); answer.Action != FinishWait {
			t.Fatalf("first ask = %+v", answer)
		}
		w.late.failing.Store(true)
		var answer FinishAnswer
		for range 60 {
			w.step()
			answer = w.c.answerFinish(w.incidentID(), FinishRequest{Status: "completed", Resumable: true})
			if answer.Action != FinishWait {
				break
			}
		}
		w.c.mu.Lock()
		joined := slices.Contains(w.c.currentIncidentRequest.JoinedKeys, findingKey(w.late.finding()))
		w.c.mu.Unlock()
		if !joined || answer.Action != FinishResume || !strings.Contains(answer.Message, alertName(w.late.finding())+" (joined alert)") {
			t.Fatalf("joined=%v answer=%+v", joined, answer)
		}
	})
}

// A fresh session of the incident keeps the earlier session's applied
// evidence but none of its identity or finished task record, so its worker
// starts a new provider session instead of returning the old result.
func TestAFreshSessionRestoresEvidenceButNotTheEarlierTask(t *testing.T) {
	request := IncidentRequest{IncidentID: "demo-1", SourceCommit: "rev"}
	earlier := NewResponderLoop(request, nil, nil, nil, PlanExecutor{}, 3)
	var saved []byte
	earlier.saveToolState = func(_ context.Context, payload []byte) error { saved = payload; return nil }
	earlier.batches = 2
	earlier.menuSession = &contracts.MitigationSession{Version: 1, IncidentId: "demo-1", SessionId: earlier.sessionID, ProviderSessionId: "provider-1"}
	result, err := incidentResultContract(completedResult("demo-1"))
	if err != nil {
		t.Fatal(err)
	}
	if err := earlier.saveContinuation(context.Background(), responderContinuation{Attempts: 1, Result: result, Done: true}); err != nil {
		t.Fatal(err)
	}
	same := NewResponderLoop(request, nil, nil, nil, PlanExecutor{}, 3)
	if err := same.restoreToolSnapshot(saved); err != nil {
		t.Fatal(err)
	}
	if !same.continuation.Done || same.sessionID != earlier.sessionID || same.menuSession.GetProviderSessionId() != "provider-1" {
		t.Fatal("a rejoined session lost its own task record")
	}
	request.SessionGeneration = 1
	fresh := NewResponderLoop(request, nil, nil, nil, PlanExecutor{}, 3)
	if err := fresh.restoreToolSnapshot(saved); err != nil {
		t.Fatal(err)
	}
	if fresh.continuation.Done || fresh.continuation.Generation != 1 || fresh.menuSession != nil || fresh.sessionID == earlier.sessionID {
		t.Fatalf("a fresh session inherited the earlier task: %+v session %q", fresh.continuation, fresh.sessionID)
	}
	if fresh.batches != 2 {
		t.Fatalf("a fresh session lost the incident's mitigation rounds: %d", fresh.batches)
	}
}

// The no-progress count and the backoff survive a controller restart: the
// persisted account restores them.
func TestTheSessionAccountSurvivesARestart(t *testing.T) {
	saved := sessionProgress{baseline: []string{"a"}, resumed: true, noProgress: 1, delay: 4 * time.Minute, judged: true}
	got := sessionProgressState(saved.contract())
	if !slices.Equal(got.baseline, []string{"a"}) || !got.resumed || got.noProgress != 1 || got.delay != 4*time.Minute || got.judged {
		t.Fatalf("restored account = %+v", got)
	}
	if (sessionProgress{}).contract() != nil {
		t.Fatal("an empty account is persisted")
	}
}

// One judgement per turn: an in-turn finish that ends the turn and the ask
// after it count the turn once, so the session is still resumed.
func TestAnInTurnEndAndTheAskAfterTheTurnCountTheTurnOnce(t *testing.T) {
	w := newResumeWorld(t)
	if answer := w.finish(true); answer.Action != FinishDeny {
		t.Fatalf("first finish: %+v", answer)
	}
	w.now = w.now.Add(time.Minute)
	w.evaluate(resumeOpening, true)
	if answer := w.finish(true); answer.Action != FinishEnd {
		t.Fatalf("second finish without progress: %+v", answer)
	}
	if answer := w.finish(true); answer.Action != FinishEnd {
		t.Fatalf("a repeated finish: %+v", answer)
	}
	w.c.mu.Lock()
	count := w.c.session.noProgress
	w.c.mu.Unlock()
	if count != 1 {
		t.Fatalf("the turn was counted %d times", count)
	}
	if answer, _ := w.returned(resumeOpening); answer.Action != FinishResume {
		t.Fatalf("after the turn: %+v; want the paced resume", answer)
	}
}

// However many alerts fail, the resume message stays within its prompt budget.
func TestTheResumeMessageIsBounded(t *testing.T) {
	var facts []alertFact
	for i := range 40 {
		facts = append(facts, alertFact{key: fmt.Sprint(i), name: strings.Repeat("n", 50), role: "joined", sample: strings.Repeat("s", 200)})
	}
	if message := renderNotRecovered(facts, nil); len([]rune(message)) > 4600 || !strings.HasSuffix(message, NotRecoveredAsk) {
		t.Fatalf("message is %d code points", len([]rune(message)))
	}
}
