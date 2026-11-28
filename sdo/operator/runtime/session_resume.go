package runtime

import (
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

// NotRecoveredAsk is the one reflective ask SDO adds, after its facts, when a
// responder session tries to finish (or has returned) before Recovered held.
// The resume message and the in-turn finish reply both end with it, byte for
// byte; a golden test pins it. It names principles, never commands.
const NotRecoveredAsk = "Not recovered. Consider why before acting. Common causes (not exhaustive):\n" +
	"- an earlier conclusion was wrong (fixed a symptom or the wrong component);\n" +
	"- another independent root cause, masked by the first or arrived later;\n" +
	"- the fix didn't take effect or didn't persist (rollout pending, reverted from source, config not reloaded, stale cache or derived data);\n" +
	"- the fix caused new harm;\n" +
	"- slow or transient recovery (warm-up, retries, flaky check)."

// Pacing of the starts that follow a turn without progress: the delay doubles
// from the first step up to the cap, resets when the failing set changes, and
// never stops while alerts still fail.
const (
	sessionBackoffFirst = 2 * time.Minute
	sessionBackoffCap   = 30 * time.Minute
	// sessionNoProgressLimit is how many consecutive resumed turns may end
	// without progress before SDO ends the session and starts a fresh one.
	sessionNoProgressLimit = 2
	// finishPoll is how often a returned worker asks again while SDO waits
	// (settle, pacing or the stability window).
	finishPoll = 10 * time.Second
	// maxRenderedFacts bounds the alerts a message lists, so it stays within
	// its prompt budget however many alerts fail.
	maxRenderedFacts = 12
)

// Actions of a finish answer.
const (
	// FinishEnd lets the turn's result stand (in-turn) or ends the worker's
	// session (after the turn): Recovered, a page, or a fresh session is next.
	FinishEnd = "end"
	// FinishDeny answers an in-turn finish: keep working, with Message.
	FinishDeny = "deny"
	// FinishWait asks a returned worker to ask again after WaitSeconds.
	FinishWait = "wait"
	// FinishResume resumes the same provider session with Message.
	FinishResume = "resume"
)

// FinishRequest is a responder session telling SDO it is finishing: from
// inside its turn (InTurn, before its result is accepted) or after the turn
// ended. Resumable is false when the worker cannot resume the session (its
// context is full or the provider lost it).
type FinishRequest struct {
	InTurn    bool   `json:"in_turn,omitempty"`
	Status    string `json:"status,omitempty"`
	Resumable bool   `json:"resumable,omitempty"`
	Reason    string `json:"reason,omitempty"`
}

// FinishAnswer is SDO's decision on a FinishRequest.
type FinishAnswer struct {
	Action      string  `json:"action"`
	Message     string  `json:"message,omitempty"`
	WaitSeconds float64 `json:"wait_seconds,omitempty"`
}

// sessionProgress is Go's no-progress account of the open incident's agent
// sessions. Progress is mechanical: a turn made progress when a key of the
// failing alert set it was prompted on has cleared. Keys that started failing
// later are new work, neither progress nor failure; writes and the agent's own
// claims never count.
type sessionProgress struct {
	// baseline is the failing alert-set keys when the session was last prompted.
	baseline []string
	// resumed is whether that prompt was a resume or an in-turn reply (the
	// first prompt of a session does not count toward no progress).
	resumed bool
	// noProgress counts consecutive resumed turns that ended without progress.
	noProgress int
	// delay is the pacing delay before the next start after no progress.
	delay time.Duration
	// returnedAt is when the session's turn ended; zero while it works.
	returnedAt time.Time
	// judged is set once the returned turn was judged; judgedFailing and
	// startAt are that judgement's failing set and earliest next start.
	judged        bool
	judgedFailing []string
	startAt       time.Time
	// generation counts the fresh sessions SDO started on the incident.
	generation int
}

// judge records whether the turn that ends now made progress on failing.
func (p *sessionProgress) judge(failing []string) bool {
	progressed := slices.ContainsFunc(p.baseline, func(key string) bool { return !slices.Contains(failing, key) })
	switch {
	case progressed:
		p.noProgress, p.delay = 0, 0
	case p.resumed:
		p.noProgress++
		p.delay = nextSessionDelay(p.delay)
	case p.delay > 0:
		// A fresh session's first turn without progress keeps backing off.
		p.delay = nextSessionDelay(p.delay)
	}
	return progressed
}

// prompt records that the session was (re)prompted on failing.
func (p *sessionProgress) prompt(failing []string, resumed bool) {
	p.baseline, p.resumed = slices.Clone(failing), resumed
	p.returnedAt, p.judged, p.judgedFailing, p.startAt = time.Time{}, false, nil, time.Time{}
}

func nextSessionDelay(delay time.Duration) time.Duration {
	if delay <= 0 {
		return sessionBackoffFirst
	}
	return min(2*delay, sessionBackoffCap)
}

// alertFact is one failing member of the incident's alert set, as the resume
// message states it.
type alertFact struct {
	key, name, role, sample string
}

// failingAlertFactsLocked are the alert-set members failing, sorted by key:
// those whose detector's latest evaluation since since fires on them, and,
// with unknownFails, also those with no evaluation since then (unknown is not
// recovered). An evaluation after SDO's last known write is the honest test:
// a finding still active only for its clearing persistence is not failing.
// Callers hold c.mu.
func (c *Controller) failingAlertFactsLocked(since time.Time, unknownFails bool) []alertFact {
	request := c.currentIncidentRequest
	if request == nil {
		return nil
	}
	samples := map[string]sdk.Finding{}
	for _, finding := range request.Findings {
		samples[findingKey(finding)] = finding
	}
	for _, state := range c.tracker.Snapshot() {
		if state.Active {
			samples[findingKey(state.Finding)] = state.Finding
		}
	}
	keys := alertKeys(request)
	sort.Strings(keys)
	var facts []alertFact
	for _, key := range slices.Compact(keys) {
		detectorID, fingerprint, _ := strings.Cut(key, "\x00")
		states, _ := c.latestEvaluations([]string{detectorID}, since, false)
		switch {
		case len(states) > 0 && states[0].Status == DetectorEvaluationFiring && slices.Contains(states[0].Fingerprints, fingerprint):
		case len(states) > 0 && (states[0].Status == DetectorEvaluationClear || states[0].Status == DetectorEvaluationFiring):
			continue
		case !unknownFails:
			continue
		}
		finding := samples[key]
		role := "opening"
		if slices.Contains(request.JoinedKeys, key) {
			role = "joined"
		}
		facts = append(facts, alertFact{key: key, name: alertName(finding), role: role, sample: factText(cmpOr(strings.TrimSpace(finding.Summary), finding.Evidence), 200)})
	}
	return facts
}

// lastKnownWriteLocked is the later of at and SDO's last known write of the
// open incident. Callers hold c.mu.
func (c *Controller) lastKnownWriteLocked(at time.Time) time.Time {
	if c.earlyLastMutationAt.After(at) {
		return c.earlyLastMutationAt
	}
	return at
}

// factText is evidence text fit for a prompt: one redacted line with no
// backticks, so a fact never reads as a command.
func factText(text string, limit int) string {
	return redactDetectText(strings.ReplaceAll(text, "`", "'"), limit)
}

func factKeys(facts []alertFact) []string {
	keys := make([]string, 0, len(facts))
	for _, fact := range facts {
		keys = append(keys, fact.key)
	}
	return keys
}

// renderNotRecovered is the resume message and the in-turn finish reply:
// Go's facts first (the verdict, each failing alert-set member with its newest
// sample, what is new since the last prompt), then NotRecoveredAsk.
func renderNotRecovered(failing []alertFact, baseline []string) string {
	var b strings.Builder
	b.WriteString("SDO's verdict: not recovered. Alerts of this incident still failing:\n")
	var fresh []string
	for i, fact := range failing {
		if i == maxRenderedFacts {
			fmt.Fprintf(&b, "- and %d more\n", len(failing)-i)
			break
		}
		line := fmt.Sprintf("- %s (%s alert)", fact.name, fact.role)
		if fact.sample != "" {
			line += ": " + fact.sample
		}
		b.WriteString(line + "\n")
		if !slices.Contains(baseline, fact.key) {
			fresh = append(fresh, fact.name)
		}
	}
	if len(fresh) > maxRenderedFacts {
		fresh = append(fresh[:maxRenderedFacts], fmt.Sprintf("and %d more", len(fresh)-maxRenderedFacts))
	}
	if len(fresh) > 0 {
		b.WriteString("New since you were last told: " + strings.Join(fresh, ", ") + "\n")
	}
	b.WriteString("\n" + NotRecoveredAsk)
	return b.String()
}

// renderPriorSession is the summary a fresh session starts from: facts Go
// holds about the incident and the session before it, never its transcript.
func renderPriorSession(generation int, failing []alertFact, result *IncidentResult) string {
	var b strings.Builder
	fmt.Fprintf(&b, "An earlier agent session (%d before you) worked this incident in this same checkout and ended before Recovered held.\n", generation)
	if result != nil {
		status := cmpOr(string(result.Status), "unknown")
		fmt.Fprintf(&b, "Its last result: %s", status)
		if result.Error != "" {
			b.WriteString("; " + factText(result.Error, 200))
		}
		b.WriteString("\n")
		var writes []string
		for _, action := range result.RepairActions {
			if action.Summary != "" || action.Target != "" {
				writes = append(writes, factText(strings.TrimSpace(action.Target+" "+action.Summary), 120))
			}
		}
		if len(writes) > 6 {
			writes = append(writes[:6], fmt.Sprintf("and %d more", len(writes)-6))
		}
		if len(writes) > 0 {
			b.WriteString("Its known writes: " + strings.Join(writes, "; ") + "\n")
		}
	}
	if len(failing) > 0 {
		b.WriteString("Alerts still failing:\n")
		for i, fact := range failing {
			if i == maxRenderedFacts {
				fmt.Fprintf(&b, "- and %d more\n", len(failing)-i)
				break
			}
			b.WriteString(fmt.Sprintf("- %s (%s alert)\n", fact.name, fact.role))
		}
	}
	summary := []rune(strings.TrimRight(b.String(), "\n"))
	if len(summary) > 4000 {
		summary = append(summary[:3999], '…')
	}
	return string(summary)
}

// answerFinish decides a responder session's finish. Go owns the done
// verdict: a finish before Recovered is answered with the facts and the ask
// (in-turn) or, after the turn, the same session is resumed once SDO's last
// known write settled; two resumed turns without progress, or a session that
// cannot resume, end the session so a fresh one starts.
func (c *Controller) answerFinish(id string, request FinishRequest) FinishAnswer {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.clock()
	incident := c.currentIncidentRequest
	if incident == nil || incident.IncidentID != id || !c.incidentOpen || pageOpened(incident) || c.earlyReportPendingLocked() {
		return FinishAnswer{Action: FinishEnd}
	}
	p := &c.session
	if !request.InTurn && p.returnedAt.IsZero() {
		p.returnedAt, p.judged = now, false
	}
	// Failing means shown failing on an evaluation after the session's last
	// known write; after its turn, one stability window after it returned or
	// wrote, so a fix that takes effect within the window is not resumed.
	since := c.lastKnownWriteLocked(c.incidentDetectedAt)
	if !request.InTurn {
		since = c.lastKnownWriteLocked(p.returnedAt).Add(c.config.ClosureStabilityWindow)
	}
	// Past the grace an alert not shown clear (an erroring detector, say)
	// counts as failing, as it does for the controller.
	failing := c.failingAlertFactsLocked(since, !request.InTurn && !now.Before(p.returnedAt.Add(c.config.VerificationTimeout)))
	keys := factKeys(failing)
	if request.InTurn {
		if len(keys) == 0 {
			return FinishAnswer{Action: FinishEnd}
		}
		// One judgement per turn: a repeated in-turn finish, or the ask after
		// the turn ends, reuses it.
		if p.resumed && !p.judged {
			p.judge(keys)
			p.judged, p.judgedFailing, p.startAt = true, slices.Clone(keys), now.Add(p.delay)
		}
		if p.noProgress >= sessionNoProgressLimit || p.delay > 0 {
			// The turn ends; the after-turn answer paces or starts fresh.
			if p.returnedAt.IsZero() {
				p.returnedAt = now
			}
			return FinishAnswer{Action: FinishEnd}
		}
		message := renderNotRecovered(failing, p.baseline)
		p.prompt(keys, true)
		slog.Info("responder_finish_denied", "incident_id", id, "failing", len(keys))
		return FinishAnswer{Action: FinishDeny, Message: message}
	}
	if !request.Resumable {
		slog.Info("responder_session_unresumable", "incident_id", id, "reason", request.Reason)
		return FinishAnswer{Action: FinishEnd}
	}
	if len(keys) == 0 {
		// Nothing fails: Recovered closes the incident and stops the worker.
		return FinishAnswer{Action: FinishWait, WaitSeconds: finishPoll.Seconds()}
	}
	if !c.writesSettled && now.Before(p.returnedAt.Add(c.config.VerificationTimeout)) {
		return FinishAnswer{Action: FinishWait, WaitSeconds: finishPoll.Seconds()}
	}
	if !p.judged {
		p.judge(keys)
		p.judged, p.judgedFailing, p.startAt = true, slices.Clone(keys), now.Add(p.delay)
	}
	if p.noProgress >= sessionNoProgressLimit {
		slog.Info("responder_session_no_progress", "incident_id", id, "turns", p.noProgress)
		return FinishAnswer{Action: FinishEnd}
	}
	changed := !slices.Equal(keys, p.judgedFailing)
	if changed {
		p.delay = 0 // a new symptom or a cleared one gets prompt service
	} else if now.Before(p.startAt) {
		return FinishAnswer{Action: FinishWait, WaitSeconds: min(p.startAt.Sub(now), finishPoll).Seconds()}
	}
	message := renderNotRecovered(failing, p.baseline)
	p.prompt(keys, true)
	slog.Info("responder_session_resumed", "incident_id", id, "failing", len(keys))
	return FinishAnswer{Action: FinishResume, Message: message}
}

// restartSessionLocked starts a fresh agent session on the open incident and
// its checkout after its session ended before Recovered held (the worker
// ended it, the response deadline passed, or the Job was lost). The ended
// worker is gone, so one writer holds by sequencing. The start is paced: right
// away after progress, after the backoff otherwise. It is a first start of a
// new session with a Go-rendered summary, never the old transcript. Callers
// hold c.mu.
func (c *Controller) restartSessionLocked(now time.Time) {
	request := c.currentIncidentRequest
	failing := c.failingAlertFactsLocked(c.incidentDetectedAt, true)
	keys := factKeys(failing)
	p := &c.session
	if !p.judged && !p.judge(keys) && !p.resumed {
		// A session that ended without progress backs off like a resume.
		p.delay = nextSessionDelay(p.delay)
	}
	start := now.Add(p.delay)
	p.generation = max(p.generation, request.SessionGeneration) + 1
	p.prompt(keys, false)
	p.noProgress = 0
	request.SessionGeneration = p.generation
	request.PriorSession = renderPriorSession(p.generation, failing, c.currentIncidentResult)
	request.PreparedAgentTask = ""
	request.ResponseDeadline = start.Add(c.config.ResponseTimeout).UTC()
	if report := deterministicStageReport(request.PriorStages); report != nil {
		report.HandoffSelected = true
	}
	c.responderDone = false
	c.responderCompletedAt = time.Time{}
	c.dispatchState = contracts.DispatchState_DISPATCH_STATE_PENDING
	c.dispatchError = ""
	c.resetDispatchRetryLocked()
	if start.After(now) {
		c.dispatchNextRetryAt = start
	}
	slog.Info("responder_session_restarted", "incident_id", request.IncidentID, "generation", p.generation, "failing", len(keys), "start_at", start)
}

// contract is the persisted part of the account: what the no-progress limit
// and the backoff need across a controller restart. A turn in flight is judged
// again after a restart, from the persisted baseline.
func (p sessionProgress) contract() *contracts.ResponderSessionProgress {
	if len(p.baseline) == 0 && !p.resumed && p.noProgress == 0 && p.delay == 0 {
		return nil
	}
	return &contracts.ResponderSessionProgress{Baseline: slices.Clone(p.baseline), Resumed: p.resumed, NoProgress: int32(p.noProgress), Delay: durationpb.New(p.delay)}
}

func sessionProgressState(message *contracts.ResponderSessionProgress) sessionProgress {
	return sessionProgress{baseline: slices.Clone(message.GetBaseline()), resumed: message.GetResumed(),
		noProgress: int(message.GetNoProgress()), delay: message.GetDelay().AsDuration()}
}
