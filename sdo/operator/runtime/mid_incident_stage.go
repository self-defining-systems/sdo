package runtime

import (
	"context"
	"fmt"
	"log/slog"

	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

// maxMidIncidentPasses bounds the automatic stage's re-runs over one incident.
// Each pass has the stage budget and never repeats an attempt that wrote, so
// the bound only limits how often a flapping detector can ask.
const maxMidIncidentPasses = 8

// midIncidentStage coalesces the re-runs of the automatic stage over the
// open incident: one pass at a time, and one more when a match arrived during it.
// A pass that clears no finding and applies no program new to the incident
// settles the stage: the responder owns what remains, and only a finding a
// program that never applied here could explain starts another pass.
type midIncidentStage struct {
	running bool
	dirty   bool
	settled bool
	passes  int
}

// StageNote tells the responder what a later automatic pass did, in the
// vocabulary of prior_stages. Each note is delivered once, in the next tool reply.
type StageNote struct {
	Stage           string `json:"stage"`
	PlaybookID      string `json:"playbook_id"`
	Result          string `json:"result"`
	SkipReason      string `json:"skip_reason,omitempty"`
	Error           string `json:"error,omitempty"`
	ClearedFindings int    `json:"cleared_findings,omitempty"`
}

func (n StageNote) String() string {
	text := fmt.Sprintf("%s applied %s: %s", n.Stage, n.PlaybookID, n.Result)
	if n.SkipReason != "" {
		text += " (" + n.SkipReason + ")"
	}
	return text
}

// attachLearnedActivation handles a finding that activates after the responder
// was dispatched. When a learned program can bind it, the finding joins the
// open incident and the automatic stage runs again, so the learned repair is
// applied when the finding appears rather than when the responder next looks.
// It reports whether the finding was taken.
func (c *Controller) attachLearnedActivation(ctx context.Context, finding sdk.Finding) bool {
	c.mu.Lock()
	loop := c.responderLoop
	if !c.incidentOpen || c.currentIncidentRequest == nil || c.incidentDispatchedAt.IsZero() ||
		c.dispatchState != contracts.DispatchState_DISPATCH_STATE_RUNNING || c.responseStagesRunning || c.responderDone ||
		loop == nil || c.midStage.passes >= maxMidIncidentPasses || !c.deterministicStageSelectedLocked() ||
		!learnedPlaybookExplains(loop, finding, nil) {
		c.mu.Unlock()
		return false
	}
	request := c.currentIncidentRequest
	finding.Fingerprint = FindingFingerprint(finding)
	known := false
	for _, existing := range request.Findings {
		known = known || FindingFingerprint(existing) == finding.Fingerprint
	}
	if !known {
		request.Findings = append(request.Findings, finding)
		sortFindings(request.Findings)
		c.incidentFindingKeys = findingKeys(request.Findings)
		c.noteBatchedLocked(request.IncidentID, []sdk.Finding{finding})
	}
	// A settled stage hands the finding to the responder with the incident.
	if c.midStage.settled && !learnedPlaybookExplains(loop, finding, c.midStageAppliedLocked()) {
		c.mu.Unlock()
		return true
	}
	if c.midStage.running {
		c.midStage.dirty = true
		c.mu.Unlock()
		return true
	}
	c.midStage.running, c.midStage.dirty = true, false
	incidentID := request.IncidentID
	c.mu.Unlock()
	c.goBackground(func() { c.runMidIncidentPasses(ctx, incidentID, loop) })
	return true
}

func (c *Controller) deterministicStageSelectedLocked() bool {
	if c.codePath == nil {
		return false
	}
	for _, name := range c.codePath.Selection.ResponseStages {
		if name == "deterministic" {
			return true
		}
	}
	return false
}

// learnedPlaybookExplains reports whether a learned program outside except
// can explain the finding.
func learnedPlaybookExplains(loop *ResponderLoop, finding sdk.Finding, except map[string]bool) bool {
	if loop.Programs == nil {
		return false
	}
	for _, spec := range loop.Programs.List() {
		if !except[spec.ID] && programLinksFinding(spec, finding) {
			return true
		}
	}
	return false
}

// midStageAppliedLocked lists the programs a settled stage has already
// applied in this incident; before the stage settles nothing is excluded.
func (c *Controller) midStageAppliedLocked() map[string]bool {
	if !c.midStage.settled || c.currentIncidentRequest == nil {
		return nil
	}
	return stageAppliedPrograms(deterministicStageReport(c.currentIncidentRequest.PriorStages))
}

func deterministicStageReport(reports []*contracts.StageReport) *contracts.StageReport {
	var report *contracts.StageReport
	for _, prior := range reports {
		if prior.GetStage() == "deterministic" {
			report = prior
		}
	}
	return report
}

func stageAppliedPrograms(report *contracts.StageReport) map[string]bool {
	applied := map[string]bool{}
	for _, attempt := range report.GetMitigations() {
		if attempt.GetResult() != "skipped" {
			applied[attempt.GetPlaybookId()] = true
		}
	}
	return applied
}

// stagePassProgressed reports whether a later pass cleared a finding or
// applied a program the incident had not applied before.
func stagePassProgressed(earlier, report *contracts.StageReport) bool {
	cleared := map[string]bool{}
	for _, fp := range earlier.GetClearedFindingFingerprints() {
		cleared[fp] = true
	}
	for _, fp := range report.GetClearedFindingFingerprints() {
		if !cleared[fp] {
			return true
		}
	}
	before := stageAppliedPrograms(earlier)
	for id := range stageAppliedPrograms(report) {
		if !before[id] {
			return true
		}
	}
	return false
}

func (c *Controller) runMidIncidentPasses(ctx context.Context, incidentID string, loop *ResponderLoop) {
	for {
		progressed, err := c.runMidIncidentPass(ctx, incidentID, loop)
		if err != nil {
			slog.Warn("later automatic stage pass", "incident_id", incidentID, "error", err)
		}
		c.mu.Lock()
		if err == nil {
			c.midStage.settled = !progressed
		}
		again := c.midStage.dirty && !c.midStage.settled && c.midStage.passes < maxMidIncidentPasses && c.incidentOpen &&
			c.currentIncidentRequest != nil && c.currentIncidentRequest.IncidentID == incidentID && !c.responderDone
		c.midStage.dirty = false
		if !again {
			c.midStage.running = false
			c.mu.Unlock()
			return
		}
		c.mu.Unlock()
	}
}

// runMidIncidentPass runs the automatic stage again on the responder's own
// loop, so its applications, rollout waits and receipts are the ones the
// responder sees, and serialize with its tool calls. It reports whether the
// pass made progress (stagePassProgressed).
func (c *Controller) runMidIncidentPass(ctx context.Context, incidentID string, loop *ResponderLoop) (bool, error) {
	actionCtx, cancel, err := c.actionContext(ctx)
	if err != nil {
		return false, err
	}
	defer cancel()
	// The pass holds the loop's locks, so no read inside it may hang: twice the
	// stage's own budget covers its checks, apply and rollout wait.
	actionCtx, stop := context.WithTimeout(actionCtx, 2*DefaultStageBudget)
	defer stop()
	c.mu.Lock()
	if c.currentIncidentRequest == nil || c.currentIncidentRequest.IncidentID != incidentID {
		c.mu.Unlock()
		return false, nil
	}
	c.midStage.passes++
	request := *cloneIncidentRequest(c.currentIncidentRequest)
	c.mu.Unlock()
	state := &IncidentState{Request: request, Findings: append([]sdk.Finding(nil), request.Findings...), Coverage: cloneCoverage(request.PlaybookCoverage), PriorStages: cloneStageReports(request.PriorStages)}
	earlier := deterministicStageReport(state.PriorStages)
	stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, KeepLoop: true}
	report, runErr := RunStageAgain(actionCtx, state, stage)
	notes := stageNotes(earlier, report)
	c.mu.Lock()
	if c.currentIncidentRequest != nil && c.currentIncidentRequest.IncidentID == incidentID {
		c.currentIncidentRequest.PriorStages = cloneStageReports(state.PriorStages)
	}
	c.mu.Unlock()
	loop.noteStages(notes)
	if err := c.PersistState(ctx); err != nil {
		slog.Warn("persist later automatic stage pass", "incident_id", incidentID, "error", err)
	}
	return stagePassProgressed(earlier, report), runErr
}

// stageNotes lists what the later pass changed in the report: attempts that are
// new, or that went from skipped to a result.
func stageNotes(earlier, report *contracts.StageReport) []StageNote {
	before := map[string]*contracts.StageMitigation{}
	for _, attempt := range earlier.GetMitigations() {
		before[stageAttemptKey(attempt.PlaybookId, programBindingsState(attempt.Bindings))] = attempt
	}
	var notes []StageNote
	for _, attempt := range report.GetMitigations() {
		prior := before[stageAttemptKey(attempt.PlaybookId, programBindingsState(attempt.Bindings))]
		if prior != nil && proto.Equal(prior, attempt) {
			continue
		}
		notes = append(notes, StageNote{Stage: "deterministic", PlaybookID: attempt.PlaybookId, Result: attempt.Result, SkipReason: attempt.SkipReason, Error: attempt.Error, ClearedFindings: len(attempt.ClearedFindingFingerprints)})
	}
	return notes
}

// noteStages queues automatic-pass results for the responder's next tool reply.
func (l *ResponderLoop) noteStages(notes []StageNote) {
	if len(notes) == 0 {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.stageNotes = append(l.stageNotes, notes...)
}

// takeStageNotes hands the queued notes to the agent's reply, once. A stage's
// own call (a later pass plans and applies through Handle) leaves them queued,
// so notes from every pass accumulate until the agent calls. Callers hold l.mu.
func (l *ResponderLoop) takeStageNotes(request ToolRequest) []StageNote {
	if request.stage {
		return nil
	}
	notes := l.stageNotes
	l.stageNotes = nil
	return notes
}

// mergeLaterStagePasses keeps what later passes added while the pipeline ran:
// the pipeline returns the reports it started with, and a stage's report only
// ever gains attempts and results, so the one with more of them is the newer.
func mergeLaterStagePasses(completed, current []*contracts.StageReport) []*contracts.StageReport {
	merged := cloneStageReports(completed)
	for i, report := range merged {
		for _, newer := range current {
			if newer.GetStage() == report.GetStage() && stageReportProgress(newer) > stageReportProgress(report) {
				merged[i] = proto.Clone(newer).(*contracts.StageReport)
			}
		}
	}
	return merged
}

func stageReportProgress(report *contracts.StageReport) int {
	progress := 0
	for _, attempt := range report.GetMitigations() {
		progress++
		if attempt.GetResult() != "skipped" {
			progress += len(report.GetMitigations()) + 1
		}
	}
	return progress
}
