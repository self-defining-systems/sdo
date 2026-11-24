package runtime

import (
	"context"
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
// settles the stage: the responder owns what remains, and later activations
// of findings the incident already holds join it without another pass. A
// finding new to the incident (a symptom joining the alert set, a clue)
// reopens the stage for one more pass.
type midIncidentStage struct {
	running bool
	dirty   bool
	settled bool
	// pending holds a finding that arrived while the first response pipeline
	// was running; one pass evaluates it when the pipeline ends.
	pending bool
	passes  int
}

// attachLearnedActivation queues a pass for linked evidence while the
// automatic stage still owns the incident. It reports whether it took the
// finding. After handoff the ordinary alert-join path handles it.
func (c *Controller) attachLearnedActivation(ctx context.Context, finding sdk.Finding) bool {
	c.mu.Lock()
	loop := c.responderLoop
	if !c.incidentOpen || c.currentIncidentRequest == nil || c.incidentDispatchedAt.IsZero() || !c.learnedPassAllowedLocked() ||
		loop == nil || c.midStage.passes >= maxMidIncidentPasses || !c.deterministicStageSelectedLocked() ||
		!learnedPlaybookExplains(loop, finding) {
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
		// New evidence reopens a settled stage: the pass that settled it never
		// saw this finding. The pass cap still bounds a flapping detector.
		c.midStage.settled = false
	}
	// The first pipeline is still running: it diagnosed without this finding,
	// so one pass follows it (resumePendingLearnedPass).
	if c.responseStagesRunning {
		c.midStage.pending = true
		c.mu.Unlock()
		return true
	}
	// A settled stage hands the finding to the responder with the incident.
	if c.midStage.settled {
		c.mu.Unlock()
		return true
	}
	// A pass is running: it ends with one more pass, so the finding is
	// evaluated even if the running pass already diagnosed.
	if c.midStage.running {
		c.midStage.dirty = true
		c.mu.Unlock()
		return true
	}
	c.startMidPassesLocked(ctx, request.IncidentID, loop)
	return true
}

// startMidPassesLocked starts the pass loop; it releases c.mu.
func (c *Controller) startMidPassesLocked(ctx context.Context, incidentID string, loop *ResponderLoop) {
	c.midStage.running, c.midStage.dirty, c.midStage.pending = true, false, false
	c.mu.Unlock()
	c.goBackground(func() { c.runMidIncidentPasses(ctx, incidentID, loop) })
}

// resumePendingLearnedPass starts the pass a finding asked for while the first
// response pipeline was running, once that pipeline has ended.
func (c *Controller) resumePendingLearnedPass(ctx context.Context) {
	c.mu.Lock()
	loop := c.responderLoop
	if !c.midStage.pending || c.responseStagesRunning || c.midStage.running || c.midStage.settled || !c.incidentOpen ||
		c.currentIncidentRequest == nil || !c.learnedPassAllowedLocked() ||
		loop == nil || c.midStage.passes >= maxMidIncidentPasses {
		c.mu.Unlock()
		return
	}
	c.startMidPassesLocked(ctx, c.currentIncidentRequest.IncidentID, loop)
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

func learnedPlaybookExplains(loop *ResponderLoop, finding sdk.Finding) bool {
	if loop.Programs == nil {
		return false
	}
	for _, spec := range loop.Programs.List() {
		if programLinksFinding(spec, finding) {
			return true
		}
	}
	return false
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

// A reverted attempt left nothing behind, so it is not an application here.
func stageAppliedPrograms(report *contracts.StageReport) map[string]bool {
	applied := map[string]bool{}
	for _, attempt := range report.GetMitigations() {
		if attempt.GetResult() != "skipped" {
			applied[attempt.GetPlaybookId()] = true
		}
	}
	return applied
}

// stagePassProgressed reports whether a later pass verified a repair of its
// own (an attempt it added or completed is applied_cleared) or applied a
// program the incident had not applied before. Findings the responder cleared
// meanwhile are not the stage's progress.
func stagePassProgressed(earlier, report *contracts.StageReport) bool {
	before := map[string]*contracts.StageMitigation{}
	for _, attempt := range earlier.GetMitigations() {
		before[stageAttemptKey(attempt.PlaybookId, programBindingsState(attempt.Bindings))] = attempt
	}
	for _, attempt := range report.GetMitigations() {
		prior := before[stageAttemptKey(attempt.PlaybookId, programBindingsState(attempt.Bindings))]
		if attempt.GetResult() == "applied_cleared" && prior.GetResult() != "applied_cleared" {
			return true
		}
	}
	applied := stageAppliedPrograms(earlier)
	for id := range stageAppliedPrograms(report) {
		if !applied[id] {
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
		// A finding that arrived during the pass has not been evaluated yet.
		if err == nil && !progressed && !c.midStage.dirty {
			c.midStage.settled = true
		}
		again := c.midStage.dirty && !c.midStage.settled && c.midStage.passes < maxMidIncidentPasses && c.incidentOpen &&
			c.currentIncidentRequest != nil && c.currentIncidentRequest.IncidentID == incidentID && c.learnedPassAllowedLocked()
		c.midStage.dirty = false
		if !again {
			c.midStage.running = false
			c.mu.Unlock()
			return
		}
		c.mu.Unlock()
	}
}

// runMidIncidentPass extends the controller-owned automatic stage with new
// evidence. It reports whether the pass made progress (stagePassProgressed).
func (c *Controller) runMidIncidentPass(ctx context.Context, incidentID string, loop *ResponderLoop) (bool, error) {
	actionCtx, cancel, err := c.actionContext(ctx)
	if err != nil {
		return false, err
	}
	defer cancel()
	// The pass holds the loop's locks, so no read inside it may hang: twice the
	// stage's own budget covers its checks and apply.
	actionCtx, stop := context.WithTimeout(actionCtx, 2*c.config.StageBudget)
	defer stop()
	c.mu.Lock()
	if c.currentIncidentRequest == nil || c.currentIncidentRequest.IncidentID != incidentID {
		c.mu.Unlock()
		return false, nil
	}
	c.midStage.passes++
	request := *cloneIncidentRequest(c.currentIncidentRequest)
	awaiting := c.awaitingAutomaticRecoveryLocked()
	if awaiting != nil {
		var cancelWait context.CancelFunc
		actionCtx, cancelWait = context.WithTimeout(actionCtx, c.automaticDeadlineLocked(awaiting).Sub(c.clock()))
		defer cancelWait()
	}
	c.mu.Unlock()
	state := &IncidentState{Request: request, Findings: append([]sdk.Finding(nil), request.Findings...), Coverage: cloneCoverage(request.PlaybookCoverage), PriorStages: cloneStageReports(request.PriorStages)}
	earlier := deterministicStageReport(state.PriorStages)
	stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, KeepLoop: true}
	if c.tuneMidStage != nil {
		c.tuneMidStage(stage)
	}
	report, runErr := RunStageAgain(actionCtx, state, stage)
	c.mu.Lock()
	if c.currentIncidentRequest != nil && c.currentIncidentRequest.IncidentID == incidentID {
		c.currentIncidentRequest.PriorStages = cloneStageReports(state.PriorStages)
		// Receipts, their diagnoses and finding coverage form one snapshot.
		// Publishing only the report loses the proof required by reflection.
		c.currentIncidentRequest.PlaybookCoverage = cloneCoverage(state.Coverage)
		c.currentIncidentRequest.InitialProgramRuns = append([]ProgramRun(nil), state.Request.InitialProgramRuns...)
		if awaiting != nil && c.awaitingAutomaticRecoveryLocked() != nil {
			previous := c.currentIncidentResult.Timing
			if merged, err := responsePipelineResult(state, previous.StartedAt, previous.CompletedAt); err == nil {
				c.currentIncidentResult = &merged
				c.closurePrep = nil
			} else if runErr == nil {
				runErr = err
			}
		}
	}
	c.mu.Unlock()
	if err := c.PersistState(ctx); err != nil {
		slog.Warn("persist later automatic stage pass", "incident_id", incidentID, "error", err)
	}
	return stagePassProgressed(earlier, report), runErr
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

// The automatic stage can take new evidence only while it owns the repair.
// Dispatch freezes the handoff report and gives write ownership to the agent.
func (c *Controller) learnedPassAllowedLocked() bool {
	if c.responseStagesRunning {
		return true
	}
	report := c.awaitingAutomaticRecoveryLocked()
	return playbookWrote(report) && !stagePlanRefused(report) && c.clock().Before(controllerTime(report.GetRecoveryDeadline()))
}
