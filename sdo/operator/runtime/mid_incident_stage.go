package runtime

import (
	"context"
	"log/slog"

	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/sdk"
)

// maxMidIncidentPasses bounds the automatic stage's re-runs over one incident.
// Each pass has the stage budget and never repeats an attempt that wrote, so
// the bound only limits how often a flapping detector can ask.
const maxMidIncidentPasses = 8

// midIncidentStage coalesces finding changes into bounded passes, one at a time.
type midIncidentStage struct {
	running bool
	dirty   bool
	pending bool
	passes  int
}

func (c *Controller) queueLearnedPassLocked() {
	if c.midStage.running {
		c.midStage.dirty = true
	} else {
		c.midStage.pending = true
	}
}

// attachLearnedActivation queues a pass for linked evidence while the
// automatic stage still owns the incident. It reports whether it took the
// finding. After handoff the ordinary alert-join path handles it.
func (c *Controller) attachLearnedActivation(finding sdk.Finding) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	loop := c.responderLoop
	if !c.incidentOpen || c.currentIncidentRequest == nil || c.incidentDispatchedAt.IsZero() || !c.learnedPassAllowedLocked() ||
		!c.pendingStageCapacityLocked() ||
		!learnedPlaybookExplains(loop, finding) {
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
	c.queueLearnedPassLocked()
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
	if !c.pendingLearnedPassReadyLocked() {
		c.mu.Unlock()
		return
	}
	c.startMidPassesLocked(ctx, c.currentIncidentRequest.IncidentID, loop)
}

func (c *Controller) pendingLearnedPassReadyLocked() bool {
	return c.midStage.pending && !c.responseStagesRunning && !c.midStage.running &&
		c.pendingStageCapacityLocked() && c.learnedPassAllowedLocked()
}

func (c *Controller) pendingStageCapacityLocked() bool {
	if c.codePath == nil || c.responderLoop == nil || c.midStage.passes >= maxMidIncidentPasses {
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
		err := c.runMidIncidentPass(ctx, incidentID, loop)
		if err != nil {
			slog.Warn("later automatic stage pass", "incident_id", incidentID, "error", err)
		}
		c.mu.Lock()
		again := c.midStage.dirty && c.midStage.passes < maxMidIncidentPasses && c.incidentOpen &&
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

// runMidIncidentPass extends controller-owned recovery within its existing deadline.
func (c *Controller) runMidIncidentPass(ctx context.Context, incidentID string, loop *ResponderLoop) error {
	c.mu.Lock()
	if c.currentIncidentRequest == nil || c.currentIncidentRequest.IncidentID != incidentID || !c.learnedPassAllowedLocked() {
		c.mu.Unlock()
		return nil
	}
	c.midStage.passes++
	request := *cloneIncidentRequest(c.currentIncidentRequest)
	awaiting := c.awaitingAutomaticRecoveryLocked()
	deadline := request.ResponseDeadline
	if playbookWrote(awaiting) {
		deadline = c.automaticDeadlineLocked(awaiting)
	}
	c.mu.Unlock()

	actionCtx, cancel, err := c.actionContext(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	budget := 2 * c.config.StageBudget
	if !deadline.IsZero() {
		budget = min(budget, deadline.Sub(c.clock()))
	}
	actionCtx, stop := context.WithTimeout(actionCtx, budget)
	defer stop()
	state := &IncidentState{Request: request, Findings: append([]sdk.Finding(nil), request.Findings...), Coverage: cloneCoverage(request.PlaybookCoverage), PriorStages: cloneStageReports(request.PriorStages)}
	stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, KeepLoop: true, StabilityWindow: c.config.ClosureStabilityWindow}
	if c.config.AfterPlaybook == settings.AfterPlaybookWait {
		stage.WaitBound = c.config.AfterPlaybookWait
	}
	if c.tuneMidStage != nil {
		c.tuneMidStage(stage)
	}
	_, runErr := RunStageAgain(actionCtx, state, stage)
	c.mu.Lock()
	if c.currentIncidentRequest != nil && c.currentIncidentRequest.IncidentID == incidentID {
		state.PriorStages = mergeStageReports(c.currentIncidentRequest.PriorStages, state.PriorStages)
		c.currentIncidentRequest.PriorStages = state.PriorStages
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
	return runErr
}

// mergeStageReports folds new evidence into the existing stage order.
func mergeStageReports(earlier, fresh []*contracts.StageReport) []*contracts.StageReport {
	merged := cloneStageReports(earlier)
	for _, report := range fresh {
		found := false
		for i, prior := range merged {
			if prior.GetStage() == report.GetStage() {
				merged[i] = mergeStagePass(prior, proto.Clone(report).(*contracts.StageReport))
				found = true
				break
			}
		}
		if !found {
			merged = append(merged, proto.Clone(report).(*contracts.StageReport))
		}
	}
	return merged
}

// The automatic stage can take new evidence only while it owns the repair.
// Dispatch freezes the handoff report and gives write ownership to the agent.
func (c *Controller) learnedPassAllowedLocked() bool {
	if request := c.currentIncidentRequest; request == nil || pageOpened(request) ||
		!request.ResponseDeadline.IsZero() && !c.clock().Before(request.ResponseDeadline) || stageEscalatedIn(request.PriorStages) {
		return false
	}
	if c.responseStagesRunning {
		return true
	}
	report := c.awaitingAutomaticRecoveryLocked()
	if playbookWrote(report) {
		return report.GetStabilityDeadline() == nil && c.clock().Before(c.automaticDeadlineLocked(report))
	}
	// Drain evidence queued during the opening pass even if it wrote nothing.
	return report != nil && !stagePlanRefused(report) && (c.midStage.pending || c.midStage.running)
}
