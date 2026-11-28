package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"google.golang.org/protobuf/encoding/protojson"
	"log/slog"
	"runtime/debug"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"slices"
	"sort"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/planwire"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/podread"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/storestate"
	"sdo.dev/operator/sdk/workloadhealth"
)

const stagePlanNotExecutable = "plan_not_executable"

func automaticPlanRefused(attempt *contracts.StageMitigation) bool {
	// Older reports recorded this reason as a skipped binding. Only a terminal
	// refusal recorded as a failed attempt ends that binding and affects trust.
	return attempt.GetResult() == "apply_error" && attempt.GetSkipReason() == stagePlanNotExecutable
}

func stagePlanRefused(report *contracts.StageReport) bool {
	return slices.ContainsFunc(report.GetMitigations(), automaticPlanRefused)
}

// stageIneffective skips a match whose program revision already applied in
// this incident for the same findings and left them open (or matching). Its
// replacement objects, such as the Pod a restart creates, are new bindings
// but not new evidence. A finding the earlier attempt did not target is.
const stageIneffective = "ineffective"

// stageBuiltinRestoreManual skips the built-in source-restore playbook when
// response.builtin_restore_auto (on by default) is switched off.
const stageBuiltinRestoreManual = "builtin_restore_manual"

// stageIneffectiveTargets maps a program revision to the findings it applied
// for and did not clear.
type stageIneffectiveTargets map[string]map[string]bool

func ineffectiveStageTargets(attempts []*contracts.StageMitigation) stageIneffectiveTargets {
	targets := stageIneffectiveTargets{}
	for _, attempt := range attempts {
		targets.add(attempt)
	}
	return targets
}

func (t stageIneffectiveTargets) add(attempt *contracts.StageMitigation) {
	if attempt.GetResult() != "applied_still_matching" {
		return
	}
	key := attempt.GetPlaybookId() + "\x00" + attempt.GetProgramRevision()
	if t[key] == nil {
		t[key] = map[string]bool{}
	}
	for _, fp := range append(append([]string(nil), attempt.GetClearedFindingFingerprints()...), attempt.GetRemainingFindingFingerprints()...) {
		t[key][fp] = true
	}
}

func (t stageIneffectiveTargets) covers(id, revision string, targets []string) bool {
	seen := t[id+"\x00"+revision]
	if len(seen) == 0 || len(targets) == 0 {
		return false
	}
	for _, fp := range targets {
		if !seen[fp] {
			return false
		}
	}
	return true
}

// DeterministicResponseStage uses the same plan/apply and immutable receipt
// boundary as the responder, operating only on accepted registry programs.
type DeterministicResponseStage struct {
	NewLoop func(IncidentRequest) *ResponderLoop
	Restore func(context.Context, IncidentRequest, *ResponderLoop) error
	// KeepLoop leaves the loop NewLoop returns open: it belongs to a running
	// responder, so a later pass must not close its working programs.
	KeepLoop bool
	// RolloutTimeout bounds the executor's apply-time observation.
	RolloutTimeout  time.Duration
	StabilityWindow time.Duration
	// WaitBound is the after_playbook wait policy's whole budget.
	WaitBound time.Duration
	// EstimateBound caps the recovery estimate of the other policies
	// (response.after_playbook_wait): the agent never waits longer than the
	// wait policy would.
	EstimateBound time.Duration
	diagnoseWait  func(context.Context, time.Duration) error
}

type responseStageProgress struct {
	RecoveryDeadline   time.Time
	Report             *contracts.StageReport
	Coverage           *contracts.PlaybookCoverage
	InitialProgramRuns []ProgramRun
	Complete           bool
}

// Restore decisions for automatic refresh and ordinary tool rejoin alike;
// unchanged accepted code cannot regain retries by crossing a checkpoint.
func (l *ResponderLoop) restoreTerminalStagePlanErrors() {
	for _, progress := range l.stageProgress {
		for _, attempt := range progress.Report.GetMitigations() {
			if attempt.SkipReason == stagePlanNotExecutable {
				l.rememberTerminalPlanError(attempt.PlaybookId, attempt.ProgramRevision, programBindingsState(attempt.Bindings), &NonExecutablePlanError{Cause: errors.New(attempt.Error)})
			}
		}
	}
}

func (*DeterministicResponseStage) Name() string { return "deterministic" }

func (stage *DeterministicResponseStage) Run(ctx context.Context, state *IncidentState) (*contracts.StageReport, error) {
	if prior := deterministicStageReport(state.PriorStages); prior.GetHandoffSelected() {
		return proto.Clone(prior).(*contracts.StageReport), nil
	}
	ctx = forAutomaticRestore(ctx)
	report := &contracts.StageReport{Stage: stage.Name(), StartedAt: contractTimestamp(lifecycleio.Now().UTC())}
	page := pageOpened(&state.Request)
	if stage.NewLoop == nil {
		return report, nil
	}
	// The budget covers everything up to the write. A write that began finishes.
	applyCtx, diagnoseCtx := ctx, ctx
	if page {
		var cancel, cancelDiagnose context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, pageScanBudget)
		defer cancel()
		diagnoseCtx, cancelDiagnose = context.WithTimeout(ctx, pageDiagnoseBudget)
		defer cancelDiagnose()
	}
	loop := stage.NewLoop(state.Request)
	if loop == nil {
		return report, nil
	}
	report.StartedAt = contractTimestamp(loop.clock().UTC())
	startedAt := report.StartedAt
	if !stage.KeepLoop {
		defer loop.CloseWorkingPrograms()
	}
	defer func() {
		state.RepairActions = loop.ApplicationRepairReceipts(state.RepairActions)
		state.ProgramToolCalls = loop.ToolCalls()
	}()
	if stage.Restore != nil {
		if err := stage.Restore(ctx, state.Request, loop); err != nil {
			return report, err
		}
	}
	loop.locked(func() { loop.recoverInterruptedScripts(ctx) })
	if loop.stageProgress == nil {
		loop.stageProgress = map[string]responseStageProgress{}
	}
	progress := loop.stageProgress[stage.Name()]
	if progress.Report != nil {
		report = proto.Clone(progress.Report).(*contracts.StageReport)
		state.Coverage = cloneCoverage(progress.Coverage)
		state.Request.PlaybookCoverage = cloneCoverage(progress.Coverage)
		state.Request.InitialProgramRuns = append([]ProgramRun(nil), progress.InitialProgramRuns...)
		loop.Request = state.Request
	}
	if state.Refresh {
		if prior := deterministicStageReport(state.PriorStages); prior != nil {
			report = mergeStagePass(prior, report)
		}
		if !playbookWrote(report) {
			report.ApplyCompletedAt, report.RecoveryWait = nil, nil
		}
	}

	if report.StartedAt == nil {
		report.StartedAt = startedAt
	}
	loop.restoreTerminalStagePlanErrors()

	save := func(complete bool) error {
		retainAdmissionStageFindings(state, report)
		report.RemainingFindingFingerprints = findingFingerprints(state.Findings)
		loop.stageProgress[stage.Name()] = responseStageProgress{Report: report, Coverage: cloneCoverage(state.Coverage), InitialProgramRuns: append([]ProgramRun(nil), state.Request.InitialProgramRuns...), Complete: complete, RecoveryDeadline: progress.RecoveryDeadline}
		return loop.persistToolState(applyCtx)
	}
	// A receipt can have reached the mutation journal just before the stage
	// checkpoint. Recover it by immutable plan identity without repeating it.
	if progress.Report != nil {
		for _, attempt := range report.Mitigations {
			if attempt.PlanId == "" {
				continue
			}
			for _, app := range loop.ApplicationEvidence() {
				if slices.Contains(app.PlanIDs, attempt.PlanId) && (attempt.ApplyReceipt == nil ||
					app.Result.Applied && hasAppliedScript(app.Result.Plan.Actions) && app.Error != "") {
					receipt, err := recordedPlanContract(app)
					if err != nil {
						return report, err
					}
					attempt.ApplyReceipt = receipt
					attempt.TouchedObjects = touchedObjects(app.Result)
					break
				}
			}
		}
		if progress.Complete && !state.Refresh {
			stage.armRecovery(state, loop, report, progress)
			return report, save(true)
		}
	}
	attempted := map[string]bool{}
	// A later pass may complete a binding an earlier one only skipped, because
	// its finding had not activated yet. A skip wrote nothing, so that attempt
	// is redone in its place; any attempt that wrote is never repeated.
	retry := map[string]*contracts.StageMitigation{}
	for _, attempt := range report.Mitigations {
		key := stageAttemptKey(attempt.PlaybookId, programBindingsState(attempt.Bindings))
		// A plan that waited for a shared object's earlier write wrote nothing
		// either; a restart before its turn redoes it.
		if attempt.Result == "skipped" && (state.Refresh && attempt.SkipReason != stagePlanNotExecutable || stageSkipRetryable(attempt.SkipReason)) {
			retry[key] = attempt
			continue
		}
		attempted[key] = true
	}
	// A later pass rebuilds coverage for what is still open, from a fresh
	// diagnosis. The opening capture is immutable evidence, so it is not retaken.
	if state.Refresh {
		state.Coverage = nil
	}
	if state.Coverage == nil {
		var runs []ProgramRun
		var err error
		// openCtx and until bound the opening diagnosis and its re-asks; zero
		// until is no bound beyond ctx.
		openCtx, until := ctx, time.Time{}
		if state.Refresh {
			// A later pass diagnoses live state: the loop's findings are the ones
			// it last observed, which predate whatever joined the incident since.
			observed := ToolResponse{}
			func() {
				loop.operationMu.Lock()
				defer loop.operationMu.Unlock()
				loop.mu.Lock()
				defer loop.mu.Unlock()
				loop.verify(ctx, nil, &observed, true)
			}()
			if observed.Error != "" && len(observed.DetectorStates) == 0 {
				return report, errors.New(observed.Error)
			}
			updateStageFindings(state, observed)
			runs, err = loop.RefreshDiagnose(ctx, state.Findings, attempted)
			if err != nil {
				return report, err
			}
			fillProgramRevisionsWith(loop.gitRead(), ctx, runs, state.Request.RepositoryWorktree, state.Request.SourceCommit)
		} else if page {
			runs = loop.scanPage(diagnoseCtx)
			if !pageMatch(runs) {
				report.Diagnoses = stageDiagnoses(runs)
				stage.armRecovery(state, loop, report, progress)
				return report, save(true)
			}
			fillProgramRevisionsWith(loop.gitRead(), ctx, runs, state.Request.RepositoryWorktree, state.Request.SourceCommit)
		} else {
			// The opening read of playbook detectors and the opening diagnosis
			// share one diagnosis deadline, so the responder's dispatch slips by
			// at most that bound in all. The read takes at most half of it,
			// leaving the diagnosis the rest.
			bound := loop.deadline()
			until = loop.clock().Add(bound)
			var cancelOpen context.CancelFunc
			openCtx, cancelOpen = context.WithTimeout(ctx, bound)
			defer cancelOpen()
			stage.readPlaybookDetectors(openCtx, loop, state, loop.clock().Add(bound/2))
			runs = loop.InitialDiagnose(openCtx)
			fillProgramRevisionsWith(loop.gitRead(), ctx, runs, state.Request.RepositoryWorktree, state.Request.SourceCommit)
		}
		if !state.Refresh {
			if runs, err = stage.reaskUnanswered(openCtx, loop, state, runs, until); err != nil && (ctx.Err() != nil || openCtx.Err() == nil) {
				return report, err
			}
		}
		report.Diagnoses = mergeStageDiagnoses(report.Diagnoses, stageDiagnoses(runs))
		if state.Refresh {
			runs = append(append([]ProgramRun(nil), state.Request.InitialProgramRuns...), runs...)
		}
		state.Request.InitialProgramRuns = runs
		state.Coverage = buildPlaybookCoverage(state.Findings, state.Request.InitialProgramRuns, loop.lastDiagnosisView, loop.Trust)
		state.Request.PlaybookCoverage = cloneCoverage(state.Coverage)
		loop.Request = state.Request
	}
	loop.operationMu.Lock()
	defer loop.operationMu.Unlock()
	loop.mu.Lock()
	defer loop.mu.Unlock()
	matches := stageCandidates(state)
	ineffective := ineffectiveStageTargets(report.Mitigations)
	// A turn diagnoses and plans every match on live state before it writes,
	// then applies the planned ones as a batch, each as its own application
	// (a failure part-way compensates only its own plan), with one verify for
	// the whole batch. Only diagnoses that wrote nothing receive a second turn,
	// and only after a turn that wrote or cleared something: an unchanged
	// cluster would give the same diagnoses again. A plan that reaches an object
	// an earlier plan of the turn writes (or whose write set is unknown) waits
	// for the next turn, so it is diagnosed and planned on the state that write
	// left. Keep the opening
	// order; dependencies are learned from live state, never names.
	deferred, shared := map[string]bool{}, map[string]bool{}
	for start := 0; start < len(matches); {
		turnEnd, turnFindings, progressed, waiting := len(matches), len(state.Findings), false, false
		var batch []stagePlanned
		var turnView program.ReadView
		var stopErr error
		// requeue gives a match one more try in the next turn, once per seen.
		requeue := func(seen map[string]bool, key string, match stageCandidate, attempt *contracts.StageMitigation) bool {
			if seen[key] {
				return false
			}
			seen[key] = true
			delete(attempted, key)
			retry[key] = attempt
			matches = append(matches, match)
			return true
		}
		// waitTurn moves a planned binding to the next turn: it wrote nothing.
		waitTurn := func(item stagePlanned) error {
			if requeue(shared, item.key, item.match, item.attempt) {
				waiting = true
			}
			item.attempt.Result, item.attempt.SkipReason = "skipped", stageObjectShared
			recordStageFindingProgress(item.attempt, item.match.targets, state.Findings)
			return save(false)
		}
		for index := start; index < turnEnd && stopErr == nil; index++ {
			match := matches[index]
			if len(state.Findings) == 0 && !page {
				break
			}
			if err := ctx.Err(); err != nil {
				// Plans already made are this turn's writes: they are applied below.
				stopErr = err
				break
			}
			bindings := programBindingsState(match.Bindings)
			key := stageAttemptKey(match.PlaybookId, bindings)
			if attempted[key] {
				continue
			}
			attempted[key] = true
			attempt := &contracts.StageMitigation{PlaybookId: match.PlaybookId, ProgramRevision: match.ProgramRevision, Bindings: programBindingsContract(bindings), Result: "skipped"}
			if attempt.ProgramRevision == "" {
				attempt.ProgramRevision = loop.programRevision(match.PlaybookId)
			}
			if earlier := retry[key]; earlier != nil {
				proto.Reset(earlier)
				proto.Merge(earlier, attempt)
				attempt = earlier
			} else {
				report.Mitigations = append(report.Mitigations, attempt)
			}
			skip := func(reason string) error {
				attempt.SkipReason = reason
				recordStageFindingProgress(attempt, match.targets, state.Findings)
				return save(false)
			}
			recordRefusal := func(err error) {
				attempt.Result = "apply_error"
				attempt.SkipReason = stagePlanNotExecutable
				attempt.Error = err.Error()
				recordStageFindingProgress(attempt, match.targets, state.Findings)
			}
			if ineffective.covers(attempt.PlaybookId, attempt.ProgramRevision, match.targets) {
				if err := skip(stageIneffective); err != nil {
					return report, err
				}
				continue
			}
			if loop.Credits.Has(match.PlaybookId) && loop.earlierRepairUnsettled(ctx, report, attempt) {
				if err := skip(stageAlreadyAttempted); err != nil {
					return report, err
				}
				continue
			}
			if match.PlaybookId == SourceRestoreID && !loop.BuiltinRestoreAuto {
				if err := skip(stageBuiltinRestoreManual); err != nil {
					return report, err
				}
				continue
			}
			if state.Request.RepairPolicy != "recorded-actions" {
				if err := skip("repair_policy"); err != nil {
					return report, err
				}
				continue
			}
			if loop.Programs == nil {
				if err := skip("no_mitigation"); err != nil {
					return report, err
				}
				continue
			}
			count := 0
			for _, spec := range loop.Programs.List() {
				if spec.ID == match.PlaybookId {
					count++
				}
			}
			if count > 1 {
				if err := skip("multiple_entry_points"); err != nil {
					return report, err
				}
				continue
			}
			// A Diagnose-only playbook still runs: an out-of-reach diagnosis
			// escalates without a mitigation.
			mitigator, canMitigate := loop.Programs.Mitigator(match.PlaybookId)
			diagnoser, ok := loop.Programs.Diagnoser(match.PlaybookId)
			if !ok {
				if err := skip("diagnose_unavailable"); err != nil {
					return report, err
				}
				continue
			}
			// The turn's diagnoses share one live view: nothing is written
			// between them.
			if turnView == nil {
				fresh, err := loop.snapshotView(ctx)
				if err != nil {
					attempt.Error = err.Error()
					if err := skip("diagnose_unavailable"); err != nil {
						return report, err
					}
					continue
				}
				turnView = fresh
			}
			view := turnView
			before := program.RunDiagnose(diagnoseCtx, diagnoser, view, bindings, loop.deadline())
			// An Unknown verdict is a read that did not answer (a memory host
			// restart, a cancelled stream), not a diagnosis: ask again before
			// giving the playbook up, and keep the reason either way.
			for try := 1; !page && before.Result == program.Unknown && try < stageDiagnoseTries; try++ {
				if err := stage.diagnoseBackoff(ctx); err != nil {
					// Plans already made are this turn's writes: they are applied below.
					stopErr = err
					break
				}
				if fresh, viewErr := loop.snapshotView(ctx); viewErr == nil {
					view = fresh
				}
				before = program.RunDiagnose(ctx, diagnoser, view, bindings, loop.deadline())
			}
			if stopErr != nil {
				break
			}
			if before.Result == program.Unknown {
				attempt.Error = diagnosisFailure(before)
			}
			if before.Result != program.Match {
				if len(matches) > 1 {
					requeue(deferred, key, match, attempt)
				}
				reason := "diagnose_unavailable"
				if before.Result == program.NoMatch {
					reason = "diagnose_no_longer_matches"
					observed := ToolResponse{}
					loop.verify(ctx, nil, &observed, true)
					updateStageFindings(state, observed)
				}
				if err := skip(reason); err != nil {
					return report, err
				}
				continue
			}
			// Once the opening symptoms clear, a later pass can repair only
			// findings that still belong to the incident's alert set.
			if state.Refresh && (!state.openingObservedActive || len(newRecoveryScope(openingKeys(&state.Request)).blocking(state.Findings)) == 0) {
				active := findingFingerprints(newRecoveryScope(alertKeys(&state.Request)).blocking(state.Findings))
				match.targets = slices.DeleteFunc(slices.Clone(match.targets), func(fp string) bool {
					return !slices.Contains(active, fp)
				})
			}
			if len(match.targets) == 0 && !page {
				if err := skip("unexplained_coverage"); err != nil {
					return report, err
				}
				continue
			}
			escalation, escalates, escalationErr := stageEscalation(ctx, mitigator, canMitigate, view, bindings, before, loop.deadline())
			var refusal *NonExecutablePlanError
			if match.PlaybookId != SourceRestoreID && errors.As(escalationErr, &refusal) {
				loop.rememberTerminalPlanError(attempt.PlaybookId, attempt.ProgramRevision, bindings, refusal)
				recordRefusal(refusal)
				if err := save(false); err != nil {
					return report, err
				}
				continue
			}
			// Only the responder may conclude that a page is out of reach.
			if escalates && page {
				if err := skip("no_mitigation"); err != nil {
					return report, err
				}
				continue
			}
			if escalates {
				escalateStageAttempt(state, attempt, match, bindings, before, escalation)
				progressed = true
				if err := save(false); err != nil {
					return report, err
				}
				continue
			}
			if !canMitigate {
				if err := skip("no_mitigation"); err != nil {
					return report, err
				}
				continue
			}
			// Pin the live diagnosis view for planning; never reuse the dispatch-time
			// view, and never invoke a working-checkout compiler in the automatic stage.
			planResponse := ToolResponse{}
			loop.matchedViews[matchedViewKey(match.PlaybookId, bindings)] = view
			delete(loop.matchedPlans, matchedViewKey(match.PlaybookId, bindings))
			if err := withoutResponderLocks(loop, "plan", true, func() {
				planResponse = loop.Handle(ctx, ToolRequest{Command: "plan", ProgramID: match.PlaybookId, Bindings: bindings, FindingFingerprints: match.targets, stage: true})
			}); err != nil {
				return report, err
			}
			if planResponse.Error != "" {
				if terminal := loop.terminalPlanError(match.PlaybookId, bindings); terminal != nil {
					attempt.Error = terminal.Error()
					if match.PlaybookId == SourceRestoreID {
						if err := skip(stagePlanNotExecutable); err != nil {
							return report, err
						}
						continue
					}
					recordRefusal(terminal)
				} else {
					attempt.Result = "apply_error"
					attempt.Error = planResponse.Error
				}
				if err := save(false); err != nil {
					return report, err
				}
				continue
			}
			planned := stagePlanned{match: match, attempt: attempt, bindings: bindings, planID: planResponse.PlanID, key: key}
			plan := loop.plans[planResponse.PlanID].plan
			if loop.sameWriteInProgress(ctx, loop.clock(), report, plan) {
				if err := skip(stageAlreadyAttempted); err != nil {
					return report, err
				}
				continue
			}
			loop.stagePlanReach(ctx, plan, &planned)
			// A plan that reaches what an earlier plan of the turn writes waits
			// for the next turn, so it is diagnosed and planned on the state that
			// write left. A source restore keeps its place over a data repair it
			// conflicts with: restores go first.
			var conflicts []int
			for i, other := range batch {
				if other.reaches(planned) {
					conflicts = append(conflicts, i)
				}
			}
			displace := len(conflicts) > 0 && !planned.data && !planned.unknown && !slices.ContainsFunc(conflicts, func(i int) bool { return !batch[i].data || batch[i].unknown })
			if len(conflicts) > 0 && !displace {
				if err := waitTurn(planned); err != nil {
					return report, err
				}
				continue
			}
			for i := len(conflicts) - 1; i >= 0; i-- {
				displaced := batch[conflicts[i]]
				batch = slices.Delete(batch, conflicts[i], conflicts[i]+1)
				if err := waitTurn(displaced); err != nil {
					return report, err
				}
			}
			// Planned and not yet written: a restart before the batch applies
			// redoes it.
			attempt.SkipReason = stagePlannedInTurn
			batch = append(batch, planned)
		}
		// A turn whose findings all cleared while it planned writes nothing more.
		if len(batch) > 0 && len(state.Findings) == 0 && !page {
			for _, item := range batch {
				item.attempt.SkipReason = stageFindingsCleared
				recordStageFindingProgress(item.attempt, item.match.targets, state.Findings)
			}
			if err := save(false); err != nil {
				return report, err
			}
			batch = nil
		}
		if len(batch) > 0 {
			if err := stage.applyBatch(applyCtx, loop, state, batch, save); err != nil {
				return report, err
			}
			progressed = true
			for _, item := range batch {
				ineffective.add(item.attempt)
			}
		}
		// An unsuccessful application is evidence about that binding, not a
		// reason to starve the other matches. Each later match diagnoses fresh
		// state and goes through the ordinary conflict and mutation fences.
		if stopErr != nil {
			return report, stopErr
		}
		if len(state.Findings) == 0 && !page {
			break
		}
		if !progressed && !waiting && len(state.Findings) >= turnFindings {
			break
		}
		start = turnEnd
	}
	if !state.Refresh {
		stage.armRecovery(state, loop, report, progress)
	}
	if err := save(true); err != nil {
		return report, err
	}
	return report, nil
}

func stageAttemptKey(id string, bindings program.Bindings) string {
	return id + "\x00" + bindingKey(bindings)
}

func findingFingerprints(findings []sdk.Finding) []string {
	var result []string
	for _, finding := range findings {
		if finding.Status == sdk.FindingActive {
			result = append(result, FindingFingerprint(finding))
		}
	}
	sort.Strings(result)
	return result
}

// A candidate retains the exact live diagnosis bindings and its finding
// associations. Authored explanation links describe coverage, not bindability.
type stageCandidate struct {
	*contracts.PlaybookMatch
	targets []string
}

func stageCandidates(state *IncidentState) []stageCandidate {
	var matches []stageCandidate
	for _, match := range state.Coverage.GetMatches() {
		bindings := programBindingsState(match.Bindings)
		matches = append(matches, stageCandidate{PlaybookMatch: match, targets: matchedFindingFingerprints(state.Request.Findings, state.Request.InitialProgramRuns, match.PlaybookId, bindings)})
	}
	// Namespace admission is a prerequisite for workload recovery. Prioritize
	// its covered fixes using the detector contract, never playbook names or
	// benchmark fault identities. Preserve order among peers.
	admission := map[string]bool{}
	for _, finding := range state.Findings {
		if finding.Status == sdk.FindingActive && finding.RuleID == workloadhealth.RulePodAdmissionRejected && finding.PrimaryResource.Kind == "Namespace" {
			admission[FindingFingerprint(finding)] = true
		}
	}
	priority := func(match stageCandidate) bool {
		for _, target := range match.targets {
			if admission[target] {
				return true
			}
		}
		return false
	}
	// A playbook a firing detector named through a finding is tried before
	// the role-less playbooks every finding offers: the detector recognised
	// this cause, the others only did not rule it out.
	named := namedStageMatches(state.Request.InitialProgramRuns)
	sort.SliceStable(matches, func(i, j int) bool {
		if priority(matches[i]) != priority(matches[j]) {
			return priority(matches[i])
		}
		return named[stageAttemptKey(matches[i].PlaybookId, programBindingsState(matches[i].Bindings))] && !named[stageAttemptKey(matches[j].PlaybookId, programBindingsState(matches[j].Bindings))]
	})
	return matches
}

// namedStageMatches keys the matching diagnoses bound by a finding that names
// the playbook (binding source hint).
func namedStageMatches(runs []ProgramRun) map[string]bool {
	named := map[string]bool{}
	for _, run := range runs {
		if run.BindingSource == BindingSourceHint && run.Verdict.Result == program.Match {
			named[stageAttemptKey(run.ProgramID, run.Bindings)] = true
		}
	}
	return named
}

func updateStageFindings(state *IncidentState, response ToolResponse) {
	if state.Refresh {
		state.openingObservedActive = len(newRecoveryScope(openingKeys(&state.Request)).blocking(response.Findings)) > 0
	}
	// Failed or unconfirmed observations cannot erase the opening findings.
	if response.Error != "" && len(response.DetectorStates) == 0 {
		return
	}
	confirmed := map[string]bool{}
	for _, evaluation := range response.DetectorStates {
		if evaluation.Status == DetectorEvaluationClear || evaluation.Status == DetectorEvaluationFiring {
			confirmed[evaluation.DetectorID] = true
		}
	}
	findings := append([]sdk.Finding(nil), response.Findings...)
	seen := map[string]bool{}
	for _, finding := range findings {
		seen[FindingFingerprint(finding)] = true
	}
	for _, finding := range state.Findings {
		if !confirmed[finding.DetectorID] && !seen[FindingFingerprint(finding)] {
			findings = append(findings, finding)
		}
	}
	if state.afterWrite {
		// After a write the stage answers for what it was meant to clear: the
		// opening alerts. A clue or a later alert is not its work.
		findings = holdingFindings(newRecoveryScope(alertKeys(&state.Request)), findings)
	}
	sortFindings(findings)
	state.Findings = findings
	state.refreshUnexplained()
}

func recordedPlanContract(application RecordedPlanApplication) (*contracts.PlanApplication, error) {
	receipt, err := application.Result.Proto()
	if err != nil {
		return nil, fmt.Errorf("encode stage apply receipt: %w", err)
	}
	receipt.ApplicationId = application.ID
	receipt.ProgramIds = append([]string(nil), application.ProgramIDs...)
	receipt.StartedAt = optionalTimestamp(application.StartedAt)
	receipt.CompletedAt = optionalTimestamp(application.CompletedAt)
	receipt.Error = application.Error
	return repairContractText(receipt), nil
}

// Protobuf receipts contain oneofs; encoding/json cannot reconstruct them.
// This type owns that storage edge so all checkpoints use ProtoJSON.
type encodedResponseStageProgress struct {
	RecoveryDeadline   time.Time
	Report             json.RawMessage
	Coverage           json.RawMessage
	InitialProgramRuns []ProgramRun
	Complete           bool
}

func (progress responseStageProgress) MarshalJSON() ([]byte, error) {
	encoded := encodedResponseStageProgress{RecoveryDeadline: progress.RecoveryDeadline, InitialProgramRuns: progress.InitialProgramRuns, Complete: progress.Complete}
	if progress.Report != nil {
		raw, err := protojson.Marshal(progress.Report)
		if err != nil {
			return nil, err
		}
		encoded.Report = raw
	}
	if progress.Coverage != nil {
		raw, err := protojson.Marshal(progress.Coverage)
		if err != nil {
			return nil, err
		}
		encoded.Coverage = raw
	}
	return json.Marshal(encoded)
}
func (progress *responseStageProgress) UnmarshalJSON(raw []byte) error {
	var encoded encodedResponseStageProgress
	if err := json.Unmarshal(raw, &encoded); err != nil {
		return err
	}
	*progress = responseStageProgress{RecoveryDeadline: encoded.RecoveryDeadline, Complete: encoded.Complete, InitialProgramRuns: encoded.InitialProgramRuns}
	if len(encoded.Report) > 0 && string(encoded.Report) != "null" {
		progress.Report = &contracts.StageReport{}
		if err := protojson.Unmarshal(encoded.Report, progress.Report); err != nil {
			return err
		}
	}
	if len(encoded.Coverage) > 0 && string(encoded.Coverage) != "null" {
		progress.Coverage = &contracts.PlaybookCoverage{}
		if err := protojson.Unmarshal(encoded.Coverage, progress.Coverage); err != nil {
			return err
		}
	}
	return nil
}

func recordStageFindingProgress(attempt *contracts.StageMitigation, covered []string, findings []sdk.Finding) {
	remaining := map[string]bool{}
	for _, finding := range findings {
		remaining[FindingFingerprint(finding)] = true
	}
	attempt.RemainingFindingFingerprints = nil
	attempt.ClearedFindingFingerprints = nil
	for _, fp := range covered {
		if remaining[fp] {
			attempt.RemainingFindingFingerprints = append(attempt.RemainingFindingFingerprints, fp)
		} else {
			attempt.ClearedFindingFingerprints = append(attempt.ClearedFindingFingerprints, fp)
		}
	}
}

// Only a completed, independently verified application may discharge its exact
// diagnosed finding targets. A fresh detector still reporting that fingerprint wins.
func clearVerifiedStageTargets(state *IncidentState, attempt *contracts.StageMitigation, covered []string, observed ToolResponse) {
	receipt := attempt.GetApplyReceipt()
	if !receipt.GetApplied() || receipt.GetDryRun() || receipt.GetRolledBack() || receipt.GetRolloutPending() != nil || receipt.GetError() != "" || receipt.GetRollbackError() != "" || !receipt.GetVerificationCompleted() || !receipt.GetVerificationPassed() || attempt.GetError() != "" || attempt.GetPostApplyDiagnose().GetError() != "" || attempt.GetPostApplyDiagnose().GetProgramId() != attempt.GetPlaybookId() || attempt.GetPostApplyDiagnose().GetProgramRevision() != attempt.GetProgramRevision() || !programBindingsState(attempt.GetBindings()).Equal(programBindingsState(attempt.GetPostApplyDiagnose().GetBindings())) || attempt.GetPostApplyDiagnose().GetVerdict().GetResult() != contracts.Verdict_RESULT_NO_MATCH {
		return
	}
	clear := map[string]bool{}
	for _, fp := range covered {
		clear[fp] = true
	}
	for _, finding := range observed.Findings {
		delete(clear, FindingFingerprint(finding))
	}
	findings := make([]sdk.Finding, 0, len(state.Findings))
	for _, finding := range state.Findings {
		if !clear[FindingFingerprint(finding)] {
			findings = append(findings, finding)
		}
	}
	state.Findings = findings
	state.refreshUnexplained()
}

// Health observations after complete repairs may consume this wait, including
// opening traffic symptoms whose rolling windows still contain pre-repair errors.
// An incident finding that depends on a completely executed write and comes
// from a windowed detector may wait out that window once, counted from when the
// write took effect; any other incident finding, a failed or superseded repair,
// or a finding that outlives its window requires investigation.
// ResponderStepPanicError reports a panic inside one step that ran with the
// responder loop locks released. The step fails with this error; the
// controller keeps running.
type ResponderStepPanicError struct {
	Step  string
	Value any
	Stack string
}

func (e *ResponderStepPanicError) Error() string {
	return fmt.Sprintf("responder step %s panicked: %v", e.Step, e.Value)
}

// withoutResponderLocks runs one step with the responder loop mutex released,
// and operationMu too when operation is set, and always reacquires them. It is
// the only way to open an unlock window (TestResponderLoopLocksAreDeferredOrHelped):
// without it, a panic in the step unwinds into the caller's deferred unlocks of
// already released mutexes, an unrecoverable fatal error.
func withoutResponderLocks(loop *ResponderLoop, step string, operation bool, run func()) (err error) {
	loop.mu.Unlock()
	if operation {
		loop.operationMu.Unlock()
	}
	defer func() {
		if operation {
			loop.operationMu.Lock()
		}
		loop.mu.Lock()
		if r := recover(); r != nil {
			err = &ResponderStepPanicError{Step: step, Value: r, Stack: string(debug.Stack())}
		}
	}()
	run()
	return nil
}

// locked runs fn while holding the responder loop mutex.
func (l *ResponderLoop) locked(fn func()) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fn()
}

// stagePlanned is one freshly diagnosed and planned binding.
type stagePlanned struct {
	match    stageCandidate
	attempt  *contracts.StageMitigation
	bindings program.Bindings
	planID   string
	key      string
	// reach is what the plan writes or acts on: its written objects and, for
	// a plan with an effect beyond Kubernetes objects (data marks a data
	// repair, a command, a script), the objects it acts on and their
	// controller owner chain, so a repair on a Pod reaches the Deployment
	// that would replace it. unknown marks a write set that cannot be listed.
	reach   []sdk.ObjectRef
	data    bool
	unknown bool
}

const (
	// stageObjectShared skips, for this turn, a plan that reaches an object
	// an earlier plan of the same turn writes. The next turn diagnoses it afresh.
	stageObjectShared = "object_shared_in_turn"
	// stagePlannedInTurn marks a planned attempt whose batch has not applied it yet.
	stagePlannedInTurn = "planned_in_turn"
	// stageFindingsCleared skips planned attempts of a turn whose findings
	// all cleared before its batch applied.
	stageFindingsCleared = "findings_cleared"
)

// stageSkipRetryable reports a skip that wrote nothing and decided nothing
// about its binding: a restart before the binding's turn redoes it.
func stageSkipRetryable(reason string) bool {
	return reason == stageObjectShared || reason == stagePlannedInTurn
}

// stagePlanReach fills the planned binding's reach.
func (l *ResponderLoop) stagePlanReach(ctx context.Context, plan program.Plan, planned *stagePlanned) {
	refs, err := l.menuPlanWriteRefs(plan)
	planned.unknown = err != nil
	planned.data = planwire.HasNonObjectEffect(plan.Actions)
	planned.reach = slices.Clone(refs)
	if planned.data {
		planned.reach = l.recoveryOwnerRefs(ctx, append(planned.reach, planCaptureRefs(plan)...), nil)
	}
}

// reaches reports whether two planned bindings of one turn may touch the
// same object: an unknown write set reaches everything.
func (p stagePlanned) reaches(other stagePlanned) bool {
	if p.unknown || other.unknown {
		return true
	}
	for _, x := range p.reach {
		if slices.ContainsFunc(other.reach, func(y sdk.ObjectRef) bool { return sameResource(x, y) }) {
			return true
		}
	}
	return false
}

// applyBatch applies one turn's planned bindings back to back, source
// restores (plans that only write Kubernetes objects) before data repairs, and
// then verifies them once. Each binding keeps its own application, receipt and
// compensation boundary, so a failure part-way rolls back only its own plan.
// Each apply takes a single rollout sample, so the other bindings run while
// recovery progresses.
func (stage *DeterministicResponseStage) applyBatch(ctx context.Context, loop *ResponderLoop, state *IncidentState, batch []stagePlanned, save func(bool) error) error {
	batch = slices.Clone(batch)
	sort.SliceStable(batch, func(i, j int) bool { return !batch[i].data && batch[j].data })
	// An interrupted batch leaves no execution flags behind.
	defer func() { loop.batchVerified = nil }()
	applied := make([]ToolResponse, len(batch))
	var ids []string
	for index, item := range batch {
		plan := loop.plans[item.planID]
		item.attempt.PlanId = item.planID
		item.attempt.PlanActionCount = int32(len(plan.plan.Actions))
		// Persist intent before mutation. Interrupted writes are never replayed.
		item.attempt.Result, item.attempt.SkipReason = "apply_error", ""
		item.attempt.Error = "application interrupted before a completed receipt; inspect current state and recorded actions"
		if err := save(false); err != nil {
			return err
		}
		// PollRollouts performs one rollout sample while preserving the normal
		// source/direct-readback I/O bound. A tiny rollout budget must not truncate
		// those reads; no serial recovery wait runs inside this apply.
		// Apply through the ordinary tool boundary: mutation fences, recorded
		// writes, immutable receipts and compensation have one owner.
		if err := withoutResponderLocks(loop, "apply", true, func() {
			applied[index] = loop.Handle(ctx, ToolRequest{Command: "apply", PlanIDs: []string{item.planID}, stage: true, batchVerify: true, afterBatchWrite: index > 0, rolloutTimeout: stage.RolloutTimeout})
		}); err != nil {
			return err
		}
		// A write whose rollout is still pending is not verified now, as
		// before batching: recovery settlement observes it.
		if id, observed := stageApplicationID(loop, item.planID); observed {
			ids = append(ids, id)
		}
	}
	// The one verification is not a tool call of its own: the applies record
	// the writes, and each attempt records its own post-apply Diagnose.
	var verified ToolResponse
	if err := withoutResponderLocks(loop, "verify", true, func() {
		if len(ids) == 0 {
			return
		}
		verified = loop.Handle(ctx, ToolRequest{Command: "stage-verify", stage: true, isolatedRead: true, batchApplications: ids})
	}); err != nil {
		return err
	}
	wrote := false
	for index, item := range batch {
		var receipt *contracts.PlanApplication
		for _, application := range loop.applicationsLog {
			if len(application.PlanIDs) == 1 && application.PlanIDs[0] == item.planID {
				var err error
				if receipt, err = recordedPlanContract(application); err != nil {
					return err
				}
				item.attempt.TouchedObjects = touchedObjects(attemptApplication(application, item.attempt).Result)
			}
		}
		wrote = wrote || receipt.GetApplied()
		attempt := item.attempt
		attempt.Error = receipt.GetError()
		if receipt == nil {
			attempt.Error = applied[index].Error
		}
		attempt.ApplyReceipt = receipt
		for _, run := range verified.ProgramRuns {
			if run.ProgramID == item.match.PlaybookId && run.Bindings.Equal(item.bindings) {
				attempt.PostApplyDiagnose = programRunContract(run)
				break
			}
		}
	}
	state.afterWrite = state.afterWrite || wrote
	// With no verify (every write still rolling out) the findings stand.
	if len(ids) > 0 {
		updateStageFindings(state, verified)
	}
	for _, item := range batch {
		attempt := item.attempt
		// Scoped executor verification and the bound post-write diagnosis are
		// newer evidence than a retained opening incident finding. An incident
		// detector may not be installed in this loop's health-only registry.
		clearVerifiedStageTargets(state, attempt, item.match.targets, verified)
	}
	for _, item := range batch {
		attempt := item.attempt
		complete := false
		for _, application := range loop.applicationsLog {
			if len(application.PlanIDs) == 1 && application.PlanIDs[0] == item.planID {
				complete = PlanActionsComplete(attemptApplication(application, attempt).Result)
			}
		}
		recordStageFindingProgress(attempt, item.match.targets, state.Findings)
		switch {
		case attempt.ApplyReceipt.GetRolloutPending().GetReasonCode() == "admission_blocked":
			attempt.Result = "admission_blocked"
		case attempt.ApplyReceipt.GetRolloutPending() != nil:
			attempt.Result = "rollout_pending_at_handoff"
		case !attempt.ApplyReceipt.GetApplied() || attempt.ApplyReceipt.GetRolledBack() || !complete:
			attempt.Result = "apply_error"
		case attempt.Error != "":
			attempt.Result = "rollout_failed"
		default:
			attempt.Result = "applied_still_matching"
		}
	}
	return save(false)
}

// stageApplicationID is the application that consumed planID, and whether it
// wrote with its rollout observed, so a verify can judge it now.
func stageApplicationID(loop *ResponderLoop, planID string) (string, bool) {
	for _, application := range loop.applicationsLog {
		if slices.Contains(application.PlanIDs, planID) {
			return application.ID, application.Result.Applied && application.Result.RolloutPending == nil
		}
	}
	return "", false
}

// attemptApplication narrows a shared stage application to one attempt's
// actions, so the repair resources it reports belong to that binding alone.
func attemptApplication(application RecordedPlanApplication, attempt *contracts.StageMitigation) RecordedPlanApplication {
	count := int(attempt.GetPlanActionCount())
	if count == 0 {
		return application
	}
	start := int(attempt.GetPlanActionStart())
	window := func(length int) (int, int) { return min(start, length), min(start+count, length) }
	result := application.Result
	low, high := window(len(result.Plan.Actions))
	result.Plan.Actions = result.Plan.Actions[low:high]
	low, high = window(len(result.ActionsApplied))
	result.ActionsApplied = result.ActionsApplied[low:high]
	low, high = window(len(result.ActionReports))
	result.ActionReports = result.ActionReports[low:high]
	if start != 0 || count != len(application.Result.Plan.Actions) {
		refs := planCaptureRefs(result.Plan)
		result.ObjectEffects = nil
		for _, effect := range application.Result.ObjectEffects {
			if slices.Contains(refs, effect.Ref) {
				result.ObjectEffects = append(result.ObjectEffects, effect)
			}
		}
	}
	application.Result = result
	return application
}

// All admitted writes precede one shared recovery wait. Observation cannot
// select another write or replenish apply authority. Deadline survives restart.
// armRecovery publishes the controller's one fixed evaluation deadline at
// apply completion. Receipt timestamps supply the fallback for old checkpoints.
func (stage *DeterministicResponseStage) armRecovery(state *IncidentState, loop *ResponderLoop, report *contracts.StageReport, progress responseStageProgress) {
	if report.ApplyCompletedAt != nil {
		return
	}
	if !playbookWrote(report) {
		report.ApplyCompletedAt = contractTimestamp(loop.clock().UTC())
		report.RecoveryWait = &contracts.RecoveryWait{ExitReason: contracts.RecoveryWait_EXIT_REASON_NOTHING_APPLIED}
		return
	}
	completed := lastStageReceipt(report)
	report.ApplyCompletedAt = contractTimestamp(completed)
	state.syncAlertSet()
	deadline := progress.RecoveryDeadline
	if deadline.IsZero() {
		deadline = automaticRecoveryDeadline(state.Request, report, completed, loop.lastDiagnosisView, loop.Detectors, stage.WaitBound, stage.EstimateBound, stage.StabilityWindow)
	}
	report.RecoveryDeadline = contractTimestamp(deadline)
	// Legacy reports may have declared clearance before the controller proved it.
	report.RecoveryWait = nil
}

// automaticRecoveryDeadline is when the stage stops waiting for its writes to
// clear the alert set. The wait policy waits its whole budget. Otherwise the
// estimate is the slowest clearing horizon of the alert set's detectors (at
// least 90 s), plus the stability window and the written workloads' observed
// startup, and it never exceeds bound (response.after_playbook_wait): a slow
// probe or a long startup history does not hold the agent longer than the
// wait policy would. The controller counts it from the settle when a rollout
// took longer, and the response deadline caps it.
func automaticRecoveryDeadline(request IncidentRequest, report *contracts.StageReport, completed time.Time, view sdk.DetectionContext, detectors []sdk.Detector, wait, bound, stability time.Duration) time.Time {
	if completed.IsZero() {
		return completed
	}
	budget := wait
	if budget <= 0 {
		lag := 90 * time.Second
		var refs []sdk.ObjectRef
		for _, attempt := range report.GetMitigations() {
			for _, ref := range attempt.GetTouchedObjects() {
				refs = append(refs, sdk.ObjectRef{APIVersion: ref.GetApiVersion(), Kind: ref.GetKind(), Namespace: ref.GetNamespace(), Name: ref.GetName()})
			}
		}
		owners := map[string]bool{}
		for _, key := range alertKeys(&request) {
			id, _, _ := strings.Cut(key, "\x00")
			owners[id] = true
		}
		for _, detector := range detectors {
			if owners[detector.Spec().ID] || len(owners) == 0 && detector.Spec().Class == sdk.DetectorClassHealth {
				if view != nil {
					lag = max(lag, detectorClearingHorizon(detector, view))
				} else {
					clearing := max(detector.Spec().Persistence.Clearing, 1)
					lag = max(lag, time.Duration(clearing)*detector.Spec().Interval)
				}
			}
		}
		budget = lag + stability
		if view != nil {
			budget += sdk.ResourceStartupAllowance(view, refs...)
		}
		if bound > 0 {
			budget = min(budget, bound)
		}
	}
	deadline := completed.Add(budget)
	if !request.ResponseDeadline.IsZero() && request.ResponseDeadline.Before(deadline) {
		return request.ResponseDeadline
	}
	return deadline
}

func lastStageReceipt(report *contracts.StageReport) time.Time {
	var completed time.Time
	for _, attempt := range report.GetMitigations() {
		receipt := attempt.GetApplyReceipt()
		if !stageReceiptWrote(receipt) {
			continue
		}
		at := receipt.GetCompletedAt()
		if at == nil {
			at = receipt.GetStartedAt()
		}
		if at.IsValid() && at.AsTime().After(completed) {
			completed = at.AsTime()
		}
	}
	return completed
}

func holdingFindings(scope recoveryScope, findings []sdk.Finding) []sdk.Finding {
	var held []sdk.Finding
	for _, finding := range findings {
		if scope.holds(finding) {
			held = append(held, finding)
		}
	}
	return held
}

// writeVerifiedForSettling reports an applied, unreverted write whose scoped verification passed and
// whose program no longer matches, so any remaining finding is waiting on detector observation.
// A confirmed non-object effect counts as verified: no object read can verify it.
// stageDiagnoseTries is how many times the stage asks a program to diagnose
// before an Unknown verdict skips it.
const stageDiagnoseTries = 3

// stageDiagnoseBackoff is the pause between those tries.
const stageDiagnoseBackoff = time.Second

func (stage *DeterministicResponseStage) diagnoseBackoff(ctx context.Context) error {
	if stage.diagnoseWait != nil {
		return stage.diagnoseWait(ctx, stageDiagnoseBackoff)
	}
	timer := lifecycleio.NewTimer(stageDiagnoseBackoff)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// reaskUnanswered asks again, up to stageDiagnoseTries diagnoses in all, for
// every candidate whose diagnosis failed: an Unknown carrying a failure (a read
// that did not answer, a deadline, an unreachable memory host). The stage's
// candidates are the Matches of this diagnosis, so such an Unknown at incident
// open (an exec that ran out of time, a memory host busy with the opening
// capture) would otherwise leave the stage nothing to apply for the whole
// incident. An Unknown the program answered itself is final. Only the failed
// programs are diagnosed again; the failed runs stay as evidence and an answer
// is appended after them. No re-ask starts once until (non-zero) has passed.
func (stage *DeterministicResponseStage) reaskUnanswered(ctx context.Context, loop *ResponderLoop, state *IncidentState, runs []ProgramRun, until time.Time) ([]ProgramRun, error) {
	for try := 1; try < stageDiagnoseTries && (until.IsZero() || loop.clock().Before(until)); try++ {
		unanswered := unansweredRuns(runs)
		if len(unanswered) == 0 {
			return runs, nil
		}
		ids := map[string]bool{}
		for _, id := range unanswered {
			ids[id] = true
		}
		if err := stage.diagnoseBackoff(ctx); err != nil {
			return runs, err
		}
		again := loop.RediagnosePrograms(ctx, ids)
		fillProgramRevisionsWith(loop.gitRead(), ctx, again, state.Request.RepositoryWorktree, state.Request.SourceCommit)
		for _, run := range again {
			if _, ok := unanswered[stageAttemptKey(run.ProgramID, run.Bindings)]; ok {
				runs = append(runs, run)
			}
		}
	}
	return runs, nil
}

// unansweredRuns maps each candidate (program and binding) whose every run is
// a failed diagnosis to its program ID.
func unansweredRuns(runs []ProgramRun) map[string]string {
	unanswered := map[string]string{}
	answered := map[string]bool{}
	for _, run := range runs {
		key := stageAttemptKey(run.ProgramID, run.Bindings)
		if diagnosisFailed(run.Verdict) {
			unanswered[key] = run.ProgramID
		} else {
			answered[key] = true
		}
	}
	for key := range answered {
		delete(unanswered, key)
	}
	return unanswered
}

// diagnosisFailed reports an Unknown that is a failed diagnosis rather than the
// program's answer: RunDiagnose and the shared candidate deadline mark one with
// a resource-less "diagnosis" fact (a failed read, a Diagnose error, a deadline).
func diagnosisFailed(v program.Verdict) bool {
	if v.Result != program.Unknown {
		return false
	}
	for _, e := range v.Evidence {
		if e.Resource.Kind == "" && e.Fact == "diagnosis" {
			return true
		}
	}
	return false
}

// diagnosisFailure is the text an Unknown verdict carries about why the
// diagnosis could not be made.
func diagnosisFailure(v program.Verdict) string {
	for _, e := range v.Evidence {
		if e.Observed != "" {
			return "diagnosis unavailable: " + e.Observed
		}
	}
	return "diagnosis unavailable"
}

// readPlaybookDetectors gives the opening diagnosis, and the responder's task,
// a current reading of every detector that names a learned playbook. Such a
// finding is what binds that playbook (or, for one with no program, shows it to
// the responder), and the incident opens on whichever detector fired first: a
// slower detector (a store read, a long interval) may not have evaluated since
// the fault began. Each one not already firing is evaluated on demand against
// a fresh read of the pod reads it may use (freshPodReads): its own, and the
// store-state reads every detector shares. A detector whose reads did not all
// answer, or that failed, is asked again after a backoff. All of it ends by
// until; a detector with no answer by then keeps the findings the incident
// opened with. With no detector that names a learned playbook, nothing is read
// and nothing waits.
func (stage *DeterministicResponseStage) readPlaybookDetectors(ctx context.Context, loop *ResponderLoop, state *IncidentState, until time.Time) {
	pending := loop.playbookDetectors(state.Findings)
	if len(pending) > 0 {
		var m appmap.Map
		if state.Request.SourceCommit != "" {
			m, _ = acceptedVerificationMap(ctx, loop.gitRead(), state.Request)
		}
		var credits ProgramCredits
		loop.locked(func() { credits = loop.Credits })
		pending = detectorsOnFailingPaths(pending, credits, fixedReachPlaybooks(loop.Programs), m, state.Request)
	}
	if len(pending) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, until.Sub(loop.clock()))
	defer cancel()
	response := ToolResponse{Findings: []sdk.Finding{}}
	for {
		pending = loop.readDetectorsOnce(ctx, pending, &response)
		if len(pending) == 0 || ctx.Err() != nil || !loop.clock().Before(until) || stage.diagnoseBackoff(ctx) != nil {
			break
		}
	}
	for _, detector := range pending {
		slog.Info("stage_playbook_detector_unanswered", "incident_id", state.Request.IncidentID, "detector_id", detector.Spec().ID)
	}
	if len(response.DetectorStates) == 0 {
		return
	}
	updateStageFindings(state, response)
	// The incident's findings are what the stage repairs and the responder is
	// told about, so a playbook's answer joins them as if it had fired first.
	// The alert set is unchanged: recovery still answers to the opening alerts.
	known := map[string]bool{}
	for _, finding := range state.Request.Findings {
		known[FindingFingerprint(finding)] = true
	}
	for _, finding := range response.Findings {
		if !known[FindingFingerprint(finding)] {
			state.Request.Findings = append(state.Request.Findings, finding)
		}
	}
	loop.locked(func() {
		loop.findings = append([]sdk.Finding(nil), state.Findings...)
		loop.Request.Findings = append([]sdk.Finding(nil), state.Request.Findings...)
	})
}

// detectorsOnFailingPaths keeps the detectors whose learned playbooks may fix
// a component on the path of an opening user test: the path of each opening
// user test's operations in the accepted map, and the components each
// playbook was credited for (CreditKey). Credits record past fixes, not
// reach, so they bound a playbook only when its program has a fixed reach (no
// object parameter, see fixedReachPlaybooks). A detector is left out only when
// all of that is known and none of its playbooks' credited components is on
// any such path: its playbooks fixed something else, so its fresh reading
// cannot bind a playbook for this incident and the stage does not wait for it.
// A missing map, an opening alert that is not a mapped user test, a playbook
// with no credit or one whose program takes an object parameter keeps the
// detector, as before. A credit pair needs a bound component role, so a
// role-less playbook earns none and is always kept: this skip only acts on
// credits that predate that rule.
func detectorsOnFailingPaths(detectors []sdk.Detector, credits ProgramCredits, fixedReach map[string]bool, m appmap.Map, request IncidentRequest) []sdk.Detector {
	if len(m.Components) == 0 {
		return detectors
	}
	pm := m.Program()
	onPath := map[string]bool{}
	for _, key := range openingKeys(&request) {
		_, scenario, ok := openingUserTest(request, key)
		if !ok {
			return detectors
		}
		var operations []string
		for _, probe := range m.Probes {
			if probe.Scenario == scenario {
				operations = append(operations, probe.Operations...)
			}
		}
		components, ok := operationComponents(m, pm, operations)
		if !ok {
			return detectors
		}
		for _, component := range components {
			if c, found := pm.Component(component); found {
				onPath[c.Workload] = true
			}
		}
	}
	if len(onPath) == 0 {
		return detectors
	}
	var kept []sdk.Detector
	for _, detector := range detectors {
		if detectorMayBindOnPath(detector.Spec(), credits, fixedReach, onPath) {
			kept = append(kept, detector)
		}
	}
	return kept
}

// detectorMayBindOnPath reports whether a playbook the detector names has an
// unknown reach (no credit, or a program that is not of fixed reach) or was
// credited for a workload on the path.
func detectorMayBindOnPath(spec sdk.DetectorSpec, credits ProgramCredits, fixedReach map[string]bool, onPath map[string]bool) bool {
	for _, reference := range spec.Playbooks {
		id, err := layout.PlaybookID(reference)
		if err != nil || id == layout.HealthObjectivePlaybook || id == SourceRestoreID {
			continue
		}
		if !credits.Has(id) || !fixedReach[id] {
			return true
		}
		for _, key := range credits[id] {
			if onPath[key.Component.Kind+"/"+key.Component.Name] {
				return true
			}
		}
	}
	return false
}

// fixedReachPlaybooks are the registered programs that declare no object
// parameter: they act on resources they name themselves, so the components
// they were credited for are where they can act. A program with an object
// parameter binds whatever resource a finding names.
func fixedReachPlaybooks(programs program.ProgramRegistry) map[string]bool {
	fixed := map[string]bool{}
	if programs == nil {
		return fixed
	}
	for _, spec := range programs.List() {
		if !slices.ContainsFunc(spec.Params, func(p program.ParamSpec) bool { return p.ValueKind == "" }) {
			fixed[spec.ID] = true
		}
	}
	return fixed
}

// playbookDetectors are the loop's detectors that name a learned playbook and
// have no active finding among findings. A learned playbook is a registered
// program, or any playbook a learned detector names (one with no program, a
// README with scripts, is still the responder's best lead). The generic
// health-objective playbook is not one.
func (l *ResponderLoop) playbookDetectors(findings []sdk.Finding) []sdk.Detector {
	programs := map[string]bool{}
	if l.Programs != nil {
		for _, spec := range l.Programs.List() {
			if spec.ID != SourceRestoreID {
				programs[spec.ID] = true
			}
		}
	}
	firing := map[string]bool{}
	for _, finding := range findings {
		if finding.Status == sdk.FindingActive {
			firing[finding.DetectorID] = true
		}
	}
	var detectors []sdk.Detector
	for _, detector := range l.Detectors {
		spec := detector.Spec()
		if firing[spec.ID] {
			continue
		}
		for _, reference := range spec.Playbooks {
			id, err := layout.PlaybookID(reference)
			if err != nil || id == layout.HealthObjectivePlaybook || id == SourceRestoreID {
				continue
			}
			if programs[id] || spec.Class == sdk.DetectorClassLearned {
				detectors = append(detectors, detector)
				break
			}
		}
	}
	return detectors
}

// detectorPodReads reports whether a pod read may feed detector: one it
// declares, or a store-state read, which every detector may consult through
// storestate.Changes.
func detectorPodReads(detector sdk.Detector) func(podread.Query) bool {
	declared := map[string]bool{}
	if consumer, ok := detector.(podread.Consumer); ok {
		for _, query := range consumer.PodReads() {
			declared[query.ID] = true
		}
	}
	return func(query podread.Query) bool {
		_, _, store := storestate.ParseQueryID(query.ID)
		return store || declared[query.ID]
	}
}

// readDetectorsOnce evaluates detectors against one snapshot whose pod reads
// they may use are read now, and adds each answer to response. It returns the
// detectors that did not answer: failed, or with a read left unread this time.
// It holds both loop locks, as the opening diagnosis does, so it runs between
// tool calls and never beside a write.
func (l *ResponderLoop) readDetectorsOnce(ctx context.Context, detectors []sdk.Detector, response *ToolResponse) []sdk.Detector {
	ctx, closeMemory := withMemoryScope(ctx)
	defer closeMemory()
	wants := make([]func(podread.Query) bool, len(detectors))
	for i, detector := range detectors {
		wants[i] = detectorPodReads(detector)
	}
	fresh := &freshPodReads{want: func(query podread.Query) bool {
		return slices.ContainsFunc(wants, func(want func(podread.Query) bool) bool { return want(query) })
	}}
	l.operationMu.Lock()
	defer l.operationMu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	view, err := l.snapshotView(withFreshPodReads(ctx, fresh))
	if err != nil {
		return detectors
	}
	var unanswered, answering []sdk.Detector
	for i, detector := range detectors {
		if fresh.unreadAny(wants[i]) {
			unanswered = append(unanswered, detector)
		} else {
			answering = append(answering, detector)
		}
	}
	results := evaluateAdmittedDetectors(ctx, answering, view)
	for _, detector := range answering {
		id := detector.Spec().ID
		result := results[id]
		if result.err != nil {
			unanswered = append(unanswered, detector)
			continue
		}
		evaluation := DetectorEvaluation{DetectorID: id, EvaluatedAt: l.clock().UTC(), Status: DetectorEvaluationClear}
		for _, finding := range result.findings {
			if finding.Status == sdk.FindingActive {
				response.Findings = append(response.Findings, finding)
				evaluation.Status = DetectorEvaluationFiring
				evaluation.Fingerprints = append(evaluation.Fingerprints, FindingFingerprint(finding))
			}
		}
		if evaluation.Status == DetectorEvaluationClear && !detectorConfirmed(detector, view) {
			evaluation.Status = DetectorEvaluationPending
		}
		response.DetectorStates = append(response.DetectorStates, evaluation)
	}
	return unanswered
}
