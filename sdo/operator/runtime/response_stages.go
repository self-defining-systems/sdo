package runtime

import (
	"context"
	"fmt"
	"slices"
	"sort"

	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

// ResponseStage operates on current evidence and reports its own actions. It
// neither chooses a later stage nor grants authority to close an incident.
type ResponseStage interface {
	Name() string
	Run(context.Context, *IncidentState) (*contracts.StageReport, error)
}

// IncidentState is local to one ordered response attempt. Request retains the
// opening evidence; Findings and UnexplainedFindings describe remaining work.
type IncidentState struct {
	Request             IncidentRequest
	Findings            []sdk.Finding
	Coverage            *contracts.PlaybookCoverage
	PriorStages         []*contracts.StageReport
	UnexplainedFindings []sdk.Finding
	Result              *IncidentResult
	RepairActions       []RepairActionReceipt
	ProgramToolCalls    []ToolCall
	// Refresh marks a later pass over an incident whose opening pass already
	// ran: Findings are re-observed and coverage is rebuilt, and attempts the
	// stage recorded earlier are never repeated.
	Refresh bool

	// LiveAlertSet reads the user tests that joined the incident since the
	// request was frozen; a stage that outlives a join judges the whole alert
	// set, not the set it started with.
	LiveAlertSet func() (joinedKeys []string, joined []sdk.Finding)
	// afterWrite is set once a playbook of this stage wrote: from then on the
	// stage observes only the incident's opening alerts.
	afterWrite            bool
	openingObservedActive bool
}

// RunResponseStages stops when no findings remain. Every attempted stage has a
// report, including errors, so subsequent workers never infer success by silence.
func RunResponseStages(ctx context.Context, state *IncidentState, stages []ResponseStage) error {
	state.refreshUnexplained()
	for _, stage := range stages {
		// A page is work even when no finding accompanies it.
		page := pageOpened(&state.Request)
		if len(state.Findings) == 0 && !page {
			break
		}

		before := findingFingerprints(state.Findings)
		report, err := stage.Run(ctx, state)
		if report == nil {
			report = &contracts.StageReport{}
		}
		state.refreshUnexplained()
		report.Stage = stage.Name()
		remaining := map[string]bool{}
		for _, fp := range findingFingerprints(state.Findings) {
			remaining[fp] = true
		}
		report.ClearedFindingFingerprints = nil
		for _, fp := range before {
			if !remaining[fp] {
				report.ClearedFindingFingerprints = append(report.ClearedFindingFingerprints, fp)
			}
		}
		report.RemainingFindingFingerprints = nil
		for _, finding := range state.Findings {
			report.RemainingFindingFingerprints = append(report.RemainingFindingFingerprints, FindingFingerprint(finding))
		}
		if err != nil {
			report.Error = err.Error()
		}
		state.PriorStages = append(state.PriorStages, report)
		// A failed attempt that wrote nothing must not hold up the page's responder.
		if err != nil && page && ctx.Err() == nil && !playbookWrote(report) {
			continue
		}
		if err != nil {
			return fmt.Errorf("response stage %s: %w", stage.Name(), err)
		}
	}
	return nil
}

// RunStageAgain runs a stage a second (or later) time over an incident whose
// earlier pass is already in state.PriorStages. The stage's one report is
// updated in place of the earlier one: its attempts keep their order and
// receipts, new attempts follow, and clearance accumulates.
func RunStageAgain(ctx context.Context, state *IncidentState, stage ResponseStage) (*contracts.StageReport, error) {
	var earlier *contracts.StageReport
	index := -1
	for i, prior := range state.PriorStages {
		if prior.GetStage() == stage.Name() {
			earlier, index = prior, i
		}
	}
	state.Refresh = true
	defer func() { state.Refresh = false }()
	report, err := stage.Run(ctx, state)
	if report == nil {
		report = &contracts.StageReport{}
	}
	state.refreshUnexplained()
	report.Stage = stage.Name()
	remaining := map[string]bool{}
	report.RemainingFindingFingerprints = nil
	for _, finding := range state.Findings {
		fp := FindingFingerprint(finding)
		remaining[fp] = true
		report.RemainingFindingFingerprints = append(report.RemainingFindingFingerprints, fp)
	}
	cleared := map[string]bool{}
	for _, fp := range earlier.GetClearedFindingFingerprints() {
		cleared[fp] = true
	}
	for _, fp := range findingFingerprints(state.Request.Findings) {
		if !remaining[fp] {
			cleared[fp] = true
		}
	}
	report.ClearedFindingFingerprints = nil
	for fp := range cleared {
		report.ClearedFindingFingerprints = append(report.ClearedFindingFingerprints, fp)
	}
	sort.Strings(report.ClearedFindingFingerprints)
	if err != nil {
		report.Error = err.Error()
	}
	if index >= 0 {
		report = mergeStagePass(earlier, report)
		state.PriorStages[index] = report
	} else {
		state.PriorStages = append(state.PriorStages, report)
	}
	if err != nil {
		return report, fmt.Errorf("response stage %s: %w", stage.Name(), err)
	}
	return report, nil
}

// mergeStagePass preserves earlier receipts even when a retry fails before
// rebuilding its report. A first write earns the normal recovery deadline.
func mergeStagePass(earlier, report *contracts.StageReport) *contracts.StageReport {
	if earlier.GetStartedAt() != nil {
		report.StartedAt = earlier.StartedAt
	}
	if playbookWrote(earlier) || !playbookWrote(report) {
		if earlier.GetApplyCompletedAt() != nil {
			report.ApplyCompletedAt = earlier.ApplyCompletedAt
		}
		if earlier.GetRecoveryWait() != nil {
			report.RecoveryWait = earlier.RecoveryWait
		}
	}
	if earlier.GetRecoveryDeadline() != nil {
		report.RecoveryDeadline = earlier.RecoveryDeadline
	}
	if earlier.GetStabilityDeadline() != nil {
		report.StabilityDeadline = earlier.StabilityDeadline
	}
	if earlier.GetHandoffSelected() {
		report.HandoffSelected, report.HandoffKeys, report.HandoffState = true, earlier.HandoffKeys, earlier.HandoffState
	}
	report.Diagnoses = mergeStageDiagnoses(earlier.GetDiagnoses(), report.Diagnoses)
	byBinding := map[string]int{}
	fresh := report.Mitigations
	report.Mitigations = slices.Clone(earlier.GetMitigations())
	for i, attempt := range report.Mitigations {
		byBinding[stageAttemptKey(attempt.PlaybookId, programBindingsState(attempt.Bindings))] = i
	}
	for _, attempt := range fresh {
		key := stageAttemptKey(attempt.PlaybookId, programBindingsState(attempt.Bindings))
		if i, ok := byBinding[key]; ok {
			previous := report.Mitigations[i]
			oldReceipt, newReceipt := previous.GetApplyReceipt(), attempt.GetApplyReceipt()
			if oldReceipt != nil && (newReceipt == nil ||
				stageReceiptWrote(oldReceipt) && !stageReceiptWrote(newReceipt) ||
				controllerTime(oldReceipt.GetCompletedAt()).After(controllerTime(newReceipt.GetCompletedAt()))) {
				continue
			}
			attempt.RolloutUnsettledAtDeadline = attempt.GetRolloutUnsettledAtDeadline() || previous.GetRolloutUnsettledAtDeadline()
			report.Mitigations[i] = attempt
		} else {
			byBinding[key] = len(report.Mitigations)
			report.Mitigations = append(report.Mitigations, attempt)
		}
	}
	return report
}

func cloneStageReports(reports []*contracts.StageReport) []*contracts.StageReport {
	var result []*contracts.StageReport
	for _, report := range reports {
		result = append(result, proto.Clone(report).(*contracts.StageReport))
	}
	return result
}

func (state *IncidentState) refreshUnexplained() {
	explained := map[string]bool{}
	for _, match := range state.Coverage.GetMatches() {
		for _, fp := range match.ExplainedFindingFingerprints {
			explained[fp] = true
		}
	}
	state.UnexplainedFindings = nil
	for _, finding := range state.Findings {
		if !explained[FindingFingerprint(finding)] {
			state.UnexplainedFindings = append(state.UnexplainedFindings, finding)
		}
	}
}

// syncAlertSet folds user tests that joined the incident while the stage ran
// into the state's request and findings, so Recovered is judged on the whole
// alert set.
func (s *IncidentState) syncAlertSet() {
	if s.LiveAlertSet == nil {
		return
	}
	keys, joined := s.LiveAlertSet()
	fresh := map[string]bool{}
	for _, key := range keys {
		if !slices.Contains(s.Request.JoinedKeys, key) {
			s.Request.JoinedKeys = append(s.Request.JoinedKeys, key)
			fresh[key] = true
		}
	}
	sort.Strings(s.Request.JoinedKeys)
	for _, finding := range joined {
		fp := FindingFingerprint(finding)
		if !slices.ContainsFunc(s.Request.Findings, func(f sdk.Finding) bool { return FindingFingerprint(f) == fp }) {
			s.Request.Findings = append(s.Request.Findings, finding)
		}
		if fresh[findingKey(finding)] && finding.Status == sdk.FindingActive && !slices.ContainsFunc(s.Findings, func(f sdk.Finding) bool { return FindingFingerprint(f) == fp }) {
			s.Findings = append(s.Findings, finding)
		}
	}
	sortFindings(s.Request.Findings)
	sortFindings(s.Findings)
}
