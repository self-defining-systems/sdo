package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"time"

	"sdo.dev/operator/sdk/program"
)

// A program's repair may depend on what the cluster does after its write, such
// as a rollout that cannot progress past old-template pods. After a pending
// rollout the runtime re-runs the same mitigator on fresh observations and
// applies its next plan until it plans no change, bounded by steps and time.
const (
	MaxProgramFollowUps            = 3
	DefaultProgramFollowUpSettle   = 5 * time.Second
	DefaultPendingApplicationStall = DefaultMitigationRolloutTimeout
)

func (l *ResponderLoop) programFollowUp(ctx context.Context, entry menuEntry, bindings program.Bindings, last program.Plan, used int) (program.Plan, bool) {
	if used >= MaxProgramFollowUps || entry.action != nil || entry.removal != nil || entry.programID == "" || l.Programs == nil {
		return program.Plan{}, false
	}
	mitigator, ok := l.Programs.Mitigator(entry.programID)
	if !ok {
		return program.Plan{}, false
	}
	refs, err := l.menuPlanWriteRefs(last)
	if err != nil {
		return program.Plan{}, false
	}
	// Give the rollout one bounded chance to show its effect. A rollout that
	// completes here settles through the ordinary pending verification.
	executor := l.Executor
	executor.PollRollouts = false
	executor.RolloutTimeout = l.ProgramFollowUpSettle
	if executor.RolloutTimeout <= 0 {
		executor.RolloutTimeout = DefaultProgramFollowUpSettle
	}
	if _, err := executor.waitForRollouts(ctx, l.Request, refs); err == nil {
		return program.Plan{}, false
	}
	view, err := l.snapshotView(ctx)
	if err != nil {
		return program.Plan{}, false
	}
	deadline := l.deadline()
	var next program.Plan
	if stepErr := withoutResponderLocks(l, "follow-up mitigator", false, func() {
		next, err = runBoundedMitigator(ctx, mitigator, view, cloneProgramBindings(bindings), deadline)
	}); stepErr != nil {
		err = stepErr
	}
	if err != nil || samePlanActions(next, last) {
		// An empty or repeated plan means the program has nothing more to do.
		return program.Plan{}, false
	}
	return next, true
}

func samePlanActions(a, b program.Plan) bool {
	left, leftErr := json.Marshal(PlanApplicationResult{Plan: program.Plan{Actions: a.Actions}})
	right, rightErr := json.Marshal(PlanApplicationResult{Plan: program.Plan{Actions: b.Actions}})
	return leftErr == nil && rightErr == nil && bytes.Equal(left, right)
}
