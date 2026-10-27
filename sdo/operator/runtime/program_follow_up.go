package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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

// programFollowUp returns the next plan, or false when the program has nothing
// new to do. A plan repeating any earlier round (A/B/A included) stops the loop.
func (l *ResponderLoop) programFollowUp(ctx context.Context, entry menuEntry, bindings program.Bindings, earlier []program.Plan) (program.Plan, bool, error) {
	if len(earlier) == 0 || len(earlier) > MaxProgramFollowUps || entry.action != nil || entry.removal != nil || entry.programID == "" || l.Programs == nil {
		return program.Plan{}, false, nil
	}
	mitigator, ok := l.Programs.Mitigator(entry.programID)
	if !ok {
		return program.Plan{}, false, nil
	}
	refs, err := l.menuPlanWriteRefs(earlier[len(earlier)-1])
	if err != nil {
		return program.Plan{}, false, nil
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
		return program.Plan{}, false, nil
	}
	view, err := l.snapshotView(ctx)
	if err != nil {
		return program.Plan{}, false, nil
	}
	deadline := l.deadline()
	var next program.Plan
	if stepErr := withoutResponderLocks(l, "follow-up mitigator", false, func() {
		next, err = BuildMitigationPlan(ctx, mitigator, view, cloneProgramBindings(bindings), deadline)
	}); stepErr != nil {
		err = stepErr
	}
	if errors.Is(err, errEmptyMitigationPlan) {
		return program.Plan{}, false, nil
	}
	if err != nil {
		l.addFailureSignal("mitigator_failure:" + entry.programID)
		return program.Plan{}, false, err
	}
	for _, plan := range earlier {
		if samePlanActions(next, plan) {
			return program.Plan{}, false, nil
		}
	}
	return next, true, nil
}

func samePlanActions(a, b program.Plan) bool {
	left, leftErr := json.Marshal(PlanApplicationResult{Plan: program.Plan{Actions: a.Actions}})
	right, rightErr := json.Marshal(PlanApplicationResult{Plan: program.Plan{Actions: b.Actions}})
	return leftErr == nil && rightErr == nil && bytes.Equal(left, right)
}
