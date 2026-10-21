package runtime

import (
	"context"
	"sort"
	"strings"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// observeProgramRuns is shared by watch events, sweeps, confirmations and
// retained-incident verification. Only tracker activations enter the batcher.
func (c *Controller) observeProgramRuns(now time.Time, event *sdk.DriftEvent, runs []ProgramRun) {
	for _, run := range runs {
		id := "drift." + run.ProgramID
		var prior *FindingState
		for _, state := range c.tracker.states {
			if state.DetectorID == id && bindingKey(state.Finding.ParameterBindings) == bindingKey(run.Bindings) {
				prior = state
				break
			}
		}
		// A binding is sampled once per controller step, even if an event and
		// a sweep or retained-incident check select it in that same step.
		if prior != nil && prior.LastObservedAt.Equal(now) {
			continue
		}
		if run.Verdict.Result != program.Match && run.Verdict.Result != program.NoMatch {
			if prior != nil {
				prior.LastObservedAt = now
				prior.ClearCount = 0
				prior.FirstClearAt = time.Time{}
			}
			continue // Inconclusive observations interrupt, never confirm, clearance.
		}
		var finding sdk.Finding
		if run.Verdict.Result == program.Match {
			observation := event
			if observation == nil {
				refs := make([]string, 0, len(run.Bindings))
				for name := range run.Bindings {
					refs = append(refs, name)
				}
				sort.Strings(refs)
				if len(refs) == 0 {
					continue
				}
				observation = &sdk.DriftEvent{Ref: run.Bindings[refs[0]], Source: "periodic program sweep", ObservedAt: now.UTC(), Classification: sdk.DriftFault, ProgramID: run.ProgramID}
			}
			finding = c.programFinding(*observation, run)
		} else if prior != nil {
			finding = prior.Finding
		} else {
			continue
		}
		policy := c.drift.ProgramPersistence
		c.tracker.SetPolicy(id, policy.Firing, policy.Clearing, policy.MinDuration)
		changes := c.tracker.ObserveBinding(now, finding, run.Verdict.Result == program.Match)
		c.batcher.RemoveKeys(changes.Cleared)
		active := c.tracker.ActiveFindings([]string{id}, nil)
		c.recordEvaluation(id, now, active)
		c.recordFiring(sdk.DetectorSpec{ID: id, Class: sdk.DetectorClassIncident, Owner: sdk.DetectorOwnerResponder}, now, active, changes)
		for _, activated := range changes.Activated {
			if !c.attachDrift(activated) {
				c.batcher.AddAtWithDebounce(activated, now, 0)
			}
		}
	}
}

func (c *Controller) programConfirmationAt(state FindingState) time.Time {
	if c.drift == nil || c.drift.Programs == nil || !strings.HasPrefix(state.DetectorID, "drift.") || state.DetectorID == "drift.unexplained" {
		return time.Time{}
	}
	var first time.Time
	if !state.Active && state.FiringCount > 0 {
		first = state.FirstSeenAt
	} else if state.Active && state.ClearCount > 0 {
		first = state.FirstClearAt
	} else {
		return time.Time{}
	}
	due := first.Add(c.drift.ProgramPersistence.MinDuration)
	if !state.LastObservedAt.Before(due) {
		interval := c.config.ConfirmationInterval
		if interval <= 0 {
			interval = time.Second
		}
		due = state.LastObservedAt.Add(interval)
	}
	return due
}

func (c *Controller) nextProgramConfirmation() time.Time {
	var next time.Time
	for _, state := range c.tracker.Snapshot() {
		at := c.programConfirmationAt(state)
		if !at.IsZero() && (next.IsZero() || at.Before(next)) {
			next = at
		}
	}
	return next
}

func (c *Controller) confirmPrograms(ctx context.Context, now time.Time, view program.ReadView) {
	if c.drift.Programs == nil {
		return
	}
	states := c.tracker.Snapshot()
	keys := make([]string, 0, len(states))
	for key, state := range states {
		at := c.programConfirmationAt(state)
		if !at.IsZero() && !now.Before(at) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	ctx, cancel := context.WithTimeout(ctx, c.drift.Deadline)
	defer cancel()
	if live, ok := view.(LiveReadView); ok {
		live.Context = ctx
		view = live
	}
	for _, key := range keys[:min(len(keys), DefaultSweepLimit)] {
		state := states[key]
		id := strings.TrimPrefix(state.DetectorID, "drift.")
		diagnoser, ok := c.drift.Programs.Diagnoser(id)
		if !ok {
			c.tracker.states[key].LastObservedAt = now
			continue
		}
		run := ProgramRun{ProgramID: id, Bindings: state.Finding.ParameterBindings, BindingSource: "hint"}
		run.Verdict = program.RunDiagnose(ctx, diagnoser, view, run.Bindings, c.drift.Deadline)
		c.observeProgramRuns(now, nil, []ProgramRun{run})
	}
}
