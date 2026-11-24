package bench

import (
	"context"
	"fmt"
	"time"
)

// hostLost handles a host that has not answered since `since`. Inside hostGrace nothing happens (an ssh blip must not
// fail a run). Past it the host's steps are given up as lost "host unreachable": the item they served goes back to the
// queue with no failure counted against it (the host failed, not the item), so another host takes it when the spec
// allows one and it waits otherwise; an alarm is raised once; and the lane keeps the step as an orphan to settle
// when the host answers again.
func (m *Manager) hostLost(host string, since time.Time, cause error) {
	if m.now().Sub(since) < hostGrace {
		return
	}
	m.alarm(AlarmHostUnreachable, host, fmt.Sprintf("no answer since %s: %v", since.Format(time.RFC3339), clip(cause.Error(), 200)))
	for _, l := range m.sortedLanes() {
		if l.Host != host || l.State == LaneDown || l.State == LaneRetired || l.State == LaneUnreachable && l.Step == nil && l.Seq == nil {
			continue
		}
		if l.Step == nil && l.Seq == nil && l.Orphan == nil {
			continue // a free lane: nothing to give up; admission already refuses the host
		}
		reason := fmt.Sprintf("host unreachable since %s: %s", since.Format(time.RFC3339), clip(cause.Error(), 160))
		if l.Step != nil {
			m.logf("lane %s: step %s %s (not counted as a failure of its item)", l.Lane, l.Step.ID, reason)
			l.Orphan, l.Step = l.Step, nil
			if l.Orphan.Kind == StepBuild && l.Prepared == nil {
				// The build may have prepared its operator before the host went away: recover tears it down.
				if x := m.exps[l.Experiment]; x != nil {
					l.Prepared = m.preparedFor(l, x)
				}
			}
		}
		l.Experiment, l.Seq, l.Attempt, l.Position = "", nil, 0, 0
		m.setState(l, LaneUnreachable, reason)
	}
}

// settleOrphan deals with a step that was given up when its host went away, now that the host answers and was
// admitted: a step still running is killed (its result is discarded: the item runs elsewhere), a settled one is
// cleaned up, and then the lane is recovered before anything else uses it.
func (m *Manager) settleOrphan(ctx context.Context, l *LaneFile, state string) {
	o := l.Orphan
	ops := m.ops(l.Host)
	if state == StepRunning {
		m.logf("lane %s: orphaned step %s still runs on %s: killing it", l.Lane, o.ID, l.Host)
		if err := ops.Kill(ctx, l.Lane, o.ID); err != nil {
			m.logf("lane %s: kill %s: %v (retried next pass)", l.Lane, o.ID, err)
		}
		return
	}
	if err := ops.Clean(ctx, l.Lane, o.ID); err != nil {
		m.logf("lane %s: clean %s: %v (staging left for gc)", l.Lane, o.ID, err)
	}
	l.Orphan = nil
	if !m.start(ctx, l, Step{Kind: StepRecover, Prev: l.Prepared, Run: o.Run}) {
		l.Orphan = o
	}
}

// cancelRemoved kills the in-flight step of an experiment that was removed: a build (also one waiting in lane-wait for
// cores) serves nobody once its experiment is gone; a run is cancelled too when the removal asked for a teardown, and
// otherwise finishes. Kill escalates TERM to KILL, and the next probe finds the step dead (died() recovers the lane).
func (m *Manager) cancelRemoved(ctx context.Context, l *LaneFile) {
	x := m.exps[l.Experiment]
	if x == nil || x.Removed == nil {
		return
	}
	ref := l.Step
	if ref.Phase.teardown() {
		return
	}
	if ref.Kind != StepBuild && !(ref.Kind == StepRun && (x.Removed.Teardown || x.Removed.Cancel)) {
		return
	}
	if ref.Killed == "" {
		ref.Killed = "cancelled: experiment " + l.Experiment + " was removed"
		m.logf("lane %s: step %s %s", l.Lane, ref.ID, ref.Killed)
	}
	if err := m.ops(l.Host).Kill(ctx, l.Lane, ref.ID); err != nil {
		m.logf("lane %s: kill %s: %v (retried next pass)", l.Lane, ref.ID, err)
	}
}

// retryTeardown replays idempotent cleanup before the lane can be assigned again.
func (m *Manager) retryTeardown(ctx context.Context, l *LaneFile, ref *StepRef) bool {
	if ref.Kind != StepRelease && !ref.Phase.teardown() {
		return false
	}
	del := ref.DeleteCluster || ref.Phase.deleting()
	prev := l.Prepared
	if ref.Phase.deleting() {
		prev = nil // teardown hooks already ran
	}
	if (!del && prev == nil) || !m.start(ctx, l, Step{Kind: StepRelease, Prev: prev, DeleteCluster: del}) {
		return false
	}
	if err := m.ops(l.Host).Clean(ctx, l.Lane, ref.ID); err != nil {
		m.logf("lane %s: clean %s: %v (staging left for gc)", l.Lane, ref.ID, err)
	}
	m.unbind(l, "retrying interrupted teardown: "+ref.ID)
	return true
}

// enforceBound kills a step that has run past its bound. The next probe finds it dead and died() records why.
func (m *Manager) enforceBound(ctx context.Context, l *LaneFile) {
	ref := l.Step
	if ref.Bound <= 0 {
		return
	}
	age := m.now().Sub(ref.Started)
	if age < ref.Bound.D() {
		return
	}
	if ref.Killed == "" {
		ref.Killed = fmt.Sprintf("exceeded its bound %s (ran %s) and was killed", ref.Bound.D().Round(time.Second), age.Round(time.Second))
		m.logf("lane %s: step %s %s", l.Lane, ref.ID, ref.Killed)
	}
	if err := m.ops(l.Host).Kill(ctx, l.Lane, ref.ID); err != nil {
		m.logf("lane %s: kill %s: %v (retried next pass)", l.Lane, ref.ID, err)
	}
}
