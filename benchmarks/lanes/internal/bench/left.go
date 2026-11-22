package bench

import (
	"context"
	"os"
	"path/filepath"
	"strings"
)

// Lanes that left hosts.yaml. A lane file whose lane is not in the inventory is closed, whatever state it was left in:
//
//   - its host is gone from the inventory too: nothing can be asked of it, so the lane is retired at once;
//   - a step of the lane is still alive: the lane is draining. It gets no new step, and is retired when its step ends;
//   - its step ended (finished, or dead: killed, or the host rebooted) or it has none: the lane's cluster is deleted by
//     a release step (which gives the lane's ledger entry back too) when it has one, and then the lane is retired.
//
// Retiring logs the last transition (so lane-time totals stop), moves the lane's files to lanes/retired and counts
// it for `status`. The state needs no new on-disk field: a lane file outside the inventory is all it reads.

// leftReleaseTries is how many release steps a lane that left may fail before it is retired with its cluster left.
const leftReleaseTries = 3

// retireLeft sorts the lane files outside the inventory into those retired now and those still leaving (m.leaving).
func (m *Manager) retireLeft() {
	m.leaving = map[string]*LaneFile{}
	ents, err := os.ReadDir(m.Root.path("lanes"))
	if err != nil {
		return
	}
	for _, e := range ents {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".json") {
			continue
		}
		lane := strings.TrimSuffix(name, ".json")
		if m.lanes[lane] != nil {
			continue
		}
		lf := &LaneFile{}
		if readJSON(m.Root.LanePath(lane), lf) != nil {
			continue
		}
		if _, ok := m.inv.Hosts[lf.Host]; ok && (lf.Step != nil || m.leftHasCluster(lf)) {
			m.leaving[lane] = lf
			continue
		}
		m.retire(lf)
	}
}

// leftHasCluster says whether a release step should delete the cluster of a lane that left: it was built, its host
// can delete clusters, and the deletion has neither been done nor failed too often.
func (m *Manager) leftHasCluster(lf *LaneFile) bool {
	h := m.inv.Hosts[lf.Host]
	if h.IsEks() {
		// An eks lane whose build failed after its cluster was made has none recorded as prepared and a cluster all the
		// same: the delete is idempotent and needs no workload, so any lane not shown gone gets it.
		return !lf.Gone && lf.BuildFailures < leftReleaseTries && h.ClusterDown != ""
	}
	return lf.Prepared != nil && !lf.Gone && lf.BuildFailures < leftReleaseTries && h.ClusterDown != ""
}

// retire closes a lane that left hosts.yaml: bench.log gets its last transition (to retired), its files move to
// lanes/retired.
func (m *Manager) retire(lf *LaneFile) {
	m.logf("lane %s: %s -> %s left hosts.yaml", lf.Lane, lf.State, LaneRetired)
	delete(m.leaving, lf.Lane)
	dir := m.Root.path("lanes", "retired")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return
	}
	name := lf.Lane + ".json"
	for _, f := range []string{name, lf.Lane + ".timeline.jsonl"} {
		if err := os.Rename(m.Root.path("lanes", f), filepath.Join(dir, f)); err != nil && !os.IsNotExist(err) {
			m.logf("lane %s: move %s: %v", lf.Lane, f, err)
		}
	}
	if m.lanesRetired == nil {
		m.lanesRetired = map[string]int{}
	}
	m.lanesRetired[lf.Host]++
}

// leftStep handles the probe of the step of a lane that left: running, it drains; ended, the next stage follows.
func (m *Manager) leftStep(ctx context.Context, lf *LaneFile, state string, ph *Phase) {
	if state == StepRunning {
		lf.Step.Phase = ph
		if reason := "left hosts.yaml: draining, retired when its step ends"; lf.Reason != reason {
			m.logf("lane %s: left hosts.yaml with step %s running: draining", lf.Lane, lf.Step.ID)
			lf.Reason = reason
		}
		return
	}
	ref := lf.Step
	lf.Step = nil
	if ref.Kind == StepRelease {
		var res Result
		tmp := m.Root.path("lanes", lf.Lane+".steps", ref.ID)
		if state == StepFinished && m.ops(lf.Host).Fetch(ctx, lf.Lane, ref.ID, tmp) == nil {
			_ = readJSON(filepath.Join(tmp, "result.json"), &res)
			_ = os.RemoveAll(tmp)
		}
		if res.OK {
			lf.Gone = true
		} else {
			lf.BuildFailures++
			m.logf("lane %s: deleting the cluster of a lane that left hosts.yaml failed (%d/%d): %s", lf.Lane, lf.BuildFailures, leftReleaseTries, res.Reason)
		}
	} else {
		m.logf("lane %s: step %s of a lane that left hosts.yaml ended (%s)", lf.Lane, ref.ID, state)
	}
	if err := m.ops(lf.Host).Clean(ctx, lf.Lane, ref.ID); err != nil {
		m.logf("lane %s: clean %s: %v", lf.Lane, ref.ID, err)
	}
}

// settleLeft moves every lane that left and has no step on: its cluster is deleted, or it is retired. A host that does
// not answer is left alone (its lanes' steps cannot be told from alive ones).
func (m *Manager) settleLeft(ctx context.Context, reachable map[string]bool) {
	for _, lf := range m.leaving {
		if lf.Step != nil || !reachable[lf.Host] {
			continue
		}
		if m.leftHasCluster(lf) {
			lf.Reason = "left hosts.yaml: deleting its cluster"
			if m.start(ctx, lf, Step{Kind: StepRelease, Prev: lf.Prepared, DeleteCluster: true}) {
				continue
			}
			lf.BuildFailures++ // refused (no recorded workload checkout): tried a bounded number of times
			continue
		}
		m.retire(lf)
	}
}
