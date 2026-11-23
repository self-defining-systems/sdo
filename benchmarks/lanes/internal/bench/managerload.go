package bench

import (
	"fmt"
	"runtime"
	"strings"
	"time"
)

// A provider-eks host runs every step on the manager's machine: the harness, `go run`, kubectl and aws all wait their
// turn there, while the clusters they work on are in AWS. A machine whose load is many times its cores makes those
// calls time out, so rows fail for reasons that have nothing to do with the application and the timings of whole rows
// are not real. The host's load gate (eks.max_load5_per_core, default DefaultEksMaxLoad5PerCore) is therefore on the
// manager's machine: no build or run starts while it is over, and a step that timed out while it was over is not
// counted as a failure of its item or of its lane.

// machineLoad is why host h's manager machine is over its load gate ("" when it is not, or the host has none): the
// load per core the last probe read, against eks.max_load5_per_core.
func (m *Manager) machineLoad(h string) string {
	limit := m.inv.Hosts[h].ManagerLoadLimit()
	st, ok := m.hosts[h]
	if limit <= 0 || !ok || st.Error != "" {
		return ""
	}
	cores := st.Cores
	if cores <= 0 {
		cores = runtime.NumCPU()
	}
	if per := st.Load5 / float64(cores); per > limit {
		return fmt.Sprintf("the manager's machine has load5 %.1f/core over max_load5_per_core %g (load5 %.0f on %d cores)", per, limit, st.Load5, cores)
	}
	return ""
}

// observeMachineLoad remembers when host h's manager machine was last seen over its gate: a step that started before
// then ran, at least in part, on an overloaded machine.
func (m *Manager) observeMachineLoad(h string) string {
	why := m.machineLoad(h)
	if why != "" {
		if m.overSeen == nil {
			m.overSeen = map[string]time.Time{}
		}
		if m.overSeen[h].IsZero() || m.now().Sub(m.overSeen[h]) > time.Hour {
			m.logf("ALARM host %s: %s, no new build or run until it is at or under the limit", h, why)
		}
		m.overSeen[h] = m.now()
	}
	return why
}

// maxLoadRetries is how many times one sequence's failures are written off to an overloaded machine before the next
// one counts (a machine that is always over its limit must not retry an item for ever).
const maxLoadRetries = 3

// timedOut: the step's reason names a timeout of a command (the harness, `go run`, kubectl, aws).
func timedOut(reason string) bool {
	r := strings.ToLower(reason)
	return strings.Contains(r, "timeout") || strings.Contains(r, "timed out")
}

// lostToLoad says whether the failure of ref (res) is the overloaded manager machine's, not its item's or its lane's:
// it is a timeout, and the machine was over its gate at some probe since the step started. It counts the write-off
// against key (the item), and refuses it after maxLoadRetries.
func (m *Manager) lostToLoad(l *LaneFile, ref *StepRef, res Result, key string) bool {
	if res.LaneDown != "" || !timedOut(res.Reason) || m.inv.Hosts[l.Host].ManagerLoadLimit() <= 0 {
		return false
	}
	if seen := m.overSeen[l.Host]; seen.IsZero() || seen.Before(ref.Started) {
		return false
	}
	if m.loadRetried == nil {
		m.loadRetried = map[string]int{}
	}
	if m.loadRetried[key] >= maxLoadRetries {
		return false
	}
	m.loadRetried[key]++
	m.logf("lane %s: step %s timed out while the manager's machine was over max_load5_per_core (%s): not counted as a failure of its item (write-off %d of %d)",
		l.Lane, ref.ID, clip(res.Reason, 120), m.loadRetried[key], maxLoadRetries)
	return true
}
