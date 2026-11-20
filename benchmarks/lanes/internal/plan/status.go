package plan

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"
	"time"
)

// ArmStatus counts an arm's nodes. Queued nodes are ready and wait for a free slot; blocked ones wait for an input.
type ArmStatus struct {
	Arm      string `json:"arm"`
	Done     int    `json:"done"`
	Inflight int    `json:"inflight"`
	Queued   int    `json:"queued"`
	Blocked  int    `json:"blocked"`
	Failed   int    `json:"failed"`
	Skipped  int    `json:"skipped"`
	Parked   int    `json:"parked"`
}

// ItemCounts counts the plan's items by state (each item is in exactly one).
type ItemCounts struct {
	Total    int `json:"total"`
	Done     int `json:"done"`
	Inflight int `json:"inflight"`
	Queued   int `json:"queued"`
	Blocked  int `json:"blocked"`
	Parked   int `json:"parked"`
	Skipped  int `json:"skipped"`
	Failed   int `json:"failed"`
}

// Item states. An item is parked when it failed twice with one reason, skipped when the operator skipped it, in
// flight when any of its nodes is released, done when all its nodes are done, failed when it ended with a node that
// did not, queued when a node is ready and waits for a slot, blocked when everything left waits on an input.
const (
	ItemDone     = "done"
	ItemInflight = "in-flight"
	ItemQueued   = "queued"
	ItemBlocked  = "blocked"
	ItemParked   = "parked"
	ItemSkipped  = "skipped"
	ItemFailed   = "failed"
)

// ItemRow is one item of the plan.
type ItemRow struct {
	Item   string `json:"item"`
	Group  string `json:"group,omitempty"`
	State  string `json:"state"`
	Reason string `json:"reason,omitempty"`
	Done   int    `json:"done"`
	Nodes  int    `json:"nodes"`
}

// RunRef names a run in status output.
type RunRef struct {
	Node string `json:"node"`
	Why  string `json:"why,omitempty"`
}

// StepStatus is a step instance's line.
type StepStatus struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	Detail string `json:"detail,omitempty"`
}

// Status is what `lanes plan status` shows.
type Status struct {
	Plan     string `json:"plan"`
	Pins     Pins   `json:"pins"`
	Order    string `json:"order"`
	Reps     int    `json:"reps"`
	Parallel int    `json:"parallel"`
	Complete bool   `json:"complete"`
	// Paused, Removed and Skipped are the operator's requests (pause, remove, skip ITEM), see Control.
	Paused  bool         `json:"paused,omitempty"`
	Removed bool         `json:"removed,omitempty"`
	Skipped []string     `json:"skipped,omitempty"`
	Arms    []ArmStatus  `json:"arms"`
	Steps   []StepStatus `json:"steps,omitempty"`
	// Parked maps each parked item to the reason it failed twice with.
	Parked map[string]string `json:"parked,omitempty"`
	// Retried maps each node `plan retry` requeued to when and what its park was.
	Retried map[string]RetryRec `json:"retried,omitempty"`
	// Items counts the items by state, ItemRows lists them in plan order; NextItems are the items of the next runs.
	Items     ItemCounts `json:"items"`
	ItemRows  []ItemRow  `json:"item_rows,omitempty"`
	NextItems []string   `json:"next_items,omitempty"`
	// Alarms are the keys of the alarms standing now; AlarmSince says since when.
	Alarms     []string             `json:"alarms,omitempty"`
	AlarmSince map[string]time.Time `json:"alarm_since,omitempty"`
	Next       []RunRef             `json:"next,omitempty"`
	Blocked    []RunRef             `json:"blocked,omitempty"`
}

const statusRuns = 5

// Status reads the state file only; it asks no backend.
func (e *Engine) Status() (*Status, error) {
	st, err := LoadState(e.Dir)
	if err != nil {
		return nil, err
	}
	out := e.statusOf(st)
	if c, err := LoadControl(e.Dir); err == nil {
		out.Paused, out.Removed = c.Paused, c.Removed
		out.Skipped = sortedKeys(c.Skipped)
	}
	return out, nil
}

func (e *Engine) statusOf(st *State) *Status {
	out := &Status{Plan: e.Plan.Name, Pins: e.Plan.Pins, Order: e.Plan.Order, Reps: e.Plan.Reps, Parallel: e.Plan.Parallel, Complete: st.Complete}
	byArm := map[string]*ArmStatus{}
	arms, _ := e.Plan.armOrder()
	for _, a := range arms {
		byArm[a] = &ArmStatus{Arm: a}
		out.Arms = append(out.Arms, ArmStatus{})
	}
	out.Arms = out.Arms[:0]
	for _, n := range st.Nodes {
		a := byArm[n.Arm]
		switch n.Status {
		case NodeDone:
			a.Done++
		case NodeSubmitted:
			a.Inflight++
		case NodeFailed:
			a.Failed++
		case NodeSkipped:
			a.Skipped++
		case NodeParked:
			a.Parked++
		case NodeWaiting:
			if why, _ := e.blocked(st, n); why != "" {
				a.Blocked++
				if len(out.Blocked) < statusRuns {
					out.Blocked = append(out.Blocked, RunRef{Node: n.ID, Why: why})
				}
			} else {
				a.Queued++
				if len(out.Next) < statusRuns {
					out.Next = append(out.Next, RunRef{Node: n.ID})
				}
			}
		}
	}
	for _, a := range arms {
		out.Arms = append(out.Arms, *byArm[a])
	}
	out.Parked = st.Parked
	out.Retried = st.Retried
	for k, at := range st.Alarms.Active {
		out.Alarms = append(out.Alarms, k)
		if out.AlarmSince == nil {
			out.AlarmSince = map[string]time.Time{}
		}
		out.AlarmSince[k] = at
	}
	seenNext := map[string]bool{}
	for _, r := range out.Next {
		if item := st.node(r.Node).Item; !seenNext[item] {
			seenNext[item] = true
			out.NextItems = append(out.NextItems, item)
		}
	}
	e.itemRows(st, out)
	sort.Strings(out.Alarms)
	for _, s := range st.Steps {
		out.Steps = append(out.Steps, StepStatus{ID: s.ID, Status: s.Status, Detail: s.Detail})
	}
	return out
}

// itemRows fills the per-item states and counts. Operator skips are read from the nodes' reasons, so a Status built
// from the state file alone still names them.
func (e *Engine) itemRows(st *State, out *Status) {
	for _, it := range e.items {
		row := ItemRow{Item: it.ID, Group: it.Group}
		var fly, queued, failed, skipped bool
		for _, n := range st.Nodes {
			if n.Item != it.ID {
				continue
			}
			row.Nodes++
			switch n.Status {
			case NodeDone:
				row.Done++
			case NodeSubmitted:
				fly = true
			case NodeFailed:
				failed = true
				if row.Reason == "" {
					row.Reason = n.Reason
				}
			case NodeSkipped:
				if strings.HasPrefix(n.Reason, "item "+it.ID+" skipped") || n.Reason == "plan removed" {
					skipped = true
					row.Reason = n.Reason
				}
			case NodeWaiting:
				if why, _ := e.blocked(st, n); why == "" {
					queued = true
				}
			}
		}
		switch reason, parked := st.Parked[it.ID]; {
		case parked:
			row.State, row.Reason = ItemParked, reason
		case skipped:
			row.State = ItemSkipped
		case fly:
			row.State = ItemInflight
		case row.Done == row.Nodes:
			row.State = ItemDone
		case failed:
			row.State = ItemFailed
		case queued:
			row.State = ItemQueued
		default:
			row.State = ItemBlocked
		}
		out.ItemRows = append(out.ItemRows, row)
		out.Items.Total++
		switch row.State {
		case ItemDone:
			out.Items.Done++
		case ItemInflight:
			out.Items.Inflight++
		case ItemQueued:
			out.Items.Queued++
		case ItemBlocked:
			out.Items.Blocked++
		case ItemParked:
			out.Items.Parked++
		case ItemSkipped:
			out.Items.Skipped++
		case ItemFailed:
			out.Items.Failed++
		}
	}
}

// Render prints the status as text.
func (s *Status) Render(w io.Writer) {
	state := "running"
	switch {
	case s.Complete:
		state = "complete"
	case s.Removed:
		state = "removed, draining"
	case s.Paused:
		state = "paused"
	}
	fmt.Fprintf(w, "plan %s (%s): workload %s, subject %s, %d rep(s), %s, parallel %d\n", s.Plan, state, s.Pins.Workload, s.Pins.Subject, s.Reps, s.Order, s.Parallel)
	c := s.Items
	fmt.Fprintf(w, "items: %d: %d done, %d in flight, %d queued, %d blocked, %d parked, %d skipped, %d failed\n",
		c.Total, c.Done, c.Inflight, c.Queued, c.Blocked, c.Parked, c.Skipped, c.Failed)
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "arm\tdone\tin-flight\tqueued\tblocked\tfailed\tskipped\tparked")
	for _, a := range s.Arms {
		fmt.Fprintf(tw, "%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\n", a.Arm, a.Done, a.Inflight, a.Queued, a.Blocked, a.Failed, a.Skipped, a.Parked)
	}
	_ = tw.Flush()
	for _, st := range s.Steps {
		fmt.Fprintf(w, "step %s: %s %s\n", st.ID, st.Status, st.Detail)
	}
	for _, r := range s.ItemRows {
		switch r.State {
		case ItemParked:
			fmt.Fprintf(w, "parked:  item %s parked: %s (failed twice)\n", r.Item, r.Reason)
		case ItemFailed:
			fmt.Fprintf(w, "failed:  item %s: %s\n", r.Item, r.Reason)
		case ItemSkipped:
			fmt.Fprintf(w, "skipped: item %s (%s)\n", r.Item, r.Reason)
		}
	}
	for _, id := range sortedKeys(s.Retried) {
		r := s.Retried[id]
		fmt.Fprintf(w, "retried: %s retried at %s (was parked: %s)\n", id, r.At.UTC().Format("2006-01-02 15:04Z"), r.WasParked)
	}
	for _, k := range s.Alarms {
		fmt.Fprintf(w, "alarm:   %s (since %s)\n", k, s.AlarmSince[k].UTC().Format("15:04Z"))
	}
	if len(s.NextItems) > 0 {
		fmt.Fprintf(w, "next items: %s\n", strings.Join(s.NextItems, ", "))
	}
	for i, r := range s.Next {
		pre := "next:    "
		if i > 0 {
			pre = "         "
		}
		fmt.Fprintf(w, "%s%s\n", pre, r.Node)
	}
	for i, r := range s.Blocked {
		pre := "blocked: "
		if i > 0 {
			pre = "         "
		}
		fmt.Fprintf(w, "%s%s (%s)\n", pre, r.Node, r.Why)
	}
}
