package plan

import (
	"fmt"
	"io"
	"text/tabwriter"
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
	Plan     string       `json:"plan"`
	Pins     Pins         `json:"pins"`
	Order    string       `json:"order"`
	Reps     int          `json:"reps"`
	Parallel int          `json:"parallel"`
	Complete bool         `json:"complete"`
	Arms     []ArmStatus  `json:"arms"`
	Steps    []StepStatus `json:"steps,omitempty"`
	Next     []RunRef     `json:"next,omitempty"`
	Blocked  []RunRef     `json:"blocked,omitempty"`
}

const statusRuns = 5

// Status reads the state file only; it asks no backend.
func (e *Engine) Status() (*Status, error) {
	st, err := LoadState(e.Dir)
	if err != nil {
		return nil, err
	}
	return e.statusOf(st), nil
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
	for _, s := range st.Steps {
		out.Steps = append(out.Steps, StepStatus{ID: s.ID, Status: s.Status, Detail: s.Detail})
	}
	return out
}

// Render prints the status as text.
func (s *Status) Render(w io.Writer) {
	state := "running"
	if s.Complete {
		state = "complete"
	}
	fmt.Fprintf(w, "plan %s (%s): workload %s, subject %s, %d rep(s), %s, parallel %d\n", s.Plan, state, s.Pins.Workload, s.Pins.Subject, s.Reps, s.Order, s.Parallel)
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "arm\tdone\tin-flight\tqueued\tblocked\tfailed\tskipped")
	for _, a := range s.Arms {
		fmt.Fprintf(tw, "%s\t%d\t%d\t%d\t%d\t%d\t%d\n", a.Arm, a.Done, a.Inflight, a.Queued, a.Blocked, a.Failed, a.Skipped)
	}
	_ = tw.Flush()
	for _, st := range s.Steps {
		fmt.Fprintf(w, "step %s: %s %s\n", st.ID, st.Status, st.Detail)
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
