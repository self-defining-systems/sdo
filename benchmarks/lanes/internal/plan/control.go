package plan

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// Control is <plan dir>/control.json: what an operator asked of the plan (pause, remove, skip an item). The commands
// write only this file, under a lock; the engine reads it at the start of every Tick and applies it to the state, so
// a command never races the manager's state write and a restart loses nothing. Every application is idempotent.
type Control struct {
	Paused bool `json:"paused,omitempty"`
	// Removed retires the plan: nothing more is released, waiting nodes and steps are skipped, runs in flight finish
	// (or are cancelled with Cancel). The plan directory and its results stay; the name is never reused.
	Removed bool `json:"removed,omitempty"`
	Cancel  bool `json:"cancel,omitempty"`
	// Purge, set by `plan remove`, asks the runner to clean the plan up: cancel its experiments, move them and the plan's
	// directory to Archive and free the name. Archive is chosen on the first pass and kept, so a restart finishes it.
	Purge     bool               `json:"purge,omitempty"`
	Archive   string             `json:"archive,omitempty"`
	RemovedAt time.Time          `json:"removed_at,omitempty"`
	Skipped   map[string]SkipRec `json:"skipped,omitempty"`
	// Retries are the `plan retry` requests; the engine applies each once (State.RetriesApplied).
	Retries []RetryReq `json:"retries,omitempty"`
}

// SkipRec is a skipped item: its nodes that are not released are skipped; those in flight finish, or are cancelled.
type SkipRec struct {
	Cancel bool `json:"cancel,omitempty"`
}

func controlFile(dir string) string { return filepath.Join(dir, "control.json") }

// LoadControl reads the plan's control file; a missing one is no requests.
func LoadControl(dir string) (Control, error) {
	var c Control
	err := readJSONFile(controlFile(dir), &c)
	if errors.Is(err, os.ErrNotExist) {
		return Control{}, nil
	}
	return c, err
}

// updateControl applies f to the control file under an exclusive lock.
func updateControl(dir string, f func(*Control) error) error {
	if _, err := os.Stat(lockFile(dir)); err != nil {
		return fmt.Errorf("no such plan: %s", dir)
	}
	lf, err := os.OpenFile(filepath.Join(dir, "control.lock"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer lf.Close()
	if err := syscall.Flock(int(lf.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	c, err := LoadControl(dir)
	if err != nil {
		return err
	}
	if err := f(&c); err != nil {
		return err
	}
	return writeJSONFile(controlFile(dir), c)
}

// PausePlan stops the release of new nodes and steps; runs in flight are still watched and recorded.
func PausePlan(dir string) error {
	return updateControl(dir, func(c *Control) error {
		if c.Removed {
			return fmt.Errorf("the plan is removed")
		}
		c.Paused = true
		return nil
	})
}

// ResumePlan undoes PausePlan.
func ResumePlan(dir string) error {
	return updateControl(dir, func(c *Control) error { c.Paused = false; return nil })
}

// PurgePlan is `plan remove`: the plan is retired (RemovePlan with cancel) and marked for cleanup, which the runner
// carries out (PlanRemove in internal/bench) and which frees the plan's name.
func PurgePlan(dir string) error {
	return updateControl(dir, func(c *Control) error {
		c.Removed, c.Paused, c.Cancel, c.Purge = true, false, true, true
		return nil
	})
}

// SetArchive records where a purge moves the plan, once; it returns the recorded path.
func SetArchive(dir, path string) (string, error) {
	var got string
	err := updateControl(dir, func(c *Control) error {
		if c.Archive == "" {
			c.Archive = path
		}
		got = c.Archive
		return nil
	})
	return got, err
}

// RemovePlan retires the plan (see Control.Removed). With cancel, the experiments in flight are cancelled and running
// steps are stopped; without, they finish.
func RemovePlan(dir string, cancel bool) error {
	return updateControl(dir, func(c *Control) error {
		c.Removed, c.Paused = true, false
		c.Cancel = c.Cancel || cancel
		return nil
	})
}

// SkipItem skips an item of the plan: its unreleased nodes (also those that come back for a retry) are skipped, and
// with cancel its runs in flight are cancelled. A skip is not a failure: it counts toward no parking.
func SkipItem(dir, item string, cancel bool) error {
	var lock Lock
	if err := readJSONFile(lockFile(dir), &lock); err != nil {
		return fmt.Errorf("no such plan: %s", dir)
	}
	if !contains(lock.Items, item) {
		return fmt.Errorf("plan %s has no item %q", lock.Plan, item)
	}
	return updateControl(dir, func(c *Control) error {
		if c.Skipped == nil {
			c.Skipped = map[string]SkipRec{}
		}
		prev := c.Skipped[item]
		prev.Cancel = prev.Cancel || cancel
		c.Skipped[item] = prev
		return nil
	})
}

// UnskipItem lifts a skip: the nodes already skipped stay skipped; nothing new is skipped.
func UnskipItem(dir, item string) error {
	return updateControl(dir, func(c *Control) error { delete(c.Skipped, item); return nil })
}

// applyControl applies the requests to the state; it returns whether the plan may release new work.
func (e *Engine) applyControl(ctx context.Context, st *State, c Control) (release bool) {
	now := e.Clock.Now().UTC()
	skip := func(n *NodeState, why string) {
		n.Status, n.Reason, n.Ended = NodeSkipped, why, now
		e.event(n.ID, "skipped", "%s", why)
	}
	cancel := func(n *NodeState, why string) {
		if err := e.Backend.Cancel(ctx, n.Exp); err != nil {
			e.logf("cancel %s: %v (next pass)", n.Exp, err)
			return
		}
		e.event(n.ID, "cancelled", "%s: %s", n.Exp, why)
		skip(n, why)
	}
	for _, n := range st.Nodes {
		var why string
		var doCancel bool
		if it, ok := c.Skipped[n.Item]; ok {
			why, doCancel = "item "+n.Item+" skipped", it.Cancel
		}
		if c.Removed {
			why, doCancel = "plan removed", c.Cancel
		}
		switch {
		case why == "":
		case n.Status == NodeWaiting:
			skip(n, why)
		case n.Status == NodeSubmitted && doCancel:
			cancel(n, why)
		}
	}
	if c.Removed {
		for _, s := range st.Steps {
			switch {
			case s.Status == StepWaiting:
				s.Status, s.Detail, s.Ended = StepSkipped, "plan removed", now
				e.event(s.ID, "skipped", "plan removed")
			case s.Status == StepRunning && c.Cancel:
				if err := e.Backend.CancelStep(ctx, e.stepDir(s.ID)); err != nil {
					e.logf("cancel %s: %v (next pass)", s.ID, err)
					continue
				}
				s.Status, s.Detail, s.Ended = StepFailed, "cancelled: plan removed", now
				e.event(s.ID, "cancelled", "plan removed")
			}
		}
	}
	return !c.Paused && !c.Removed
}
