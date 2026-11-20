package plan

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// RetryReq is one `plan retry` request in control.json: a node id or an item (Target), or every parked item (All).
// ID makes the application idempotent: the engine records the ids it applied in the state.
type RetryReq struct {
	ID     string    `json:"id"`
	Target string    `json:"target,omitempty"`
	All    bool      `json:"all,omitempty"`
	At     time.Time `json:"at"`
}

// RetryRec is the state's log of one retried node: when, and what its park was.
type RetryRec struct {
	At        time.Time `json:"at"`
	WasParked string    `json:"was_parked,omitempty"`
}

// retryable says whether a node's end is one a retry undoes: parked, failed, or skipped because an input never came.
func retryable(n *NodeState) bool {
	return n.Status == NodeParked || n.Status == NodeFailed || (n.Status == NodeSkipped && strings.HasPrefix(n.Reason, "upstream:"))
}

// retryItems resolves a request against the state to the items it un-parks.
func retryItems(st *State, target string, all bool) ([]string, error) {
	if all {
		var items []string
		for it := range st.Parked {
			items = append(items, it)
		}
		sort.Strings(items)
		if len(items) == 0 {
			return nil, fmt.Errorf("no parked item to retry")
		}
		return items, nil
	}
	if n := st.node(target); n != nil {
		switch n.Status {
		case NodeDone:
			return nil, fmt.Errorf("node %s is done", target)
		case NodeSubmitted:
			return nil, fmt.Errorf("node %s is in flight", target)
		}
		if !retryable(n) {
			return nil, fmt.Errorf("node %s is %s, not parked", target, n.Status)
		}
		return []string{n.Item}, nil
	}
	known := false
	for _, n := range st.Nodes {
		if n.Item != target {
			continue
		}
		known = true
		if retryable(n) {
			return []string{target}, nil
		}
	}
	if !known {
		return nil, fmt.Errorf("no node or item %q in the plan", target)
	}
	return nil, fmt.Errorf("item %s has nothing parked or failed to retry", target)
}

// RetryPlan is `plan retry`: it asks the engine to un-park the item (or the node's item, or every parked item with
// all), reset its failure count and requeue its parked and failed nodes. It refuses a node that is done or in flight.
func RetryPlan(dir, target string, all bool) error {
	if (target == "") == !all {
		return fmt.Errorf("retry wants an item or node, or --all-parked")
	}
	st, err := LoadState(dir)
	if err != nil {
		return err
	}
	if _, err := retryItems(st, target, all); err != nil {
		return err
	}
	return updateControl(dir, func(c *Control) error {
		if c.Removed {
			return fmt.Errorf("the plan is removed")
		}
		now := time.Now().UTC()
		c.Retries = append(c.Retries, RetryReq{ID: fmt.Sprintf("%d", now.UnixNano()), Target: target, All: all, At: now})
		return nil
	})
}

// applyRetries applies the requests not applied yet and says whether it changed the state. The park and the failure
// counts of each item are cleared and its retryable nodes wait again; their attempt number keeps growing so
// experiment names and result rows stay unique. The state logs when and why. A request that no longer fits (its
// node went on) is dropped with an event.
func (e *Engine) applyRetries(st *State, c Control) bool {
	now := e.Clock.Now().UTC()
	changed := false
	for _, r := range c.Retries {
		if contains(st.RetriesApplied, r.ID) {
			continue
		}
		changed = true
		st.RetriesApplied = append(st.RetriesApplied, r.ID)
		items, err := retryItems(st, r.Target, r.All)
		if err != nil {
			e.event("", "retry-dropped", "%s: %v", r.Target, err)
			continue
		}
		for _, it := range items {
			was := st.Parked[it]
			delete(st.Parked, it)
			for k := range st.Failures {
				if strings.HasPrefix(k, it+"|") {
					delete(st.Failures, k)
				}
			}
			for _, n := range st.Nodes {
				if n.Item != it || !retryable(n) {
					continue
				}
				why := was
				if why == "" {
					why = n.Reason
				}
				n.Status, n.Exp, n.Reason, n.Artifacts, n.Submitted, n.Ended, n.SubmitFails = NodeWaiting, "", "", nil, time.Time{}, time.Time{}, 0
				if st.Retried == nil {
					st.Retried = map[string]RetryRec{}
				}
				st.Retried[n.ID] = RetryRec{At: now, WasParked: why}
				e.event(n.ID, "retried", "was parked: %s", why)
				st.Complete = false
			}
		}
	}
	return changed
}
