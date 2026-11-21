package plan

import (
	"fmt"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"
)

// RetryReq is one `plan retry` request in control.json: a node id or an item (Target), or every parked item (All).
// ID makes the application idempotent: the engine records the ids it applied in the state.
type RetryReq struct {
	ID     string `json:"id"`
	Target string `json:"target,omitempty"`
	All    bool   `json:"all,omitempty"`
	// Repin maps a role (subject, workload) to the resolved pin the retried nodes run with from now on.
	Repin map[string]Pin `json:"repin,omitempty"`
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
	return RetryPlanRepin(dir, target, all, nil, nil)
}

type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now() }

// RetryPlanRepin is RetryPlan that also moves the retried nodes to new pins. Each repin is role=ref (role subject or
// workload; ref a revision or a ref, optionally ref:/abs/checkout, resolved to a full revision now through the
// resolver option, as `plan add` does). Every node the retry requeues is rendered with the new pins and validate
// checks the spec (the platform's rendered-spec rules); a refusal leaves no request. Nodes that are done keep their
// pins, and so do the plan's lock and the rows already written.
func RetryPlanRepin(dir, target string, all bool, repin []string, validate func([]byte) error, opts ...Option) error {
	if (target == "") == !all {
		return fmt.Errorf("retry wants an item or node, or --all-parked")
	}
	st, err := LoadState(dir)
	if err != nil {
		return err
	}
	items, err := retryItems(st, target, all)
	if err != nil {
		return err
	}
	var pins map[string]Pin
	if len(repin) > 0 {
		if pins, err = resolveRepin(dir, repin, optionsOf(opts)); err != nil {
			return err
		}
		if err := validateRepin(dir, st, items, pins, validate); err != nil {
			return err
		}
	}
	return updateControl(dir, func(c *Control) error {
		if c.Removed {
			return fmt.Errorf("the plan is removed")
		}
		// The control file only grows, under its lock, so the next number is a unique id; the engine stamps the time.
		c.Retries = append(c.Retries, RetryReq{ID: fmt.Sprint(len(c.Retries) + 1), Target: target, All: all, Repin: pins})
		return nil
	})
}

// resolveRepin turns role=ref arguments into pins named like the plan's own (the lock has the names).
func resolveRepin(dir string, repin []string, o options) (map[string]Pin, error) {
	var lock Lock
	if err := readJSONFile(lockFile(dir), &lock); err != nil {
		return nil, fmt.Errorf("no such plan: %s", dir)
	}
	out := map[string]Pin{}
	for _, r := range repin {
		role, val, ok := strings.Cut(r, "=")
		if !ok || val == "" {
			return nil, fmt.Errorf("--repin %q: want role=revision (role subject or workload)", r)
		}
		var cur Pin
		switch role {
		case "subject":
			cur = lock.Pins.Subject
		case "workload":
			cur = lock.Pins.Workload
		default:
			return nil, fmt.Errorf("--repin %q: the role is subject or workload", r)
		}
		if cur.Name == "" {
			return nil, fmt.Errorf("--repin %s: the plan pins no %s", r, role)
		}
		if _, dup := out[role]; dup {
			return nil, fmt.Errorf("--repin: %s given twice", role)
		}
		ref, d, _ := strings.Cut(val, ":")
		if !refRE.MatchString(ref) || (d != "" && !filepath.IsAbs(d)) {
			return nil, fmt.Errorf("--repin %q: want role=revision or role=ref[:/abs/checkout]", r)
		}
		if o.resolve == nil {
			if !revRE.MatchString(ref) || d != "" {
				return nil, fmt.Errorf("--repin %s names a ref, and no resolver is available to resolve it", r)
			}
			out[role] = Pin{Name: cur.Name, Rev: ref}
			continue
		}
		if d == "" {
			d = lock.Base
		}
		rev, err := o.resolve(d, ref)
		if err != nil {
			return nil, fmt.Errorf("--repin %s: resolving %s in %s: %w", r, ref, d, err)
		}
		if !fullRevRE.MatchString(rev) {
			return nil, fmt.Errorf("--repin %s: %q is not a full revision", r, rev)
		}
		out[role] = Pin{Name: cur.Name, Rev: rev}
	}
	return out, nil
}

// withRepin is pins with the roles of repin replaced.
func withRepin(pins Pins, repin map[string]Pin) Pins {
	if p, ok := repin["subject"]; ok {
		pins.Subject = p
	}
	if p, ok := repin["workload"]; ok {
		pins.Workload = p
	}
	return pins
}

// validateRepin renders every node the retry would requeue with its new pins and runs validate on each distinct spec.
func validateRepin(dir string, st *State, items []string, repin map[string]Pin, validate func([]byte) error) error {
	e, err := Open(dir, nil, wallClock{})
	if err != nil {
		return err
	}
	seen := map[string]bool{}
	for _, n := range st.Nodes {
		if !contains(items, n.Item) || !retryable(n) {
			continue
		}
		np := withRepin(e.pinsOf(n.Pins), repin)
		node := n.node()
		node.Pins = &np
		b, name, err := e.Render(node, n.Attempt+1, e.placeholderArtifacts(n.Arm))
		if err != nil {
			return fmt.Errorf("node %s: %w", n.ID, err)
		}
		key := strings.ReplaceAll(string(b), name, "")
		if seen[key] || validate == nil {
			continue
		}
		seen[key] = true
		if err := validate(b); err != nil {
			return fmt.Errorf("node %s with the new pins: spec: %w", n.ID, err)
		}
	}
	return nil
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
				if len(r.Repin) > 0 {
					e.repinNode(st, n, r, now)
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

// repinNode moves n to the pins of the request and logs the change. Attempts of n that have no row yet keep the pins
// they ran with.
func (e *Engine) repinNode(st *State, n *NodeState, r RetryReq, now time.Time) {
	old := e.pinsOf(n.Pins)
	for i := range n.Attempts {
		if a := &n.Attempts[i]; !a.Rowed && a.Pins == nil {
			o := old
			a.Pins = &o
		}
	}
	np := withRepin(old, r.Repin)
	n.Pins = &np
	for _, role := range []string{"subject", "workload"} {
		p, ok := r.Repin[role]
		if !ok {
			continue
		}
		from := old.Subject
		if role == "workload" {
			from = old.Workload
		}
		i := slices.IndexFunc(st.PinChanges, func(c PinChange) bool { return c.Retry == r.ID && c.Role == role && c.From == from.String() })
		if i < 0 {
			st.PinChanges = append(st.PinChanges, PinChange{At: now, Retry: r.ID, Role: role, From: from.String(), To: p.String()})
			i = len(st.PinChanges) - 1
		}
		st.PinChanges[i].Nodes = append(st.PinChanges[i].Nodes, n.ID)
	}
	e.event(n.ID, "repinned", "subject %s, workload %s", np.Subject, np.Workload)
}
