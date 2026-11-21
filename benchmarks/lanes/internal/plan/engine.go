package plan

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Engine executes one plan. All of its state is on disk (state.json, events.jsonl): Tick can be called from a fresh
// process at any point and continues where the last one stopped.
type Engine struct {
	Dir     string
	Plan    *Plan
	Lock    Lock
	Backend Backend
	Clock   Clock
	// Logf, when set, receives one line per decision.
	Logf func(format string, a ...any)
	// Alarms receives the alarms the engine raises (nil: they are only logged and kept in events.jsonl).
	Alarms AlarmSink

	items []Item
	// releasing is false while the plan is paused or removed: nothing new starts.
	releasing bool
}

// Open loads a plan directory and checks the stored copies against the lock.
func Open(dir string, b Backend, c Clock) (*Engine, error) {
	e := &Engine{Dir: dir, Backend: b, Clock: c}
	if err := readJSONFile(lockFile(dir), &e.Lock); err != nil {
		return nil, fmt.Errorf("plan %s: %w", dir, err)
	}
	pb, err := os.ReadFile(planFile(dir))
	if err != nil {
		return nil, err
	}
	if sha(pb) != e.Lock.PlanSHA {
		return nil, fmt.Errorf("plan %s: plan.json differs from the lock (plans are not edited: add a new plan)", e.Lock.Plan)
	}
	var p Plan
	if err := readJSONFileBytes(pb, &p); err != nil {
		return nil, err
	}
	p.defaults()
	e.Plan = &p
	e.items = p.Items.List
	for arm, want := range e.Lock.Templates {
		tb, err := os.ReadFile(templateFile(dir, arm))
		if err != nil {
			return nil, err
		}
		if sha(tb) != want {
			return nil, fmt.Errorf("plan %s: template of arm %s differs from the lock", e.Lock.Plan, arm)
		}
	}
	return e, nil
}

// pinsOf is the pins a node runs with: its own when a retry re-pinned it, else the plan's.
func (e *Engine) pinsOf(p *Pins) Pins {
	if p != nil {
		return *p
	}
	return e.Plan.Pins
}

func (e *Engine) logf(f string, a ...any) {
	if e.Logf != nil {
		e.Logf("plan %s: "+f, append([]any{e.Plan.Name}, a...)...)
	}
}

// Tick is one pass: observe what was released, verify artifacts, run steps, release what is ready. It returns an
// error only when the state cannot be read or written; a backend that fails leaves the work for the next pass.
func (e *Engine) Tick(ctx context.Context) error {
	st, err := LoadState(e.Dir)
	if err != nil {
		return err
	}
	ctl, err := LoadControl(e.Dir)
	if err != nil {
		return err
	}
	retried := e.applyRetries(st, ctl) // before the Complete check: a retry reopens a finished plan
	if st.Complete {
		if retried {
			return writeJSONFile(stateFile(e.Dir), st)
		}
		return nil
	}
	e.observe(ctx, st)
	e.verifyPending(ctx, st)
	e.releasing = e.applyControl(ctx, st, ctl)
	e.runSteps(ctx, st)
	e.skipOrphans(st)
	if e.releasing {
		e.release(ctx, st)
	}
	e.skipOrphans(st) // a release that found a stale artifact failed its source
	e.recordRows(ctx, st)
	e.checkAlarms(ctx, st)
	if e.allTerminal(st) {
		st.Complete = true
		e.event("", "complete", "%d nodes", len(st.Nodes))
	}
	return writeJSONFile(stateFile(e.Dir), st)
}

// allTerminal: every node and step has ended and every ended attempt has its row (a plan is not complete while a grade
// is still to come).
func (e *Engine) allTerminal(st *State) bool {
	for _, n := range st.Nodes {
		if !terminalNode(n.Status) {
			return false
		}
		for _, a := range n.Attempts {
			if !a.Rowed && a.Exp != "" {
				return false
			}
		}
	}
	for _, s := range st.Steps {
		if s.Status != StepDone && s.Status != StepFailed && s.Status != StepSkipped {
			return false
		}
	}
	return true
}

func (e *Engine) observe(ctx context.Context, st *State) {
	for _, n := range st.Nodes {
		if n.Status != NodeSubmitted {
			continue
		}
		obs, err := e.Backend.Observe(ctx, n.Exp)
		if err != nil {
			e.logf("observe %s: %v (next pass)", n.Exp, err)
			continue
		}
		switch obs.Phase {
		case PhaseDone:
			rd, err := e.Backend.RunData(ctx, n.Exp)
			if err != nil {
				e.logf("run data %s: %v (next pass)", n.Exp, err)
				continue
			}
			if why := e.sourceMismatch(e.pinsOf(n.Pins), rd); why != "" {
				// Invariant 1: a run on other source than the arm's is not a result; it runs again.
				e.checkStages(st, n, obs)
				e.fail(st, n, "source-mismatch", why, obs.Stages)
				continue
			}
			n.Status, n.Ended = NodeDone, nonZero(obs.Ended, e.Clock.Now().UTC())
			n.Attempts = append(n.Attempts, AttemptRecord{Attempt: n.Attempt, Exp: n.Exp, Submitted: n.Submitted, Ended: n.Ended, Stages: obs.Stages})
			e.checkStages(st, n, obs)
			e.event(n.ID, "done", "%s", n.Exp)
		case PhaseFailed:
			e.checkStages(st, n, obs)
			e.fail(st, n, obs.Reason, obs.Detail, obs.Stages)
		case PhaseAbsent:
			e.fail(st, n, "lost", "the backend has no experiment "+n.Exp, nil)
		default:
			e.watchStage(st, n, obs)
		}
	}
}

func nonZero(t, def time.Time) time.Time {
	if t.IsZero() {
		return def
	}
	return t.UTC()
}

// FailuresToPark is how many failures of one item with the same reason park it (invariant 3).
const FailuresToPark = 2

// fail ends the node's current attempt with reason and decides what follows. The first failure of an (item, reason)
// sends the node back to waiting: it runs again as a new attempt under a new experiment name (-x<n>). The second
// identical failure, on any node of the item, parks the item: this node fails for good and the item's other nodes
// that have not been released are parked. Only the runner retries: the experiments run with retries 0.
func (e *Engine) fail(st *State, n *NodeState, reason, detail string, stages map[string]time.Duration) {
	if reason == "" {
		reason = "failed"
	}
	now := e.Clock.Now().UTC()
	rec := AttemptRecord{Attempt: n.Attempt, Exp: n.Exp, Reason: reason, Detail: detail, Submitted: n.Submitted, Ended: now, Stages: stages}
	if k := len(n.Attempts); k > 0 && n.Attempts[k-1].Attempt == n.Attempt {
		rec.Stages = n.Attempts[k-1].Stages // the attempt had ended done; a bad artifact fails it afterwards
		n.Attempts[k-1] = rec
	} else {
		n.Attempts = append(n.Attempts, rec)
	}
	if st.Failures == nil {
		st.Failures = map[string]int{}
	}
	key := n.Item + "|" + reason
	st.Failures[key]++
	e.event(n.ID, "failed", "%s: %s", reason, detail)
	e.logf("%s failed (attempt %d, %d of %d for item %s): %s: %s", n.ID, n.Attempt, st.Failures[key], FailuresToPark, n.Item, reason, detail)
	_, parked := st.Parked[n.Item]
	if st.Failures[key] < FailuresToPark && !parked {
		n.Status, n.Exp, n.Reason, n.Artifacts, n.Submitted, n.Ended = NodeWaiting, "", reason, nil, time.Time{}, time.Time{}
		e.event(n.ID, "retry", "attempt %d after %s", n.Attempt+1, reason)
		return
	}
	n.Status, n.Reason, n.Ended = NodeFailed, reason, now
	if parked {
		return
	}
	if st.Parked == nil {
		st.Parked = map[string]string{}
	}
	st.Parked[n.Item] = reason
	e.event(n.ID, "parked", "item %s failed %d times with %s", n.Item, FailuresToPark, reason)
	for _, o := range st.Nodes {
		if o.Item == n.Item && o.Status == NodeWaiting {
			o.Status, o.Reason, o.Ended = NodeParked, fmt.Sprintf("item %s parked: %s twice", n.Item, reason), now
		}
	}
}

// outgoing returns the (kind, mode) pairs the edges ask of arm's nodes (or a step's artifact).
func (e *Engine) outgoing(from string) map[string][]string {
	out := map[string][]string{}
	for _, ed := range e.Plan.Edges {
		if ed.From != from || ed.Artifact == "" {
			continue
		}
		mode := e.mode(ed)
		have := false
		for _, m := range out[ed.Artifact] {
			have = have || m == mode
		}
		if !have {
			out[ed.Artifact] = append(out[ed.Artifact], mode)
		}
	}
	return out
}

func (e *Engine) mode(ed Edge) string {
	if ed.Verify != "" {
		return ed.Verify
	}
	if d, ok := e.Plan.Artifacts[ed.Artifact]; ok && d.Verify != "" {
		return d.Verify
	}
	return VerifyExists
}

func (e *Engine) pathTemplate(from, kind string) string {
	if e.Plan.isStep(from) {
		return e.Plan.stepOf(from).Out
	}
	return e.Plan.Artifacts[kind].Path
}

func (e *Engine) nodeSource(n *NodeState) Source { return Source{ID: n.ID, Exp: n.Exp} }

func (e *Engine) stepDir(id string) string {
	return filepath.Join(e.Dir, "steps", strings.NewReplacer("/", "_", ":", "_").Replace(id))
}

// verifyPending exports and verifies every artifact the edges take from done nodes and done steps. A failed verify
// fails the source (the artifact is bad: the node must run again, and no dependent may start on it).
func (e *Engine) verifyPending(ctx context.Context, st *State) {
	for _, n := range st.Nodes {
		if n.Status != NodeDone {
			continue
		}
		if n.Artifacts == nil {
			n.Artifacts = map[string]*ArtifactState{}
		}
		for kind, modes := range e.outgoing(n.Arm) {
			if a := n.Artifacts[kind]; a != nil && a.Verified {
				continue
			}
			ok, detail, err := e.exportVerify(ctx, e.nodeSource(n), n.Artifacts, kind, e.pathTemplate(n.Arm, kind), modes)
			switch {
			case err != nil:
				e.logf("%s artifact %s: %v (next pass)", n.ID, kind, err)
			case !ok:
				e.fail(st, n, "artifact:"+kind, detail, nil)
			}
			if n.Status != NodeDone {
				break
			}
		}
	}
	for _, s := range st.Steps {
		if s.Status != StepDone {
			continue
		}
		if s.Artifacts == nil {
			s.Artifacts = map[string]*ArtifactState{}
		}
		for kind, modes := range e.outgoing(s.Name) {
			if a := s.Artifacts[kind]; a != nil && a.Verified {
				continue
			}
			src := Source{ID: s.ID, Step: true, Dir: e.stepDir(s.ID)}
			ok, detail, err := e.exportVerify(ctx, src, s.Artifacts, kind, e.pathTemplate(s.Name, kind), modes)
			switch {
			case err != nil:
				e.logf("%s artifact %s: %v (next pass)", s.ID, kind, err)
			case !ok:
				s.Status, s.Detail, s.Ended = StepFailed, "artifact "+kind+": "+detail, e.Clock.Now().UTC()
				e.event(s.ID, "failed", "%s", s.Detail)
			}
			if s.Status == StepFailed {
				break
			}
		}
	}
}

// exportVerify exports kind and runs every mode. err: an infrastructure failure, nothing recorded as a verdict.
func (e *Engine) exportVerify(ctx context.Context, src Source, held map[string]*ArtifactState, kind, tmpl string, modes []string) (bool, string, error) {
	a, err := e.Backend.Export(ctx, src, kind, tmpl)
	if err != nil {
		return false, "", err
	}
	for _, m := range modes {
		v, err := e.Backend.Verify(ctx, src, a, m)
		if err != nil {
			return false, "", err
		}
		if !v.OK {
			held[kind] = &ArtifactState{Path: a.Path, Detail: m + ": " + v.Detail, At: e.Clock.Now().UTC()}
			return false, m + ": " + v.Detail, nil
		}
	}
	held[kind] = &ArtifactState{Path: a.Path, Verified: true, At: e.Clock.Now().UTC()}
	e.event(src.ID, "artifact", "%s verified at %s", kind, a.Path)
	return true, "", nil
}

// runSteps starts the lane-free steps whose inputs ended and polls the running ones.
func (e *Engine) runSteps(ctx context.Context, st *State) {
	for _, s := range st.Steps {
		def := e.Plan.stepOf(s.Name)
		switch s.Status {
		case StepWaiting:
			if !e.releasing {
				continue
			}
			inputs, ready := e.stepInputs(st, def, s.Rep)
			if !ready {
				continue
			}
			if len(inputs) == 0 {
				s.Status, s.Detail, s.Ended = StepSkipped, "no input node is done", e.Clock.Now().UTC()
				e.event(s.ID, "skipped", "%s", s.Detail)
				continue
			}
			timeout := 30 * time.Minute
			if def.Timeout != "" {
				timeout, _ = time.ParseDuration(def.Timeout)
			}
			err := e.Backend.StartStep(ctx, StepJob{Plan: e.Plan.Name, ID: s.ID, Name: s.Name, Run: def.Run, Dir: e.stepDir(s.ID),
				Timeout: timeout, Inputs: inputs})
			if err != nil {
				e.logf("start %s: %v (next pass)", s.ID, err)
				continue
			}
			s.Status, s.Started = StepRunning, e.Clock.Now().UTC()
			e.event(s.ID, "step-start", "%d inputs", len(inputs))
		case StepRunning:
			obs, err := e.Backend.ObserveStep(ctx, e.stepDir(s.ID))
			if err != nil {
				e.logf("observe %s: %v (next pass)", s.ID, err)
				continue
			}
			switch obs.Phase {
			case PhaseDone:
				s.Status, s.Ended = StepDone, e.Clock.Now().UTC()
				e.event(s.ID, "step-done", "-")
			case PhaseFailed:
				s.Status, s.Detail, s.Ended = StepFailed, obs.Detail, e.Clock.Now().UTC()
				e.event(s.ID, "failed", "%s", obs.Detail)
			case PhaseAbsent:
				s.Status = StepWaiting // the start never reached the backend
			}
		}
	}
}

// stepInputs are the done nodes of the step's After arms (that rep's, for a per-rep step); ready: every such node
// has ended, one way or the other. A failed or skipped input does not stop the step: it runs over what is done.
func (e *Engine) stepInputs(st *State, def Step, rep int) ([]StepInput, bool) {
	var in []StepInput
	for _, n := range st.Nodes {
		if !contains(def.After, n.Arm) || (rep != 0 && n.Rep != rep) {
			continue
		}
		if !terminalNode(n.Status) {
			return nil, false
		}
		if n.Status != NodeDone {
			continue
		}
		si := StepInput{Node: n.ID, Arm: n.Arm, Item: n.Item, Rep: n.Rep, Exp: n.Exp, Artifacts: map[string]string{}}
		for k, a := range n.Artifacts {
			if a.Verified {
				si.Artifacts[k] = a.Path
			}
		}
		in = append(in, si)
	}
	return in, true
}

func contains(l []string, s string) bool {
	for _, x := range l {
		if x == s {
			return true
		}
	}
	return false
}

// incoming are the edges into arm.
func (e *Engine) incoming(arm string) []Edge {
	var out []Edge
	for _, ed := range e.Plan.Edges {
		if ed.To == arm {
			out = append(out, ed)
		}
	}
	return out
}

// upstream finds what edge ed takes n's input from: a node (the same item and rep) or a step instance.
func (e *Engine) upstream(st *State, n *NodeState, ed Edge) (status, id string, arts map[string]*ArtifactState) {
	if e.Plan.isStep(ed.From) {
		rep := 0
		if e.Plan.stepOf(ed.From).Per == "rep" {
			rep = n.Rep
		}
		s := st.step(StepID(ed.From, rep))
		if s == nil {
			return StepFailed, StepID(ed.From, rep), nil
		}
		return s.Status, s.ID, s.Artifacts
	}
	u := st.node(NodeID(ed.From, n.Item, n.Rep))
	if u == nil {
		return NodeFailed, NodeID(ed.From, n.Item, n.Rep), nil
	}
	return u.Status, u.ID, u.Artifacts
}

// blocked says why a waiting node cannot be released yet ("" when it can), and whether the wait is final (an input
// failed or was skipped, so it never will).
func (e *Engine) blocked(st *State, n *NodeState) (why string, final bool) {
	for _, ed := range e.incoming(n.Arm) {
		status, id, arts := e.upstream(st, n, ed)
		switch status {
		case NodeDone:
			if ed.Artifact != "" {
				if a := arts[ed.Artifact]; a == nil || !a.Verified {
					return fmt.Sprintf("%s: artifact %s not verified", id, ed.Artifact), false
				}
			}
		case NodeFailed, NodeSkipped, NodeParked:
			return fmt.Sprintf("%s %s", id, status), true
		default:
			return fmt.Sprintf("waits for %s", id), false
		}
	}
	return "", false
}

// skipOrphans ends the waiting nodes whose inputs ended without delivering.
func (e *Engine) skipOrphans(st *State) {
	for changed := true; changed; {
		changed = false
		for _, n := range st.Nodes {
			if n.Status != NodeWaiting {
				continue
			}
			if why, final := e.blocked(st, n); final {
				n.Status, n.Reason, n.Ended = NodeSkipped, "upstream: "+why, e.Clock.Now().UTC()
				e.event(n.ID, "skipped", "%s", n.Reason)
				changed = true
			}
		}
	}
}

// Inflight counts the experiments released and not yet ended.
func (s *State) Inflight() int {
	k := 0
	for _, n := range s.Nodes {
		if n.Status == NodeSubmitted {
			k++
		}
	}
	return k
}

// release submits the ready nodes in plan order, up to Parallel in flight.
func (e *Engine) release(ctx context.Context, st *State) {
	inflight := st.Inflight()
	for _, n := range st.Nodes {
		if inflight >= e.Plan.Parallel {
			return
		}
		if n.Status != NodeWaiting {
			continue
		}
		if why, _ := e.blocked(st, n); why != "" {
			continue
		}
		arts, ok := e.recheck(ctx, st, n)
		if !ok {
			continue
		}
		attempt := n.Attempt + 1
		spec, name, err := e.Render(n.node(), attempt, arts)
		if err != nil {
			e.fail(st, n, "spec", err.Error(), nil)
			continue
		}
		path := filepath.Join(e.Dir, "specs", name+".json")
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err == nil {
			err = os.WriteFile(path, spec, 0o644)
		}
		if err != nil {
			e.logf("write spec %s: %v", name, err)
			continue
		}
		err = e.Backend.Submit(ctx, Job{Plan: e.Plan.Name, Node: n.ID, Exp: name, SpecPath: path})
		if err != nil && !errors.Is(err, ErrExists) {
			// A submit that fails is retried, but not forever: after SubmitPasses passes in a row the failure is an
			// alarm and a failed attempt, and the same failure again parks the item.
			n.SubmitFails++
			e.logf("submit %s: %v (pass %d of %d)", name, err, n.SubmitFails, SubmitPasses)
			if n.SubmitFails >= SubmitPasses {
				n.SubmitFails = 0
				e.raise(st, Alarm{Alarm: AlarmSubmit, Key: AlarmSubmit + ":" + n.ID, Subject: n.ID,
					Detail: fmt.Sprintf("submit failed %d passes in a row: %v", SubmitPasses, err)})
				e.fail(st, n, "submit", err.Error(), nil)
			}
			continue
		}
		n.SubmitFails = 0
		n.Status, n.Attempt, n.Exp, n.Reason, n.Submitted = NodeSubmitted, attempt, name, "", e.Clock.Now().UTC()
		inflight++
		e.event(n.ID, "submitted", "%s", name)
	}
}

// recheck verifies, just before a node is released, the artifacts of its incoming edges once more: the world may
// have moved since the source ended (a head that advanced, a file that went). A bad one fails the source.
func (e *Engine) recheck(ctx context.Context, st *State, n *NodeState) (map[string]string, bool) {
	arts := map[string]string{}
	for _, ed := range e.incoming(n.Arm) {
		if ed.Artifact == "" {
			continue
		}
		_, id, held := e.upstream(st, n, ed)
		a := held[ed.Artifact]
		var src Source
		if e.Plan.isStep(ed.From) {
			src = Source{ID: id, Step: true, Dir: e.stepDir(id)}
		} else {
			src = e.nodeSource(st.node(id))
		}
		v, err := e.Backend.Verify(ctx, src, Artifact{Kind: ed.Artifact, Path: a.Path}, e.mode(ed))
		switch {
		case err != nil:
			e.logf("recheck %s for %s: %v (next pass)", ed.Artifact, n.ID, err)
			return nil, false
		case !v.OK:
			if u := st.node(id); u != nil {
				u.Artifacts[ed.Artifact].Verified = false
				e.fail(st, u, "artifact:"+ed.Artifact, "at release of "+n.ID+": "+v.Detail, nil)
			} else if s := st.step(id); s != nil {
				s.Status, s.Detail, s.Ended = StepFailed, "artifact "+ed.Artifact+": "+v.Detail, e.Clock.Now().UTC()
			}
			return nil, false
		}
		arts[ed.Artifact] = a.Path
	}
	return arts, true
}
