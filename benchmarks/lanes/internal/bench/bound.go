package bench

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Bounds on what the manager waits for on a host. Vars so tests can shorten them.
var (
	// probeBound is how long one probe of a host may take before the host counts as unreachable. A host already
	// unreachable is probed with the shorter downProbeBound, so a wedged host costs a pass little.
	probeBound     = 45 * time.Second
	downProbeBound = 10 * time.Second
	// hostCallBound is how long any other call on a host (start, clean, gc, kill) may take.
	hostCallBound = 2 * time.Minute
	// fetchBound is how long copying a finished step, or staging an input, may take.
	fetchBound = 5 * time.Minute
	// hostGrace is how long a host stays unreachable before the steps on it are given up as lost.
	hostGrace = 3 * time.Minute
)

// ErrHostUnreachable is what a call on a host answers at once while the host is marked unreachable.
var ErrHostUnreachable = errors.New("host unreachable")

// bounded runs f and returns its result, or an error naming what when bound passes first. f gets a context that ends
// at the bound; a call that ignores it (a wedged ssh) is abandoned, so the caller never waits on one host for longer
// than bound.
func bounded[T any](ctx context.Context, bound time.Duration, what string, f func(context.Context) (T, error)) (T, error) {
	cctx, cancel := context.WithTimeout(ctx, bound)
	defer cancel()
	type out struct {
		v   T
		err error
	}
	ch := make(chan out, 1)
	go func() {
		v, err := f(cctx)
		ch <- out{v, err}
	}()
	select {
	case o := <-ch:
		return o.v, o.err
	case <-cctx.Done():
		var zero T
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		return zero, fmt.Errorf("%s: no answer within %s", what, bound)
	}
}

func none(err error) (struct{}, error) { return struct{}{}, err }

// guardedOps puts the manager's bounds around a host's operations: every call ends within its bound, and a host the
// manager has marked unreachable answers every call but the probe with ErrHostUnreachable at once, so the pass never
// queues up behind it.
type guardedOps struct {
	in   HostOps
	m    *Manager
	host string
}

func (g guardedOps) down() error {
	if g.m.isDown(g.host) {
		return fmt.Errorf("%s: %w", g.host, ErrHostUnreachable)
	}
	return nil
}

func (g guardedOps) Start(ctx context.Context, s Step) error {
	if err := g.down(); err != nil {
		return err
	}
	_, err := bounded(ctx, hostCallBound, "start on "+g.host, func(c context.Context) (struct{}, error) { return none(g.in.Start(c, s)) })
	return err
}

func (g guardedOps) Probe(ctx context.Context, steps map[string]string, fz []string) (Probe, error) {
	bound := probeBound
	if g.m.isDown(g.host) {
		bound = min(bound, downProbeBound)
	}
	return bounded(ctx, bound, "probe of "+g.host, func(c context.Context) (Probe, error) { return g.in.Probe(c, steps, fz) })
}

func (g guardedOps) Fetch(ctx context.Context, lane, id, dest string) error {
	if err := g.down(); err != nil {
		return err
	}
	_, err := bounded(ctx, fetchBound, "fetch from "+g.host, func(c context.Context) (struct{}, error) { return none(g.in.Fetch(c, lane, id, dest)) })
	return err
}

func (g guardedOps) Put(ctx context.Context, lane, id, src string) error {
	if err := g.down(); err != nil {
		return err
	}
	_, err := bounded(ctx, fetchBound, "put on "+g.host, func(c context.Context) (struct{}, error) { return none(g.in.Put(c, lane, id, src)) })
	return err
}

func (g guardedOps) Salvage(ctx context.Context, lane, id, dest string) error {
	if err := g.down(); err != nil {
		return err
	}
	_, err := bounded(ctx, fetchBound, "salvage from "+g.host, func(c context.Context) (struct{}, error) { return none(g.in.Salvage(c, lane, id, dest)) })
	return err
}

func (g guardedOps) Clean(ctx context.Context, lane, id string) error {
	if err := g.down(); err != nil {
		return err
	}
	_, err := bounded(ctx, hostCallBound, "clean on "+g.host, func(c context.Context) (struct{}, error) { return none(g.in.Clean(c, lane, id)) })
	return err
}

func (g guardedOps) Stage(ctx context.Context, root Root, in Input) (string, error) {
	if err := g.down(); err != nil {
		return "", err
	}
	return bounded(ctx, fetchBound, "stage on "+g.host, func(c context.Context) (string, error) { return g.in.Stage(c, root, in) })
}

func (g guardedOps) GC(ctx context.Context, keep []string) ([]string, error) {
	if err := g.down(); err != nil {
		return nil, err
	}
	return bounded(ctx, hostCallBound, "gc on "+g.host, func(c context.Context) ([]string, error) { return g.in.GC(c, keep) })
}

func (g guardedOps) ReconcileLedger(ctx context.Context, req LedgerReq) ([]string, error) {
	if err := g.down(); err != nil {
		return nil, err
	}
	return bounded(ctx, hostCallBound, "ledger reconcile on "+g.host, func(c context.Context) ([]string, error) { return g.in.ReconcileLedger(c, req) })
}

func (g guardedOps) Kill(ctx context.Context, lane, id string) error {
	if err := g.down(); err != nil {
		return err
	}
	_, err := bounded(ctx, hostCallBound, "kill on "+g.host, func(c context.Context) (struct{}, error) { return none(g.in.Kill(c, lane, id)) })
	return err
}

// stepKillGrace is how many seconds the manager gives a killed step's processes, after TERM, to undo what they were
// doing before the rest are killed.
const stepKillGrace = 20

// killScript ends the step whose step.json is at stepPath and everything it started: TERM to its whole session
// (the step is a session of its own, from `setsid`; the workload adapter runs in a process group of its own inside
// it, so a signal to the step's group alone never reaches the adapter and leaves its run going on the lane), and KILL
// to what is still in the session after grace seconds. It finds the step by its command line (`... step <stepPath>`),
// so it needs nothing from the pinned binary. The bracket keeps pgrep from matching this shell.
func killScript(stepPath string, grace int) string {
	n := len(stepPath) - 1
	pat := stepPath[:n] + "[" + stepPath[n:] + "]"
	return fmt.Sprintf(`pid=$(pgrep -f -- %s | head -n 1); [ -n "$pid" ] || exit 0; `+
		`pkill -TERM -s $pid 2>/dev/null; kill -TERM -- -$pid 2>/dev/null; i=0; `+
		`while [ $i -lt %d ] && pgrep -s $pid >/dev/null 2>&1; do sleep 1; i=$((i+1)); done; `+
		`if pgrep -s $pid >/dev/null 2>&1; then pkill -KILL -s $pid 2>/dev/null; kill -KILL -- -$pid 2>/dev/null; fi; true`,
		shellQuote(pat), grace)
}

// Alarms the manager raises on its own (plan alarms are raised by the plan engine).
const (
	AlarmHostUnreachable = "host-unreachable"
	AlarmDiskFloor       = "disk-floor"
)

// alarm appends an alarm to alarms.jsonl once while its condition stands (until alarmClear), where `lanes status`
// shows it.
func (m *Manager) alarm(kind, host, detail string) {
	key := kind + "/" + host
	if m.alarmed[key] {
		return
	}
	if m.alarmed == nil {
		m.alarmed = map[string]bool{}
	}
	m.alarmed[key] = true
	m.logf("ALARM %s %s: %s", kind, host, detail)
	_ = appendJSONL(m.Root.path("alarms.jsonl"), map[string]any{"at": m.now().UTC(), "alarm": kind, "key": key, "subject": host, "host": host, "detail": detail})
}

func (m *Manager) alarmClear(kind, host string) {
	delete(m.alarmed, kind+"/"+host)
}

func (m *Manager) isDown(host string) bool {
	m.downMu.Lock()
	defer m.downMu.Unlock()
	_, ok := m.down[host]
	return ok
}

// stepBound is how long a step of kind may run before the manager kills it: far above what a healthy step takes (the
// hooks' own limits sum to less), so only a hung step reaches it.
func stepBound(kind string, s Spec) time.Duration {
	_, rt, cap := s.Durations()
	switch kind {
	case StepRun:
		return cap + 2*rt + 45*time.Minute
	case StepBuild:
		return 4*time.Hour + rt
	case StepRelease:
		// The teardown's idle wait lasts at most the cap and the cluster delete (45 minutes at most) follows it: a
		// bound at the cap would kill a release that is only waiting as long as it may, and the lane's nodes would
		// bill for want of its cluster delete.
		return max(2*time.Hour, cap+time.Hour)
	case StepVerify:
		return hookTimeouts[verbVerifyEvidence] + 15*time.Minute
	}
	return 2 * time.Hour
}
