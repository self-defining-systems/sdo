package plan

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeClock is a manually advanced clock.
type fakeClock struct{ t time.Time }

func newClock() *fakeClock               { return &fakeClock{t: time.Date(2026, 11, 20, 8, 0, 0, 0, time.UTC)} }
func (c *fakeClock) Now() time.Time      { return c.t }
func (c *fakeClock) Add(d time.Duration) { c.t = c.t.Add(d) }

// fakeBackend is the experiments and steps of a fake platform: nothing runs until the test finishes it.
type fakeBackend struct {
	mu       sync.Mutex
	subs     []Job
	exps     map[string]Observation
	verdicts map[string]Verdict // by "kind@exp"; absent: OK
	verifies []string
	steps    map[string]StepObservation
	started  []StepJob
	exportOK bool
}

func newFake() *fakeBackend {
	return &fakeBackend{exps: map[string]Observation{}, verdicts: map[string]Verdict{}, steps: map[string]StepObservation{}}
}

func (f *fakeBackend) Submit(_ context.Context, j Job) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.exps[j.Exp]; ok {
		return ErrExists
	}
	f.subs = append(f.subs, j)
	f.exps[j.Exp] = Observation{Phase: PhaseQueued}
	return nil
}

func (f *fakeBackend) Observe(_ context.Context, exp string) (Observation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	o, ok := f.exps[exp]
	if !ok {
		return Observation{Phase: PhaseAbsent}, nil
	}
	return o, nil
}

func (f *fakeBackend) Export(_ context.Context, src Source, kind, tmpl string) (Artifact, error) {
	return Artifact{Kind: kind, Path: fmt.Sprintf("/art/%s/%s", srcKey(src), kind)}, nil
}

func (f *fakeBackend) Verify(_ context.Context, src Source, a Artifact, mode string) (Verdict, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.verifies = append(f.verifies, a.Kind+"@"+srcKey(src))
	if v, ok := f.verdicts[a.Kind+"@"+srcKey(src)]; ok {
		return v, nil
	}
	return Verdict{OK: true}, nil
}

func (f *fakeBackend) StartStep(_ context.Context, j StepJob) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.started = append(f.started, j)
	f.steps[j.ID] = StepObservation{Phase: PhaseRunning}
	return nil
}

func (f *fakeBackend) ObserveStep(_ context.Context, dir string) (StepObservation, error) {
	id := stepIDOf(dir)
	f.mu.Lock()
	defer f.mu.Unlock()
	if o, ok := f.steps[id]; ok {
		return o, nil
	}
	return StepObservation{Phase: PhaseAbsent}, nil
}

// finish ends the experiment of node (found by its job) as phase.
func (f *fakeBackend) finish(exp, phase, reason string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.exps[exp] = Observation{Phase: phase, Reason: reason}
}

// names returns the experiments submitted so far, in order.
func (f *fakeBackend) nodes() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, j := range f.subs {
		out = append(out, j.Node)
	}
	return out
}

func (f *fakeBackend) expOf(node string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := len(f.subs) - 1; i >= 0; i-- {
		if f.subs[i].Node == node {
			return f.subs[i].Exp
		}
	}
	return ""
}

// lab is a plan directory with templates on disk.
type lab struct {
	t     *testing.T
	root  string
	clock *fakeClock
	fb    *fakeBackend
}

func newLab(t *testing.T) *lab {
	return &lab{t: t, root: t.TempDir(), clock: newClock(), fb: newFake()}
}

func (l *lab) write(rel, content string) string {
	p := filepath.Join(l.root, rel)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		l.t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		l.t.Fatal(err)
	}
	return p
}

// add writes plan.yaml (and the spec templates every arm shares) and adds the plan.
func (l *lab) add(planYAML string) *Engine {
	l.t.Helper()
	l.write("tpl.yaml", `{"name": "x", "app": "examples/${item}", "operator": {"hooks": "h", "seed": "${artifact.memory}"}, "faults": {"instances": "inst.json", "ids": ["${item}"]}, "marker": "${arm}-${rep}"}`)
	l.write("tpl-plain.yaml", `{"name": "x", "app": "examples/${item}", "operator": {"hooks": "h"}, "faults": {"ids": ["${item}"]}}`)
	file := l.write("plan.yaml", planYAML)
	if _, err := Add(filepath.Join(l.root, "plans"), file, l.clock, nil); err != nil {
		l.t.Fatal(err)
	}
	return l.open()
}

func (l *lab) open() *Engine {
	l.t.Helper()
	e, err := Open(filepath.Join(l.root, "plans", "tp"), l.fb, l.clock)
	if err != nil {
		l.t.Fatal(err)
	}
	return e
}

func (l *lab) tick(e *Engine) {
	l.t.Helper()
	if err := e.Tick(context.Background()); err != nil {
		l.t.Fatal(err)
	}
}

func (l *lab) state() *State {
	l.t.Helper()
	st, err := LoadState(filepath.Join(l.root, "plans", "tp"))
	if err != nil {
		l.t.Fatal(err)
	}
	return st
}

var nameRE40 = regexpMust(`^[a-z0-9][a-z0-9-]{0,39}$`)

func regexpMust(s string) *regexp.Regexp { return regexp.MustCompile(s) }

func srcKey(s Source) string {
	if s.Exp != "" {
		return s.Exp
	}
	return s.ID
}

// stepIDOf maps a step directory back to its id (the fake keys steps by id).
func stepIDOf(dir string) string {
	base := filepath.Base(dir)
	base = strings.Replace(base, "step_", "step:", 1)
	return strings.Replace(base, "_r", "/r", 1)
}
