//go:build integration

package bench

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// The failure-scenario suite drives the real Manager and plan code against a fake world: hosts whose calls can
// succeed, be refused or hang, and steps that the test finishes, kills or leaves running. No real host, ssh or
// process is involved, and time is the manager's injectable clock: nothing sleeps.

// Host modes of the fake world.
const (
	hostUp      = ""
	hostRefuses = "refuse" // ssh answers at once with an error (sshd refuses: Exceeded MaxStartups)
	hostHangs   = "hang"   // the call never returns (a wedged kernel); only the manager's own bound ends the wait
)

type fakeStep struct {
	s     Step
	host  string
	state string // StepRunning, StepFinished or StepDead
	res   Result
}

type fakeWorld struct {
	mu      sync.Mutex
	mode    map[string]string
	steps   map[string]*fakeStep
	started []Step // every Start, in order
	killed  []string
	cleaned []string
	// salvaged are the dead steps whose files were brought home.
	salvaged []string
	free     map[string]int64
	load     map[string]float64 // the 5-minute load each host's probe reports (0: idle)
	release  chan struct{}
	// ledgerReqs is every ledger reconciliation asked of a host; ledgerReleased is what the fake host reports released.
	ledgerReqs     []LedgerReq
	ledgerReleased map[string][]string
}

func newWorld(t *testing.T) *fakeWorld {
	w := &fakeWorld{mode: map[string]string{}, steps: map[string]*fakeStep{}, free: map[string]int64{}, load: map[string]float64{}, release: make(chan struct{})}
	t.Cleanup(func() { close(w.release) })
	return w
}

func (w *fakeWorld) set(host, mode string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.mode[host] = mode
}

// gate is what every call to host meets first.
func (w *fakeWorld) gate(host, verb string) error {
	w.mu.Lock()
	mode := w.mode[host]
	w.mu.Unlock()
	switch {
	case mode == hostRefuses:
		return fmt.Errorf("ssh %s: exit status 255: kex_exchange_identification: Exceeded MaxStartups", host)
	case mode == hostHangs || mode == hostHangs+":"+verb:
		<-w.release
		return errors.New("released")
	}
	return nil
}

func (w *fakeWorld) ops(host string, _ Host) HostOps { return fakeOps{w, host} }

type fakeOps struct {
	w    *fakeWorld
	host string
}

func (o fakeOps) Start(_ context.Context, s Step) error {
	if err := o.w.gate(o.host, "Start"); err != nil {
		return err
	}
	o.w.mu.Lock()
	defer o.w.mu.Unlock()
	o.w.started = append(o.w.started, s)
	o.w.steps[s.ID] = &fakeStep{s: s, host: o.host, state: StepRunning}
	return nil
}

func (o fakeOps) Probe(_ context.Context, steps map[string]string, _ []string) (Probe, error) {
	if err := o.w.gate(o.host, "Probe"); err != nil {
		return Probe{}, err
	}
	o.w.mu.Lock()
	defer o.w.mu.Unlock()
	p := Probe{Steps: map[string]string{}, Free: o.w.free[o.host], Load5: o.w.load[o.host]}
	for lane, id := range steps {
		if fs := o.w.steps[id]; fs != nil && fs.host == o.host {
			p.Steps[lane] = fs.state
		} else {
			p.Steps[lane] = StepDead
		}
	}
	return p, nil
}

func (o fakeOps) Fetch(_ context.Context, _, id, dest string) error {
	if err := o.w.gate(o.host, "Fetch"); err != nil {
		return err
	}
	o.w.mu.Lock()
	fs := o.w.steps[id]
	o.w.mu.Unlock()
	if fs == nil {
		return fmt.Errorf("no step %s", id)
	}
	if err := os.MkdirAll(dest, 0o755); err != nil {
		return err
	}
	return writeJSON(filepath.Join(dest, "result.json"), fs.res)
}

func (o fakeOps) Put(context.Context, string, string, string) error { return o.w.gate(o.host, "Put") }

func (o fakeOps) Salvage(_ context.Context, _, id, dest string) error {
	if err := o.w.gate(o.host, "Salvage"); err != nil {
		return err
	}
	o.w.mu.Lock()
	defer o.w.mu.Unlock()
	o.w.salvaged = append(o.w.salvaged, id)
	return os.MkdirAll(dest, 0o755)
}

func (o fakeOps) Clean(_ context.Context, _, id string) error {
	if err := o.w.gate(o.host, "Clean"); err != nil {
		return err
	}
	o.w.mu.Lock()
	defer o.w.mu.Unlock()
	o.w.cleaned = append(o.w.cleaned, id)
	return nil
}

func (o fakeOps) Stage(_ context.Context, _ Root, in Input) (string, error) {
	if err := o.w.gate(o.host, "Stage"); err != nil {
		return "", err
	}
	return "/staged/" + in.Path, nil
}

func (o fakeOps) GC(context.Context, []string) ([]string, error) { return nil, o.w.gate(o.host, "GC") }

func (o fakeOps) ReconcileLedger(_ context.Context, req LedgerReq) ([]string, error) {
	if err := o.w.gate(o.host, "ReconcileLedger"); err != nil {
		return nil, err
	}
	o.w.mu.Lock()
	defer o.w.mu.Unlock()
	o.w.ledgerReqs = append(o.w.ledgerReqs, req)
	return o.w.ledgerReleased[o.host], nil
}

// Kill is how the manager ends a step that outlived its bound (the HostOps method of that name).
func (o fakeOps) Kill(_ context.Context, _, id string) error {
	if err := o.w.gate(o.host, "Kill"); err != nil {
		return err
	}
	o.w.mu.Lock()
	defer o.w.mu.Unlock()
	o.w.killed = append(o.w.killed, id)
	if fs := o.w.steps[id]; fs != nil {
		fs.state = StepDead
	}
	return nil
}

// finish ends the step with res (kind and times are filled in).
func (w *fakeWorld) finish(id string, res Result) {
	w.mu.Lock()
	defer w.mu.Unlock()
	fs := w.steps[id]
	res.Kind = fs.s.Kind
	if res.Started.IsZero() {
		res.Started, res.Ended = time.Now(), time.Now()
	}
	fs.res, fs.state = res, StepFinished
}

// last is the newest step started of kind on lane ("" kind: any), or nil.
func (w *fakeWorld) last(lane, kind string) *Step {
	w.mu.Lock()
	defer w.mu.Unlock()
	for i := len(w.started) - 1; i >= 0; i-- {
		if s := w.started[i]; s.Lane == lane && (kind == "" || s.Kind == kind) {
			return &s
		}
	}
	return nil
}

// startsOn counts the steps started on host after the first n starts.
func (w *fakeWorld) startsOn(host string, from int) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := 0
	for _, s := range w.started[from:] {
		if s.Host == host {
			n++
		}
	}
	return n
}

func (w *fakeWorld) nStarted() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.started)
}

func (w *fakeWorld) killedIDs() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.killed...)
}

// flab is a bench root with fake hosts, one lane each (h1 has c1, h2 has c2, ...), and a manager on the fake world.
type flab struct {
	t      *testing.T
	root   Root
	w      *fakeWorld
	m      *Manager
	now    time.Time
	dir    string
	specs  string
	hosts  []string
	extra  map[string]string // host -> extra inventory fields
	clusts map[string][]string
	// moreFz adds fuzzops checkouts to hosts.yaml (", 0000000: {h1: /x}"); specExtra adds lines to every spec.
	moreFz, specExtra string
}

var t0 = time.Date(2026, 11, 20, 8, 0, 0, 0, time.UTC)

// newFlab makes a root whose hosts are named, each with the lanes listed ("h1=c1,c2").
func newFlab(t *testing.T, hostLanes ...string) *flab {
	t.Helper()
	dir := t.TempDir()
	l := &flab{t: t, root: Root(filepath.Join(dir, "root")), w: newWorld(t), now: t0, dir: dir, specs: filepath.Join(dir, "in"), extra: map[string]string{}, clusts: map[string][]string{}}
	for _, hl := range hostLanes {
		h, cl, _ := strings.Cut(hl, "=")
		l.hosts = append(l.hosts, h)
		l.clusts[h] = strings.Split(cl, ",")
	}
	must(t, os.MkdirAll(string(l.root), 0o755))
	must(t, os.MkdirAll(l.specs, 0o755))
	must(t, os.WriteFile(filepath.Join(l.specs, "instances.json"), []byte(`{"ids": ["a@x", "b@y"]}`), 0o644))
	l.writeInventory()
	t.Setenv("FAKE_STATE", filepath.Join(dir, "fake"))
	must(t, os.MkdirAll(filepath.Join(dir, "fake"), 0o755))
	// The add-time pin check dials no host: every host's command runs here.
	old := addHostRunner
	addHostRunner = func(Inventory) runFn { return localRunner }
	t.Cleanup(func() { addHostRunner = old })
	l.m = &Manager{Root: l.root, Poll: time.Second, Ops: l.w.ops, Log: os.Stderr}
	l.m.Now = func() time.Time { return l.now }
	return l
}

func (l *flab) writeInventory() {
	var hs strings.Builder
	var fz []string
	for i, h := range l.hosts {
		access := "local: true"
		if i > 0 {
			access = "ssh: fake-" + h // never dialed: the manager's Ops is the fake world
		}
		fmt.Fprintf(&hs, "  %s: {%s, isolation: one-way-ok, staging: %s/stg-%s, max_builds: 1, max_load5: 100000, kubeconfigs: %s/kube, evidence: %s/ev, clusters: [%s]%s}\n",
			h, access, l.dir, h, l.dir, l.dir, strings.Join(l.clusts[h], ", "), l.extra[h])
		fz = append(fz, h+": "+testdata("fakefuzzops"))
	}
	var hk []string
	for _, h := range l.hosts {
		hk = append(hk, h+": "+l.dir)
	}
	inv := "hosts:\n" + hs.String() + "checkouts:\n  fuzzops: {0b528ac: {" + strings.Join(fz, ", ") + "}" + l.moreFz + "}\n  hooks:\n    test: {abcdef0: {" + strings.Join(hk, ", ") + "}}\nhook_command: {test: \"true\"}\n"
	must(l.t, os.WriteFile(l.root.path("hosts.yaml"), []byte(inv), 0o644))
	for _, h := range l.hosts {
		adoptClusters(l.t, filepath.Join(l.dir, "kube"), l.clusts[h]...)
	}
	stubPlatformTools(l.t)
}

// add writes and adds an experiment of the faults ids with one sequence of one run.
func (l *flab) add(name string, hosts []string, ids string, retries int) {
	l.t.Helper()
	must(l.t, l.addErr(name, hosts, ids, retries))
}

// addErr is add that returns the refusal.
func (l *flab) addErr(name string, hosts []string, ids string, retries int) error {
	l.t.Helper()
	spec := fmt.Sprintf(`name: %s
app: examples/shop
fuzzops: 0b528ac
operator: {builtin: none}
namespace: app1
faults:
  instances: instances.json
  ids: [%s]
sequences: [[1]]
run: {window: 1s}
ready_for: 100ms
ready_timeout: 1s
cap: 10m
lanes: %d
hosts: [%s]
retries: %d
pause_after_wave: false
%s`, name, ids, len(l.hosts)*2, strings.Join(hosts, ", "), retries, l.specExtra)
	p := filepath.Join(l.specs, name+".yaml")
	must(l.t, os.WriteFile(p, []byte(spec), 0o644))
	return Add(context.Background(), l.root, p, os.Stderr)
}

// pass is one manager pass at the current fake time.
func (l *flab) pass() {
	l.t.Helper()
	done := make(chan error, 1)
	go func() { done <- l.m.Reconcile(context.Background()) }()
	select {
	case err := <-done:
		must(l.t, err)
	case <-time.After(10 * time.Second): // real time, spent only when a pass blocks on a host
		l.t.Fatalf("a manager pass was still blocked after 10 s: it waits on a host\n%s", readString(l.root.path("bench.log")))
	}
}

// advance moves the fake clock and runs a pass.
func (l *flab) advance(d time.Duration) {
	l.now = l.now.Add(d)
	l.pass()
}

func (l *flab) lane(name string) LaneFile {
	l.t.Helper()
	var lf LaneFile
	must(l.t, readJSON(l.root.LanePath(name), &lf))
	return lf
}

func (l *flab) exp(name string) *Experiment {
	l.t.Helper()
	x, err := l.root.LoadExperiment(name)
	must(l.t, err)
	return x
}

func (l *flab) hostStatus() map[string]HostStatus {
	l.t.Helper()
	var hs map[string]HostStatus
	must(l.t, readJSON(l.root.path("hosts.status.json"), &hs))
	return hs
}

func (l *flab) alarms() []string {
	lines, _ := readJSONL[map[string]any](l.root.path("alarms.jsonl"))
	var out []string
	for _, a := range lines {
		out = append(out, fmt.Sprint(a["alarm"], " ", a["host"], " ", a["detail"]))
	}
	sort.Strings(out)
	return out
}

func (l *flab) hasAlarm(kind, host string) bool {
	for _, a := range l.alarms() {
		if strings.HasPrefix(a, kind+" "+host+" ") {
			return true
		}
	}
	return false
}

// build finishes the build step on lane as good.
func (l *flab) buildOK(lane string) {
	l.t.Helper()
	s := l.w.last(lane, StepBuild)
	if s == nil {
		l.t.Fatalf("no build step on %s", lane)
	}
	p := &Prepared{Experiment: s.Experiment, Namespace: "app1", Ready: true, WorkloadRev: "0b528ac"}
	if s.New != nil { // what the step was given, as the real step returns it
		p.Workload, p.WorkloadRev = s.New.Workload, s.New.WorkloadRev
	}
	l.w.finish(s.ID, Result{OK: true, Prepared: p})
}

// to runs passes until the step of kind on lane exists, failing the test when it never starts.
func (l *flab) runningStep(lane, kind string) *Step {
	l.t.Helper()
	for i := 0; i < 6; i++ {
		l.pass()
		if s := l.w.last(lane, kind); s != nil {
			if lf := l.lane(lane); lf.Step != nil && lf.Step.ID == s.ID {
				return s
			}
		}
	}
	l.t.Fatalf("no %s step running on %s; lane %+v\n%s", kind, lane, l.lane(lane), readString(l.root.path("bench.log")))
	return nil
}

func readString(p string) string {
	b, _ := os.ReadFile(p)
	return string(b)
}

// toRun gets the first sequence of exp running as a run step on lane (build, finish, run).
func (l *flab) toRun(lane string) *Step {
	l.t.Helper()
	l.runningStep(lane, StepBuild)
	l.buildOK(lane)
	return l.runningStep(lane, StepRun)
}

func localRunner(ctx context.Context, _ string, script string) (string, error) {
	out, err := exec.CommandContext(ctx, "sh", "-c", script).Output()
	return string(out), err
}
