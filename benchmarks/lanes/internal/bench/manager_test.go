//go:build integration

package bench

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// The conformance tests run real detached steps (the fuzzops-bench binary)
// against a fake fuzzops whose faults are marker files, and the shell hook in
// testdata.

// benchBin and pkgDir are set once by TestMain and only read afterwards.
var (
	benchBin string
	pkgDir   string // the package directory, resolved once: no test asks the OS for its working directory
)

// testdata is the absolute path of a file under this package's testdata directory.
func testdata(rel string) string { return filepath.Join(pkgDir, "testdata", rel) }

func TestMain(m *testing.M) {
	var err error
	if pkgDir, err = os.Getwd(); err != nil {
		panic(err)
	}
	dir, err := os.MkdirTemp("", "bench-bin-")
	if err != nil {
		panic(err)
	}
	benchBin = filepath.Join(dir, "fuzzops-bench")
	if out, err := exec.Command("go", "build", "-o", benchBin, "../../cmd/lanes").CombinedOutput(); err != nil {
		panic(fmt.Sprintf("build fuzzops-bench: %v\n%s", err, out))
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

type lab struct {
	t       *testing.T
	root    Root
	state   string // FAKE_STATE
	hooks   string // HOOK_STATE
	specs   string
	m       *Manager
	hooked  string // the operator block of a hooked experiment (the lab's hooks checkout)
	cap     string // the cap of the experiments added next; labCap unless a test shortens it
	staging string // the host's staging directory
	ready   string // their ready_timeout; labReadyTimeout unless a test needs a check to turn clean over time
}

// The spec's cap is a budget a run must stay inside; a test that waits for an
// outcome takes it from the lab, generous enough that a loaded host never
// spends it, and a test that needs it spent (the cap) shortens it and makes the
// state that spends it permanent, never a stretch of time that happens to pass.
// ready_timeout is the same for a check that has to turn clean (an operator
// still starting); the default is short because several tests expect a check
// that never turns clean to give up, and the lab's tests pay it each time.
const (
	labReadyTimeout = "1s"
	labCap          = "10m"
)

const labSpec = `name: %s
app: examples/shop
fuzzops: 0b528ac
operator: %s
namespace: app1
faults:
  instances: instances.json
  ids: [%s]
sequences: %s
run: {window: 1s}
ready_for: 100ms
ready_timeout: %s
cap: %s
lanes: 1
hosts: [h1]
retries: %d
pause_after_wave: false
`

func newLab(t *testing.T) *lab {
	t.Helper()
	dir := t.TempDir()
	l := &lab{t: t, staging: filepath.Join(dir, "staging"), cap: labCap, ready: labReadyTimeout, root: Root(filepath.Join(dir, "root")), state: filepath.Join(dir, "fake"), hooks: filepath.Join(dir, "hookstate"), specs: filepath.Join(dir, "in")}
	for _, d := range []string{string(l.root), l.state, l.hooks, filepath.Join(l.specs, "base")} {
		must(t, os.MkdirAll(d, 0o755))
	}
	fake := testdata("fakefuzzops")
	hook := testdata("hook.sh")
	adoptClusters(t, filepath.Join(dir, "kube"), "c1")
	stubPlatformTools(t)
	inv := `hosts:
  h1: {local: true, isolation: one-way-ok, staging: ` + dir + `/staging, max_builds: 1, max_load5: 100000, kubeconfigs: ` + dir + `/kube, evidence: ` + dir + `/evidence, clusters: [c1]}
checkouts:
  fuzzops: {0b528ac: {h1: ` + fake + `}}
  hooks:
    test: {HEAD: {h1: ` + dir + `}}
hook_command: {test: "` + hook + `"}
`
	must(t, os.WriteFile(filepath.Join(l.specs, "instances.json"), []byte(`{"ids": ["a@x", "b@y"]}`), 0o644))
	must(t, os.WriteFile(filepath.Join(l.specs, "base", "baseline.json"), []byte(`{}`), 0o644))
	t.Setenv("FAKE_STATE", l.state)
	t.Setenv("HOST_CANARY_PROCSYS", testdata("fakeprocsys")) // a lab host meets every kernel minimum
	fakebin := testdata("fakebin")
	t.Setenv("PATH", fakebin+string(os.PathListSeparator)+os.Getenv("PATH"))
	must(t, os.MkdirAll(dir, 0o755))
	t.Setenv("HOOK_STATE", l.hooks)
	t.Setenv("HOOK_LOG", filepath.Join(l.state, "hooks.log"))
	for _, c := range [][]string{{"init", "-q"}, {"-c", "user.email=t@t", "-c", "user.name=t", "commit", "-q", "--allow-empty", "-m", "hooks"}} {
		must(t, exec.Command("git", append([]string{"-C", dir}, c...)...).Run())
	}
	head, err := exec.Command("git", "-C", dir, "rev-parse", "HEAD").Output()
	must(t, err)
	l.hooked = "{hooks: test, revision: " + strings.TrimSpace(string(head)) + ", baseline: base, settings: {startup_bound: 0s}}"
	rev := strings.TrimSuffix(strings.TrimPrefix(l.hooked, "{hooks: test, revision: "), ", baseline: base, settings: {startup_bound: 0s}}")
	must(t, os.WriteFile(l.root.path("hosts.yaml"), []byte(strings.Replace(inv, "HEAD", rev, 1)), 0o644))
	l.m = &Manager{Root: l.root, Poll: 50 * time.Millisecond, Bin: benchBin}
	// Before the temporary directories go: let the lane's last step (a release) finish.
	t.Cleanup(func() {
		for end := giveUp(t); time.Now().Before(end); time.Sleep(50 * time.Millisecond) {
			if lf := l.lane(); lf.Step == nil && (lf.State == LaneFree || lf.State == LaneDown || lf.State == LaneRetired) && (lf.Prepared == nil || lf.Prepared.Hooks == "" || lf.Prepared.Reset || lf.State == LaneDown || lf.State == LaneRetired) {
				return
			} else if l.held(lf) { // a pool lane stays ready for good: nothing is left to wait for
				return
			}
			_ = l.m.Reconcile(context.Background())
		}
	})
	return l
}

// ageStep makes a killed step count as dead now. A step that does not hold its
// lock yet counts as starting for startGrace after step.json was written; a
// test that kills one says its start is long past instead of waiting out the
// grace, which a loaded host needs long (a step must get to take its lock).
func (l *lab) ageStep(id string) {
	l.t.Helper()
	old := time.Now().Add(-time.Hour)
	must(l.t, os.Chtimes(filepath.Join(l.staging, "c1", id, "step.json"), old, old))
}

// held reports a ready lane of a Hold experiment with no step in flight: it is
// kept that way until its experiment is removed.
func (l *lab) held(lf LaneFile) bool {
	if lf.State != LaneReady || lf.Step != nil || lf.Experiment == "" {
		return false
	}
	x, err := l.root.LoadExperiment(lf.Experiment)
	return err == nil && x.Spec != nil && x.Measured().Hold
}

func (l *lab) add(name, op, ids, seqs string, retries int) {
	l.t.Helper()
	p := filepath.Join(l.specs, name+".yaml")
	must(l.t, os.WriteFile(p, []byte(fmt.Sprintf(labSpec, name, op, ids, seqs, l.ready, l.cap, retries)), 0o644))
	must(l.t, Add(context.Background(), l.root, p, io.Discard))
}

// addReusing adds a hooked experiment of two sequences (a@x, b@y) with spec lines appended.
func (l *lab) addReusing(name, extra string) {
	l.t.Helper()
	p := filepath.Join(l.specs, name+".yaml")
	must(l.t, os.WriteFile(p, []byte(fmt.Sprintf(labSpec, name, l.hooked, "a@x, b@y", "[[1]]", l.ready, l.cap, 0)+extra), 0o644))
	must(l.t, Add(context.Background(), l.root, p, io.Discard))
}

// setHook writes a file of the conformance hook's state ($HOOK_STATE, see testdata/hook.sh).
func (l *lab) setHook(file, content string) {
	must(l.t, os.WriteFile(filepath.Join(l.hooks, file), []byte(content), 0o644))
}

func (l *lab) set(file, content string) {
	must(l.t, os.WriteFile(filepath.Join(l.state, file), []byte(content), 0o644))
}

// maxWait is the most one wait may take. It is far above what a slow host needs (the slowest tests take seconds
// at a load of 250), so only a state that never comes reaches it; a hang then costs a few minutes of the package's
// budget, not all of it, and the tests after it still run.
const maxWait = 5 * time.Minute

// giveUp is when a wait fails with its diagnostics instead of hanging: maxWait from now, and never later than
// shortly before the package's own test timeout (CI sets one below its job limit), so the diagnostics are
// printed before anything cancels the run.
func giveUp(t *testing.T) time.Time {
	end := time.Now().Add(maxWait)
	if d, ok := t.Deadline(); ok && d.Add(-10*time.Second).Before(end) {
		end = d.Add(-10 * time.Second)
	}
	return end
}

// until reconciles until cond holds (or fails the test with the lab's logs when giveUp passes).
func (l *lab) until(what string, cond func() bool) {
	l.t.Helper()
	deadline := giveUp(l.t)
	for !cond() {
		if time.Now().After(deadline) {
			b, _ := os.ReadFile(l.root.path("bench.log"))
			l.t.Fatalf("timed out waiting for %s\nbench.log:\n%s\nfuzzops.log:\n%s", what, b, l.read("fuzzops.log"))
		}
		must(l.t, l.m.Reconcile(context.Background()))
		time.Sleep(30 * time.Millisecond)
	}
}

func (l *lab) read(file string) string {
	b, _ := os.ReadFile(filepath.Join(l.state, file))
	return string(b)
}

func (l *lab) readRoot(file string) string {
	b, _ := os.ReadFile(l.root.path(file))
	return string(b)
}

func (l *lab) lane() LaneFile {
	var lf LaneFile
	_ = readJSON(l.root.LanePath("c1"), &lf)
	return lf
}

func (l *lab) readPath(p string) string {
	b, _ := os.ReadFile(p)
	return string(b)
}

func (l *lab) exp(name string) *Experiment {
	x, err := l.root.LoadExperiment(name)
	must(l.t, err)
	return x
}

func (l *lab) noFault() {
	l.t.Helper()
	if ms, _ := filepath.Glob(filepath.Join(l.state, "marker-*")); len(ms) > 0 {
		l.t.Fatalf("fault left in place: %v", ms)
	}
}

func idle(l *lab) func() bool {
	return func() bool {
		lf := l.lane()
		return lf.Step == nil && lf.State == LaneFree && (lf.Prepared == nil || lf.Prepared.Hooks == "")
	}
}

// A host's env reaches the hooks, so a host can name its own registry port without the hooks knowing the host.
func TestBenchHostEnvReachesTheHooks(t *testing.T) {
	l := newLab(t)
	inv, _ := os.ReadFile(l.root.path("hosts.yaml"))
	must(t, os.WriteFile(l.root.path("hosts.yaml"), []byte(strings.Replace(string(inv), "h1: {local: true,", "h1: {env: {FUZZOPS_REGISTRY_PORT: \"5999\"}, local: true,", 1)), 0o644))
	l.add("e1", l.hooked, "a@x", "[[1]]", 0)
	l.until("run done", func() bool { return l.exp("e1").Count(0).Done == 1 })
	l.until("lane released", idle(l))
	b, err := os.ReadFile(filepath.Join(l.hooks, "regport"))
	if err != nil || strings.TrimSpace(string(b)) != "5999" {
		t.Fatalf("FUZZOPS_REGISTRY_PORT = %q (%v)", b, err)
	}
	l.noFault()
}

// The hooks get the pinned checkout's host registry script, so an operator's images are pushed once per host
// instead of loaded into every node.
func TestBenchHooksKnowTheHostRegistryScript(t *testing.T) {
	l := newLab(t)
	l.add("e1", l.hooked, "a@x", "[[1]]", 0)
	l.until("run done", func() bool { return l.exp("e1").Count(0).Done == 1 })
	l.until("lane released", idle(l))
	b, err := os.ReadFile(filepath.Join(l.hooks, "registry"))
	// The lanes install's script, never the workload checkout's.
	got := strings.TrimSpace(string(b))
	if err != nil || !strings.Contains(got, string(filepath.Separator)+platformPrefix) || !strings.HasSuffix(got, filepath.Join("deploy", "registry.sh")) ||
		strings.HasPrefix(got, testdata("fakefuzzops")) {
		t.Fatalf("BENCH_REGISTRY = %q (%v)", b, err)
	}
	l.noFault()
}

// A sequence runs first and repeat on one build; results are home and the lane is released.
func TestBenchRunsASequenceAndReleasesTheLane(t *testing.T) {
	l := newLab(t)
	l.add("e1", l.hooked, "a@x", "[[1, 2]]", 0)
	l.until("both runs done", func() bool { return l.exp("e1").Count(0).Done == 2 })
	l.until("lane released", idle(l))
	x := l.exp("e1")
	r2 := x.Runs[RunID("a@x", 1, 2, 1)]
	if r2.Seed != 2 || r2.OperatorLabel != "exec:test-e1-r2" || r2.InjectAt == nil || r2.SettleEnd == nil || r2.Fuzzops.MeasureVersion != 12 {
		t.Fatalf("run 2: %+v", r2)
	}
	if b, err := os.ReadFile(filepath.Join(l.root.RunDir("e1", r2.ID), "operator", "collected.txt")); err != nil || !strings.Contains(string(b), r2.ID) {
		t.Fatalf("collect output: %v %s", err, b)
	}
	if n := strings.Count(l.read("fuzzops.log"), "app up"); n != 1 {
		t.Fatalf("%d builds for one sequence", n)
	}
	hooks := l.read("hooks.log")
	if !strings.Contains(hooks, "prepare c1 e1") || !strings.HasSuffix(strings.TrimSpace(hooks), "teardown c1 e1\ntranscripts c1 e1") {
		t.Fatalf("hooks:\n%s", hooks)
	}
	ivs, _ := readJSONL[Interval](l.root.TimelinePath("c1"))
	kinds := map[string]bool{}
	for _, iv := range ivs {
		kinds[iv.Kind] = true
	}
	for _, k := range []string{"app", "prepare", "check", "run", "settle", "collect", "safety", "teardown"} {
		if !kinds[k] {
			t.Errorf("timeline has no %s interval", k)
		}
	}
	l.noFault()
}

// An alarm at the gate injects nothing: the run stays queued, the lane is
// rebuilt, and the second not-ready gate in a row takes the lane down.
func TestBenchFailedGateInjectsNothing(t *testing.T) {
	l := newLab(t)
	l.set("busy-at-gate", "alarm: incident open")
	l.add("e1", l.hooked, "a@x", "[[1]]", 0)
	l.until("lane down", func() bool { return l.lane().State == LaneDown })
	if strings.Contains(l.read("fuzzops.log"), "inject") {
		t.Fatalf("a fault was injected:\n%s", l.read("fuzzops.log"))
	}
	if !strings.Contains(l.lane().Reason, "2 not-ready gates") || strings.Count(l.read("fuzzops.log"), "app up") != 2 {
		t.Fatalf("lane %+v\n%s", l.lane(), l.read("fuzzops.log"))
	}
	c := l.exp("e1").Count(0)
	if c.Queued != 1 || c.Invalid != 0 {
		t.Fatalf("counts %+v", c)
	}
	gates, _ := readJSONL[Gate](l.root.GatesPath("e1"))
	if len(gates) != 2 || !gates[0].Alarm || gates[0].Where != "gate" {
		t.Fatalf("gates %+v", gates)
	}
}

// cleanFails is a host whose staging cleanup fails (an ssh that drops, a step still writing into its directory).
type cleanFails struct{ HostOps }

func (cleanFails) Clean(context.Context, string, string) error { return errors.New("host unreachable") }

// A finished step is adopted whether or not its staging directory can be removed afterwards: a failed cleanup
// is logged and left for gc, never a panic of the manager and never a step adopted twice.
func TestBenchFailedCleanupDoesNotUndoAnAdoptedStep(t *testing.T) {
	l := newLab(t)
	l.m.Ops = func(_ string, h Host) HostOps { return cleanFails{localHost{staging: h.Staging, bin: benchBin}} }
	l.set("busy-at-gate", "alarm: incident open")
	l.add("e1", l.hooked, "a@x", "[[1]]", 0)
	l.until("lane down", func() bool { return l.lane().State == LaneDown })
	// Two builds fail their gate (the first is rebuilt, the second takes the lane down), each adopted once.
	if log := l.readRoot("bench.log"); !strings.Contains(log, "host unreachable") || strings.Count(log, "finished ok=false") != 2 {
		t.Fatalf("the cleanup failure is logged and each of the two builds adopted once:\n%s", log)
	}
}

// A run whose operator is still busy at the cap is invalid "capped", breaks
// its sequence, and the lane is not reused busy.
func TestBenchCapEndsTheSequenceAndRebuilds(t *testing.T) {
	if testing.Short() {
		t.Skip("real detached steps take several seconds: nightly tier")
	}
	l := newLab(t)
	l.set("busy-after-inject", "busy: reflection queued") // never clears: the cap is spent whatever the host's speed
	l.cap = "2s"
	l.add("e1", l.hooked, "a@x", "[[1, 2]]", 0)
	l.until("sequence ended", func() bool { return l.exp("e1").Progress(Seq{"a@x", 1}).Ended() })
	x := l.exp("e1")
	r1, r2 := x.Runs[RunID("a@x", 1, 1, 1)], x.Runs[RunID("a@x", 1, 2, 1)]
	if !r1.Capped || r1.Status != StatusInvalid || r1.Reason != "capped: busy: reflection queued" || !strings.HasPrefix(r2.Reason, "sequence broken: capped") {
		t.Fatalf("r1 %+v\nr2 %+v", r1, r2)
	}
	l.until("lane released", idle(l))
	if strings.Count(l.read("fuzzops.log"), "inject") != 1 {
		t.Fatalf("the busy lane ran again:\n%s", l.read("fuzzops.log"))
	}
}

// remove mid-run: the run in flight finishes (fault undone), the rest is dropped, the lane released.
func TestBenchRemoveMidRun(t *testing.T) {
	l := newLab(t)
	l.set("sleep", "2")
	l.add("e1", l.hooked, "a@x, b@y", "[[1, 2]]", 0)
	l.until("running", func() bool { return l.lane().State == LaneRunning })
	must(t, Remove(l.root, "e1", false))
	l.until("lane released", idle(l))
	c := l.exp("e1").Count(0)
	if c.Done != 1 || c.Dropped != 3 || c.Queued != 0 {
		t.Fatalf("counts %+v", c)
	}
	l.noFault()
}

// SIGKILL of a step (and a manager restart): the next manager recovers the
// lane, undoes the fault, marks the run lost and retries the sequence.
func TestBenchKilledStepIsRecovered(t *testing.T) {
	l := newLab(t)
	l.set("sleep", "30")
	l.add("e1", l.hooked, "a@x", "[[1]]", 1)
	l.until("injected", func() bool { return strings.Contains(l.read("fuzzops.log"), "inject") })
	step := l.lane().Step.ID
	out, _ := exec.Command("pgrep", "-f", step).Output()
	for _, pid := range strings.Fields(string(out)) {
		_ = exec.Command("kill", "-9", pid).Run()
	}
	l.ageStep(step)
	if b, err := os.ReadFile(filepath.Join(l.state, "run.pid")); err == nil {
		_ = syscall.Kill(-atoi(strings.TrimSpace(string(b))), syscall.SIGKILL) // the run and its children
	}
	os.Remove(filepath.Join(l.state, "lane-held")) // its lane lock dies with it
	if ms, _ := filepath.Glob(filepath.Join(l.state, "marker-*")); len(ms) == 0 {
		t.Fatal("test setup: the killed run left no fault")
	}
	os.Remove(filepath.Join(l.state, "sleep"))
	l.m = &Manager{Root: l.root, Poll: 50 * time.Millisecond, Bin: benchBin} // a restarted manager
	l.until("retry done", func() bool { return l.exp("e1").Progress(Seq{"a@x", 1}).State == SeqDone })
	x := l.exp("e1")
	if r := x.Runs[RunID("a@x", 1, 1, 1)]; r.Reason != "lane lost: step died" {
		t.Fatalf("lost run %+v", r)
	}
	l.until("lane released", idle(l))
	l.noFault()
}

// A build the manager cancelled (its experiment was removed with --cancel) dies inside `app up`, which is not
// re-entrant: it leaves the application half installed on the cluster this very build made, and an `up` on that
// install fails for ever (a second failed build takes the lane down for a human reset). The next build makes the
// cluster anew, as after any other failed build on a reused one.
func TestBenchCancelledBuildLeavesTheLaneToBeRecreated(t *testing.T) {
	if testing.Short() {
		t.Skip("real detached steps take several seconds: nightly tier")
	}
	l := newLab(t)
	l.withClusters("1K")
	l.set("app-up-sleep", "30")
	l.addReusing("e1", "keep_lanes: true\n")
	l.until("app up running", func() bool {
		return l.lane().State == LaneBuilding && strings.Contains(l.read("fuzzops.log"), "app up")
	})
	must(t, RemoveCancel(l.root, "e1"))
	l.until("lane free", func() bool { lf := l.lane(); return lf.Step == nil && lf.State == LaneFree })
	if lf := l.lane(); !lf.Recreate {
		t.Fatalf("the next build reuses the cluster of a build that was killed mid-install: %+v\n%s", lf, l.readRoot("bench.log"))
	}
}

// A fault the safety check cannot undo takes the lane down and keeps it out.
func TestBenchStuckFaultTakesTheLaneDown(t *testing.T) {
	l := newLab(t)
	l.set("sleep", "30")
	l.add("e1", l.hooked, "a@x", "[[1]]", 1)
	l.until("injected", func() bool { return strings.Contains(l.read("fuzzops.log"), "inject") })
	step := l.lane().Step.ID
	out, _ := exec.Command("pgrep", "-f", step).Output()
	for _, pid := range strings.Fields(string(out)) {
		_ = syscall.Kill(atoi(pid), syscall.SIGKILL)
	}
	l.ageStep(step)
	if b, err := os.ReadFile(filepath.Join(l.state, "run.pid")); err == nil {
		_ = syscall.Kill(-atoi(strings.TrimSpace(string(b))), syscall.SIGKILL)
	}
	os.Remove(filepath.Join(l.state, "lane-held"))
	l.set("stuck", "")
	l.until("lane down", func() bool { return l.lane().State == LaneDown })
	if !strings.Contains(l.lane().Reason, "fault still active") {
		t.Fatalf("lane %+v", l.lane())
	}
	before := strings.Count(l.read("fuzzops.log"), "app up")
	for i := 0; i < 5; i++ {
		must(t, l.m.Reconcile(context.Background()))
	}
	if strings.Count(l.read("fuzzops.log"), "app up") != before {
		t.Fatal("a down lane was used")
	}
}

func atoi(s string) int {
	n := 0
	fmt.Sscan(s, &n)
	return n
}

// A builtin experiment that takes a lane a hooked one used tears the hooked operator down first.
func TestBenchBuiltinAfterHookedTearsTheOperatorDown(t *testing.T) {
	l := newLab(t)
	l.add("e1", l.hooked, "a@x", "[[1]]", 0)
	l.until("e1 running", func() bool { return l.lane().State == LaneRunning })
	// e2 waits for the only lane: its build gets the lane with e1's operator still on it.
	l.add("e2", "{builtin: noop}", "a@x", "[[1], [2]]", 0)
	l.until("e2 done", func() bool { return l.exp("e2").Count(0).Done == 2 })
	hooks := l.read("hooks.log")
	if !strings.Contains(hooks, "teardown c1 e1") || strings.Count(l.read("fuzzops.log"), "app up") != 3 {
		t.Fatalf("hooked operator never torn down:\n%s", hooks)
	}
	after := hooks[strings.LastIndex(hooks, "collect c1 e1"):]
	ivs, _ := readJSONL[Interval](l.root.TimelinePath("c1"))
	for _, iv := range ivs {
		if strings.HasPrefix(iv.Step, StepRelease) {
			t.Fatalf("e1's lane was released, not rebuilt: %+v", iv)
		}
	}
	if !strings.Contains(after, "teardown c1 e1") {
		t.Fatalf("e2's build did not tear down e1's operator:\n%s", hooks)
	}
	if r := l.exp("e2").Runs[RunID("a@x", 2, 1, 1)]; r.OperatorLabel != "noop" || r.Seed != 2 {
		t.Fatalf("noop run %+v", r)
	}
	l.until("lane idle", idle(l))
	l.noFault()
}

// A killed step whose fuzzops run lives on: recover waits for that run (it
// holds the lane and undoes its own fault), then the lane goes on.
func TestBenchOrphanedRunIsWaitedFor(t *testing.T) {
	if testing.Short() {
		t.Skip("real detached steps take several seconds: nightly tier")
	}
	l := newLab(t)
	l.set("sleep", "3")
	l.add("e1", l.hooked, "a@x", "[[1]]", 1)
	l.until("injected", func() bool { return strings.Contains(l.read("fuzzops.log"), "inject") })
	step := l.lane().Step.ID
	out, _ := exec.Command("pgrep", "-f", step).Output()
	for _, pid := range strings.Fields(string(out)) {
		_ = syscall.Kill(atoi(pid), syscall.SIGKILL) // the step runner only
	}
	l.ageStep(step)
	l.until("retry done", func() bool { return l.exp("e1").Progress(Seq{"a@x", 1}).State == SeqDone })
	if l.lane().State == LaneDown {
		t.Fatalf("lane down: %+v", l.lane())
	}
	l.noFault()
}

// The app must have been clean for ready_for with no operator on the lane before the operator is restored: an
// operator that watches an app still warming up would act on a real failing probe.
func TestBenchOperatorIsRestoredOnlyOnAnAppThatWasClean(t *testing.T) {
	l := newLab(t)
	l.add("e1", l.hooked, "a@x", "[[1]]", 0)
	l.until("run done", func() bool { return l.exp("e1").Count(0).Done == 1 })
	log := l.read("fuzzops.log")
	without, with := strings.Index(log, "check-without-operator"), strings.Index(log, "check-with-operator")
	if without < 0 || with < 0 || without > with {
		t.Fatalf("the journeys must be checked on the bare app first:\n%s", log)
	}
	l.until("lane idle", idle(l))
}

// An app that never gets clean is never given an operator, and the build says so.
func TestBenchNoOperatorIsRestoredOnAnUnhealthyApp(t *testing.T) {
	l := newLab(t)
	l.set("app-unhealthy", "")
	l.add("e1", l.hooked, "a@x", "[[1]]", 0)
	l.until("a build failed", func() bool { return strings.Contains(l.lane().Reason, "app not clean") || l.lane().State == LaneDown })
	if strings.Contains(l.read("hooks.log"), "prepare") {
		t.Fatalf("an operator was restored on an unhealthy app:\n%s", l.read("hooks.log"))
	}
	if !strings.Contains(l.lane().Reason, "app not clean") {
		t.Fatalf("lane %+v", l.lane())
	}
}

// The bare-app gate tolerates the build check's few bad seconds: a lone conflicting write is not an unhealthy app.
func TestBenchAppGateToleratesBadSeconds(t *testing.T) {
	l := newLab(t)
	l.set("app-bad-seconds", "1")
	l.add("e1", l.hooked, "a@x", "[[1]]", 0)
	l.until("run done", func() bool { return l.exp("e1").Count(0).Done == 1 })
	log := l.read("fuzzops.log")
	if n := strings.Count(log, "check-without-operator"); n != 1 {
		t.Fatalf("the bare-app gate took %d tries for an app with one bad second:\n%s", n, log)
	}
	l.until("lane idle", idle(l))
}

// An app with more bad seconds than the tolerance still fails the gate and is never given an operator.
func TestBenchAppGateStillFailsABrokenApp(t *testing.T) {
	l := newLab(t)
	l.set("app-bad-seconds", fmt.Sprint(DefaultBuildBadSeconds+1))
	l.add("e1", l.hooked, "a@x", "[[1]]", 0)
	l.until("a build failed", func() bool { return strings.Contains(l.lane().Reason, "app not clean") || l.lane().State == LaneDown })
	if strings.Contains(l.read("hooks.log"), "prepare") {
		t.Fatalf("an operator was restored on a broken app:\n%s", l.read("hooks.log"))
	}
}

// A freshly prepared operator that takes a while to start does not fail the
// build: the check is tried again until one ready_for span is clean.
func TestBenchBuildCheckWaitsForTheOperatorToStart(t *testing.T) {
	l := newLab(t)
	l.ready = "30s" // the controller's four not-idle answers must fit in it on a loaded host
	must(t, os.WriteFile(filepath.Join(l.hooks, "starting"), []byte("4"), 0o644))
	l.add("e1", l.hooked, "a@x", "[[1]]", 0)
	l.until("run done", func() bool { return l.exp("e1").Count(0).Done == 1 })
	if strings.Count(l.read("fuzzops.log"), "app up") != 1 {
		t.Fatalf("rebuilt:\n%s", l.read("fuzzops.log"))
	}
	l.until("lane idle", idle(l))
}

// The journeys span of the build check starts once the operator is idle: a controller still starting does not
// burn a whole ready_for span of journeys per try.
func TestBenchBuildCheckStartsJourneysOnceTheOperatorIsIdle(t *testing.T) {
	l := newLab(t)
	l.ready = "30s" // the controller's four not-idle answers must fit in it on a loaded host
	must(t, os.WriteFile(filepath.Join(l.hooks, "starting"), []byte("4"), 0o644))
	l.add("e1", l.hooked, "a@x", "[[1]]", 0)
	l.until("run done", func() bool { return l.exp("e1").Count(0).Done == 1 })
	if n := strings.Count(l.read("fuzzops.log"), "check-with-operator"); n != 1 {
		t.Fatalf("journeys checks with the operator = %d, want 1:\n%s", n, l.read("fuzzops.log"))
	}
}

// With gate_after_check, a run that starts right after the build check passed does not repeat the whole ready_for
// span in its gate; without it the gate is ready_for.
func TestBenchRunGateIsShortRightAfterTheBuildCheckWhenTheSpecAsks(t *testing.T) {
	l := newLab(t)
	l.addReusing("e1", "gate_after_check: 10ms\n")
	l.until("run done", func() bool { return l.exp("e1").Count(0).Done == 1 })
	log := l.read("fuzzops.log")
	if !strings.Contains(log, "ready-for 10ms") {
		t.Fatalf("run gate was not shortened:\n%s", log)
	}
}

// withClusters gives the lab's host cluster_up and cluster_down commands (logged) and a disk floor.
func (l *lab) withClusters(minFree string) {
	l.t.Helper()
	inv, err := os.ReadFile(l.root.path("hosts.yaml"))
	must(l.t, err)
	s := strings.Replace(string(inv), "clusters: [c1]}", "clusters: [c1], min_free: "+minFree+
		`, cluster_up: "echo up {cluster} {kubeconfig} {evidence} >> $FAKE_STATE/clusters.log", cluster_down: "echo down {cluster} >> $FAKE_STATE/clusters.log"}`, 1)
	must(l.t, os.WriteFile(l.root.path("hosts.yaml"), []byte(s), 0o644))
}

// A fault that leaves a cluster-scoped object breaks its sequence, and the next build makes the cluster anew.
func TestBenchClusterScopedResidueRecreatesTheCluster(t *testing.T) {
	l := newLab(t)
	l.withClusters("1K")
	l.set("residue", "")
	l.add("e1", l.hooked, "a@x", "[[1, 2]]", 1)
	l.until("first try ended", func() bool { return l.exp("e1").Progress(Seq{"a@x", 1}).Attempt == 2 })
	if r := l.exp("e1").Runs[RunID("a@x", 1, 2, 1)]; !strings.Contains(r.Reason, "cluster-scoped residue: validatingwebhookconfiguration") {
		t.Fatalf("run 2: %+v", r)
	}
	os.Remove(filepath.Join(l.state, "residue"))
	l.until("retry built", func() bool { return strings.Contains(l.read("clusters.log"), "down c1\nup c1") })
	if !strings.Contains(l.read("clusters.log"), "down c1\nup c1") || !strings.Contains(l.read("fuzzops.log"), "app images") {
		t.Fatalf("clusters:\n%s\nfuzzops:\n%s", l.read("clusters.log"), l.read("fuzzops.log"))
	}
}

// Two not-ready gates on a reused cluster make it anew instead of taking the lane down.
func TestBenchTwoFailedGatesRecreateTheCluster(t *testing.T) {
	l := newLab(t)
	l.withClusters("1K")
	l.set("busy-at-gate", "alarm: incident open")
	l.add("e1", l.hooked, "a@x", "[[1]]", 0)
	l.until("lane down", func() bool { return l.lane().State == LaneDown })
	if n := strings.Count(l.read("clusters.log"), "up c1"); n != 2 || strings.Count(l.read("fuzzops.log"), "app up") != 4 {
		t.Fatalf("%d recreations\n%s", n, l.read("fuzzops.log"))
	}
}

// When its experiment ends, a lane's cluster is deleted, unless the spec keeps its lanes.
func TestBenchEndedExperimentDeletesItsClusters(t *testing.T) {
	for _, keep := range []bool{false, true} {
		l := newLab(t)
		l.withClusters("1K")
		name := "e1"
		l.add(name, l.hooked, "a@x", "[[1]]", 0)
		if keep {
			p := l.root.SpecPath(name)
			b, _ := os.ReadFile(p)
			must(t, os.WriteFile(p, append(b, []byte("keep_lanes: true\n")...), 0o644))
		}
		l.until("run done", func() bool { return l.exp(name).Count(0).Done == 1 })
		l.until("lane idle", func() bool {
			lf := l.lane()
			return lf.Step == nil && (lf.Gone || lf.Prepared == nil || lf.Prepared.Hooks == "")
		})
		if got := l.lane().Gone; got == keep {
			b, _ := os.ReadFile(l.root.path("bench.log"))
			t.Fatalf("keep_lanes=%v: cluster deleted=%v\n%s\n%s", keep, got, l.read("clusters.log"), b)
		}
		// A new experiment on a deleted lane creates the cluster first.
		if !keep {
			l.add("e2", "{builtin: noop}", "a@x", "[[1]]", 0)
			l.until("e2 done", func() bool { return l.exp("e2").Count(0).Done == 1 })
			if !strings.Contains(l.read("clusters.log"), "down c1\nup c1") || !strings.HasPrefix(l.read("clusters.log"), "up c1") {
				t.Fatalf("clusters:\n%s", l.read("clusters.log"))
			}
		}
	}
}

// gc deletes the clusters no experiment needs and the staging nothing uses; below the disk floor no build starts.
func TestBenchGCAndDiskFloor(t *testing.T) {
	l := newLab(t)
	l.withClusters("1K")
	l.add("e1", l.hooked, "a@x", "[[1]]", 0)
	l.until("run done", func() bool { return l.exp("e1").Count(0).Done == 1 })
	l.until("cluster deleted", func() bool { return l.lane().Gone })
	stale := filepath.Join(filepath.Dir(string(l.root)), "staging", "c1", "build-old")
	must(t, os.MkdirAll(stale, 0o755))
	must(t, GC(l.root))
	l.until("gc done", func() bool { return !fileExists(l.root.path("gc.request")) })
	if fileExists(stale) {
		t.Fatal("stale staging kept")
	}
	// A floor above any disk: nothing builds.
	inv, _ := os.ReadFile(l.root.path("hosts.yaml"))
	must(t, os.WriteFile(l.root.path("hosts.yaml"), []byte(strings.Replace(string(inv), "min_free: 1K", "min_free: 1000000T", 1)), 0o644))
	l.add("e2", "{builtin: noop}", "a@x", "[[1]]", 0)
	for i := 0; i < 5; i++ {
		must(t, l.m.Reconcile(context.Background()))
	}
	if l.lane().Step != nil || l.exp("e2").Count(0).Done != 0 {
		t.Fatalf("built below the floor: %+v", l.lane())
	}
	b, _ := os.ReadFile(l.root.path("bench.log"))
	if !strings.Contains(string(b), "below its floor") {
		t.Fatalf("bench.log:\n%s", b)
	}
}

// A lane is not ready until the operator's sink is proven on the host.
func TestBenchLaneNeedsItsEvidenceSink(t *testing.T) {
	l := newLab(t)
	must(t, os.WriteFile(filepath.Join(l.hooks, "fail-transcripts"), []byte("no sink: layout.json missing\n"), 0o644))
	l.add("e1", l.hooked, "a@x", "[[1]]", 0)
	l.until("lane down", func() bool { return l.lane().State == LaneDown })
	if !strings.Contains(l.lane().Reason, "evidence sink not on the host") || strings.Contains(l.read("fuzzops.log"), "inject") {
		t.Fatalf("lane %+v", l.lane())
	}
}

// Every teardown waits for idle, then checks the transcripts; a gap is recorded and the reset goes on.
func TestBenchTeardownChecksTranscripts(t *testing.T) {
	l := newLab(t)
	l.add("e1", l.hooked, "a@x", "[[1]]", 0)
	l.until("run done", func() bool { return l.exp("e1").Count(0).Done == 1 })
	// From now on the sink misses a transcript: the release records it and still frees the lane.
	must(t, os.WriteFile(filepath.Join(l.hooks, "fail-transcripts"), []byte("transcripts missing: 1 (abc)\n"), 0o644))
	l.until("lane released", idle(l))
	if !strings.Contains(l.lane().Transcripts, "transcripts missing: 1") {
		t.Fatalf("lane %+v", l.lane())
	}
	hooks := l.read("hooks.log")
	i, j := strings.LastIndex(hooks, "idle c1 e1"), strings.LastIndex(hooks, "teardown c1 e1")
	if i < 0 || j < i {
		t.Fatalf("teardown did not wait for idle:\n%s", hooks)
	}
	b, _ := os.ReadFile(l.root.path("transcripts.jsonl"))
	if !strings.Contains(string(b), "transcripts missing") {
		t.Fatalf("transcripts.jsonl: %s", b)
	}
}

// A lane in its run window says so: the step's phase reaches the lane file.
func TestBenchLaneShowsItsStepPhase(t *testing.T) {
	if testing.Short() {
		t.Skip("real detached steps take several seconds: nightly tier")
	}
	l := newLab(t)
	l.set("sleep", "3")
	l.add("e1", l.hooked, "a@x", "[[1]]", 0)
	l.until("the lane in its window", func() bool {
		lf := l.lane()
		return lf.Step != nil && lf.Step.Kind == StepRun && lf.Step.Phase != nil && lf.Step.Phase.Name == PhaseWindow
	})
	l.until("run done", func() bool { return l.exp("e1").Count(0).Done == 1 })
	l.until("lane released", idle(l))
}

// extraProbe wraps a host and reports, besides what was asked, steps of lanes this root does not own and
// of every lane it knows, as a host shared with another bench root (or an older host binary) does.
type extraProbe struct {
	HostOps
	m *Manager
}

func (e extraProbe) Probe(ctx context.Context, steps map[string]string, fz []string) (Probe, error) {
	p, err := e.HostOps.Probe(ctx, steps, fz)
	if p.Steps == nil {
		p.Steps = map[string]string{}
	}
	p.Steps["other-root-lane"] = StepFinished
	for lane := range e.m.lanes {
		if _, ok := steps[lane]; !ok {
			p.Steps[lane] = StepFinished // a step this manager already settled
		}
	}
	return p, err
}

func TestBenchReconcileIgnoresStepsItDidNotAsk(t *testing.T) {
	l := newLab(t)
	l.m.Ops = func(host string, h Host) HostOps {
		return extraProbe{HostOps: localHost{staging: h.Staging, bin: benchBin}, m: l.m}
	}
	l.add("e1", l.hooked, "a@x", "[[1]]", 0)
	l.until("run done", func() bool { return l.exp("e1").Count(0).Done == 1 })
	l.until("lane idle", idle(l))
}

// The build check tolerates a few bad seconds by default, and the spec sets how many.
func TestBenchBuildCheckToleratesBadSeconds(t *testing.T) {
	l := newLab(t)
	l.add("e1", l.hooked, "a@x", "[[1]]", 0)
	l.until("run done", func() bool { return l.exp("e1").Count(0).Done == 1 })
	if log := l.read("fuzzops.log"); !strings.Contains(log, fmt.Sprintf("check-max-bad %d\n", DefaultBuildBadSeconds)) {
		t.Fatalf("build check did not pass the default tolerance:\n%s", log)
	}
	l.until("lane idle", idle(l))
}

func TestSpecBuildBadSeconds(t *testing.T) {
	var s Spec
	if s.BuildBad() != 2 {
		t.Fatalf("default %d", s.BuildBad())
	}
	n := 5
	s.BuildBadSeconds = &n
	if s.BuildBad() != 5 {
		t.Fatalf("set %d", s.BuildBad())
	}
	if (Step{}).buildBad() != 2 {
		t.Fatal("step default")
	}
}

func countLines(log, what string) int { return strings.Count(log, what+" ") }

// Without reuse_app every sequence puts down a new app copy.
func TestBenchSequencesBuildFreshAppsByDefault(t *testing.T) {
	l := newLab(t)
	l.addReusing("e1", "")
	l.until("both sequences done", func() bool { return l.exp("e1").Count(0).Done == 2 })
	l.until("lane idle", idle(l))
	if n := countLines(l.read("fuzzops.log"), "app up"); n != 2 {
		t.Fatalf("%d app copies for two sequences:\n%s", n, l.read("fuzzops.log"))
	}
}

// reuse_app rewinds the lane in place: the second sequence restores the operator on the app copy the first
// one left, after recover and the bare-app gate, and never puts the app down.
func TestBenchReuseAppRewindsTheLaneInPlace(t *testing.T) {
	l := newLab(t)
	l.addReusing("e1", "reuse_app: true\n")
	l.until("both sequences done", func() bool { return l.exp("e1").Count(0).Done == 2 })
	l.until("lane idle", idle(l))
	log, hooks := l.read("fuzzops.log"), l.read("hooks.log")
	if n := countLines(log, "app up"); n != 1 {
		t.Fatalf("%d app copies, want 1:\n%s", n, log)
	}
	if n := strings.Count(hooks, "prepare c1 e1"); n != 2 {
		t.Fatalf("%d operator restores, want 2:\n%s", n, hooks)
	}
	if n := strings.Count(hooks, "teardown c1 e1"); n < 2 {
		t.Fatalf("the first operator must leave through teardown:\n%s", hooks)
	}
	if n := countLines(log, "check-without-operator"); n < 2 {
		t.Fatalf("the reused app must pass the bare gate:\n%s", log)
	}
	l.noFault()
}

// A lane an experiment keeps (keep_lanes) is a warm pool: the next experiment on the same app and namespace takes
// its app copy instead of putting a new one down.
func TestBenchReuseAppAcrossExperiments(t *testing.T) {
	l := newLab(t)
	l.addReusing("e1", "reuse_app: true\nkeep_lanes: true\n")
	l.until("e1 done", func() bool { return l.exp("e1").Count(0).Done == 2 })
	l.until("lane idle", idle(l))
	l.addReusing("e2", "reuse_app: true\n")
	l.until("e2 done", func() bool { return l.exp("e2").Count(0).Done == 2 })
	l.until("lane idle", idle(l))
	if n := countLines(l.read("fuzzops.log"), "app up"); n != 1 {
		b, _ := os.ReadFile(l.root.path("bench.log"))
		t.Fatalf("%d app copies for two experiments, want 1:\n%s\n%s", n, l.read("fuzzops.log"), b)
	}
}

// A held lane is a warm pool: an experiment that claims it gets its operator by the hooks' swap, with no app copy,
// no teardown and no prepare.
func TestBenchClaimTakesAHeldLaneBySwap(t *testing.T) {
	l := newLab(t)
	l.addReusing("pool", "hold: true\nkeep_lanes: true\n")
	l.until("pool lane held", func() bool { lf := l.lane(); return lf.State == LaneReady && lf.Experiment == "pool" && lf.Step == nil })
	l.addReusing("e2", "claim: pool\nreuse_app: true\n")
	l.until("e2 done", func() bool { return l.exp("e2").Count(0).Done == 2 })
	hooks, log := l.read("hooks.log"), l.read("fuzzops.log")
	if n := strings.Count(hooks, "swap c1 e2"); n < 1 {
		t.Fatalf("no swap for the claimant:\n%s", hooks)
	}
	if n := strings.Count(hooks, "prepare c1 e2"); n != 0 {
		t.Fatalf("claimant prepares %d times (its second sequence takes the reset lane by swap), want 0:\n%s", n, hooks)
	}
	if n := countLines(log, "app up"); n != 1 {
		t.Fatalf("%d app copies, want 1:\n%s", n, log)
	}
}

// A held lane that was claimed returns to its prepared state by the hooks' reset (golden copy taken once after its
// build check), not by teardown and a rebuild: the pool refills by swapping the pool's sources back in.
func TestBenchPoolLaneReturnsByResetNotRebuild(t *testing.T) {
	l := newLab(t)
	l.addReusing("pool", "hold: true\nkeep_lanes: true\n")
	l.until("pool lane held with a golden copy", func() bool {
		lf := l.lane()
		return lf.State == LaneReady && lf.Experiment == "pool" && lf.Step == nil && lf.Prepared != nil && lf.Prepared.Golden
	})
	l.addReusing("e2", "claim: pool\nreuse_app: true\n")
	l.until("e2 done", func() bool { return l.exp("e2").Count(0).Done == 2 })
	l.until("pool lane held again", func() bool {
		lf := l.lane()
		return lf.State == LaneReady && lf.Experiment == "pool" && lf.Step == nil && lf.Prepared != nil && lf.Prepared.Golden && !lf.Prepared.Reset
	})
	hooks := l.read("hooks.log")
	if n := strings.Count(hooks, "snapshot c1 pool"); n != 1 {
		t.Fatalf("%d snapshots, want 1:\n%s", n, hooks)
	}
	if n := strings.Count(hooks, "reset c1 "); n < 2 {
		t.Fatalf("%d resets, want one after each claimed run:\n%s", n, hooks)
	}
	if n := strings.Count(hooks, "prepare c1 "); n != 1 {
		t.Fatalf("%d prepares, want only the pool's first:\n%s", n, hooks)
	}
	if n := strings.Count(hooks, "teardown c1 "); n != 1 {
		t.Fatalf("%d teardowns, want only the pool's first build's (a reset lane is never torn down):\n%s", n, hooks)
	}
}

// A lane whose reset fails is torn down and rebuilt as before.
func TestBenchFailedResetFallsBackToTeardownAndRebuild(t *testing.T) {
	l := newLab(t)
	l.set("fail-reset", "error: no golden copy")
	l.addReusing("pool", "hold: true\nkeep_lanes: true\n")
	l.until("pool lane held", func() bool { lf := l.lane(); return lf.State == LaneReady && lf.Experiment == "pool" && lf.Step == nil })
	l.addReusing("e2", "claim: pool\nreuse_app: true\n")
	l.until("e2 done", func() bool { return l.exp("e2").Count(0).Done == 2 })
	l.until("pool lane held again", func() bool {
		lf := l.lane()
		return lf.State == LaneReady && lf.Experiment == "pool" && lf.Step == nil
	})
	if hooks := l.read("hooks.log"); !strings.Contains(hooks, "teardown c1 ") {
		t.Fatalf("a failed reset must fall back to teardown:\n%s", hooks)
	}
}

// A claimant waits for a lane of its pool: with no held lane it builds nothing (a run on a fresh build costs a whole
// app copy and prepare), and status says what it waits for. It runs by swap once the pool has a lane.
func TestBenchClaimWaitsForItsPool(t *testing.T) {
	l := newLab(t)
	l.addReusing("e2", "claim: pool\nreuse_app: true\n")
	for i := 0; i < 5; i++ {
		must(t, l.m.Reconcile(context.Background()))
	}
	if lf := l.lane(); lf.Step != nil || lf.Experiment != "" {
		t.Fatalf("a claimant took a free lane with no pool lane ready: %+v", lf)
	} else if !strings.Contains(lf.Idle, "it waits for a lane of its pool pool (claim_only) [e2]") {
		t.Fatalf("the free lane does not say the claimant waits for its pool: %q", lf.Idle)
	}
	var out strings.Builder
	must(t, Status(l.root, "", &out))
	if !strings.Contains(out.String(), "claim: waits for pool pool, which does not exist") || !strings.Contains(out.String(), "runs on fresh builds: 0") {
		t.Fatalf("status does not say what the claimant waits for:\n%s", out.String())
	}
	l.addReusing("pool", "hold: true\nkeep_lanes: true\n")
	l.until("e2 done", func() bool { return l.exp("e2").Count(0).Done == 2 })
	if n := strings.Count(l.read("hooks.log"), "prepare c1 e2"); n != 0 {
		t.Fatalf("claimant prepared %d times, want 0 (it only swaps):\n%s", n, l.read("hooks.log"))
	}
	st, err := StatusOf(l.root, "")
	must(t, err)
	if st.FreshBuilds != 0 {
		t.Fatalf("fresh builds %d, want 0", st.FreshBuilds)
	}
}

// A claimant that waits for its pool longer than claim_wait_minutes stops: the experiment is paused with a named
// error (bench.log, the pause mark, status) instead of waiting forever. Resume starts the wait again.
func TestBenchClaimWaitIsBounded(t *testing.T) {
	l := newLab(t)
	l.addReusing("e2", "claim: pool\nreuse_app: true\nclaim_wait_minutes: 5\n")
	for i := 0; i < 3; i++ {
		must(t, l.m.Reconcile(context.Background()))
	}
	if l.exp("e2").Paused {
		t.Fatal("paused before the bound")
	}
	l.m.claimSince["e2"] = time.Now().Add(-6 * time.Minute)
	must(t, l.m.Reconcile(context.Background()))
	x := l.exp("e2")
	if !x.Paused {
		t.Fatalf("a claimant past its bound is not paused:\n%s", l.readRoot("bench.log"))
	}
	if !strings.Contains(l.readRoot("bench.log"), "claim wait exceeded: e2 waited") || !strings.Contains(l.readPath(l.root.PausedPath("e2")), "claim wait exceeded") {
		t.Fatalf("no named error:\n%s", l.readRoot("bench.log"))
	}
	if lf := l.lane(); lf.Step != nil || lf.Experiment != "" {
		t.Fatalf("a fresh build started: %+v", lf)
	}
}

// claim_only: false lets a claimant without a held lane fall back to the ordinary build, which status counts as a
// run on a fresh build.
func TestBenchClaimOnlyFalseBuildsAndIsCounted(t *testing.T) {
	l := newLab(t)
	l.addReusing("e2", "claim: nobody\nclaim_only: false\n")
	l.until("e2 done", func() bool { return l.exp("e2").Count(0).Done == 2 })
	if strings.Contains(l.read("hooks.log"), "swap ") {
		t.Fatalf("swapped with no held lane:\n%s", l.read("hooks.log"))
	}
	st, err := StatusOf(l.root, "")
	must(t, err)
	if st.FreshBuilds != 2 || st.Experiments[0].FreshBuilds != 2 {
		t.Fatalf("fresh builds %d (experiment %d), want 2", st.FreshBuilds, st.Experiments[0].FreshBuilds)
	}
	var out strings.Builder
	must(t, Status(l.root, "", &out))
	if !strings.Contains(out.String(), "runs on fresh builds: 2") || !strings.Contains(out.String(), "fresh builds: 2") {
		t.Fatalf("status:\n%s", out.String())
	}
}

// A reused copy that is not clean is rebuilt, never given an operator.
func TestBenchReuseAppRebuildsACopyThatIsNotClean(t *testing.T) {
	l := newLab(t)
	l.set("dirties", "")
	l.addReusing("e1", "reuse_app: true\n")
	l.until("both sequences done", func() bool { return l.exp("e1").Count(0).Done == 2 })
	l.until("lane idle", idle(l))
	log := l.read("fuzzops.log")
	if n := countLines(log, "app up"); n != 2 {
		t.Fatalf("%d app copies, want 2 (the dirty copy rebuilt):\n%s", n, log)
	}
}

// A reused copy whose data stores differ from the fresh deploy's (a repair or an undo left them different, which the
// journeys cannot see) is put down anew: the next sequence never inherits the previous run's data.
func TestBenchReuseAppRebuildsACopyWhoseStoresDiffer(t *testing.T) {
	l := newLab(t)
	l.set("corrupts", "")
	l.addReusing("e1", "reuse_app: true\n")
	l.until("both sequences done", func() bool { return l.exp("e1").Count(0).Done == 2 })
	l.until("lane idle", idle(l))
	log := l.read("fuzzops.log")
	if n := countLines(log, "app up"); n != 2 {
		t.Fatalf("%d app copies, want 2 (the corrupted copy rebuilt):\n%s", n, log)
	}
}

// A fault in fresh_app_faults gets a new copy, and so does the sequence after it.
func TestBenchFreshAppFaultsAreNotReused(t *testing.T) {
	l := newLab(t)
	l.addReusing("e1", "reuse_app: true\nfresh_app_faults: [a@x]\n")
	l.until("both sequences done", func() bool { return l.exp("e1").Count(0).Done == 2 })
	l.until("lane idle", idle(l))
	if n := countLines(l.read("fuzzops.log"), "app up"); n != 2 {
		t.Fatalf("%d app copies, want 2:\n%s", n, l.read("fuzzops.log"))
	}
}

// A vcluster host makes its lanes with cluster_up, loads no images into nodes (there are none), and tells the hooks
// the provider, that evidence is pulled rather than mounted, and the entry host.
func TestBenchVclusterHostLane(t *testing.T) {
	l := newLab(t)
	inv, err := os.ReadFile(l.root.path("hosts.yaml"))
	must(t, err)
	s := strings.Replace(string(inv), "clusters: [c1]}", "clusters: [c1], provider: vcluster, entry_host: 10.9.8.7, host_kubeconfig: /etc/shared.yaml"+
		`, cluster_up: "echo up {cluster} {kubeconfig} >> $FAKE_STATE/clusters.log", cluster_down: "echo down {cluster} >> $FAKE_STATE/clusters.log"}`, 1)
	must(t, os.WriteFile(l.root.path("hosts.yaml"), []byte(s), 0o644))
	l.add("e1", l.hooked, "a@x", "[[1]]", 0)
	l.until("run done", func() bool { return l.exp("e1").Count(0).Done == 1 })
	l.until("lane released", idle(l))
	b, err := os.ReadFile(filepath.Join(l.hooks, "provider"))
	if want := "vcluster vcluster pull 10.9.8.7 /etc/shared.yaml"; err != nil || strings.TrimSpace(string(b)) != want {
		t.Fatalf("hook environment = %q (%v), want %q", b, err, want)
	}
	if !strings.HasPrefix(l.read("clusters.log"), "up c1") {
		t.Fatalf("clusters:\n%s", l.read("clusters.log"))
	}
	if strings.Contains(l.read("fuzzops.log"), "app images") {
		t.Fatalf("a vcluster lane has no nodes to load images into:\n%s", l.read("fuzzops.log"))
	}
	l.noFault()
}

// A pool lane keeps the ledger of its build check: a claim swaps without a journeys span and its run starts with the
// ledger and the short gate; the lane's reset puts the ledger back for the next claim.
func TestBenchPoolLedgerReplacesTheSwapCheckAndTheLongGate(t *testing.T) {
	l := newLab(t)
	l.addReusing("pool", "hold: true\nkeep_lanes: true\n")
	l.until("pool lane held with a golden copy", func() bool {
		lf := l.lane()
		return lf.State == LaneReady && lf.Experiment == "pool" && lf.Step == nil && lf.Prepared != nil && lf.Prepared.Golden
	})
	l.addReusing("e2", "claim: pool\nreuse_app: true\n")
	l.until("e2 done", func() bool { return l.exp("e2").Count(0).Done == 2 })
	l.until("pool lane held again", func() bool {
		lf := l.lane()
		return lf.State == LaneReady && lf.Experiment == "pool" && lf.Step == nil && lf.Prepared != nil && !lf.Prepared.Reset
	})
	log := l.read("fuzzops.log")
	if n := countLines(log, "check-with-operator"); n != 1 {
		t.Fatalf("%d journeys checks with the operator, want only the pool's build check:\n%s", n, log)
	}
	if n := countLines(log, "ledger-in"); n != 2 {
		t.Fatalf("%d runs started with the ledger, want both claimed runs (the reset restores it):\n%s", n, log)
	}
}

// Fewer confirmed writes than ledgerMin: the swap runs the full check and the run keeps the full gate.
func TestBenchSmallLedgerKeepsTheFullCheckAndGate(t *testing.T) {
	l := newLab(t)
	l.set("ledger-count", "10")
	l.addReusing("pool", "hold: true\nkeep_lanes: true\n")
	l.until("pool lane held", func() bool { lf := l.lane(); return lf.State == LaneReady && lf.Experiment == "pool" && lf.Step == nil })
	l.addReusing("e2", "claim: pool\nreuse_app: true\n")
	l.until("e2 done", func() bool { return l.exp("e2").Count(0).Done == 2 })
	log := l.read("fuzzops.log")
	if strings.Contains(log, "ledger-in") {
		t.Fatalf("a ledger of 10 writes was used:\n%s", log)
	}
	if n := countLines(log, "check-with-operator"); n < 2 {
		t.Fatalf("%d checks, want the swap to run the full check:\n%s", n, log)
	}
}

// A build whose pack does not load is refused before a cluster is touched: the lane is not recreated, and
// DefaultMaxBuildFailures refusals in a row put it down with the pack's reason (bounded, not a loop to the step's cap).
func TestBenchBuildWithAPackThatDoesNotLoadStopsWithItsReason(t *testing.T) {
	l := newLab(t)
	l.set("bad-pack", "1")
	l.addReusing("e1", "")
	l.until("the lane is down", func() bool { return l.lane().State == LaneDown })
	lf := l.lane()
	if !strings.Contains(lf.Reason, "no app.yaml or app.json") || !strings.Contains(lf.Reason, fmt.Sprintf("%d builds failed", DefaultMaxBuildFailures)) {
		t.Errorf("lane reason %q does not carry the pack's error", lf.Reason)
	}
	if lf.Recreate {
		t.Error("a refused build marked the lane for recreation")
	}
	if n := countLines(l.read("fuzzops.log"), "pack "); n != DefaultMaxBuildFailures {
		t.Errorf("pack load ran %d times, want %d", n, DefaultMaxBuildFailures)
	}
	for _, v := range []string{"app up", "recover", "check"} {
		if n := countLines(l.read("fuzzops.log"), v+" "); n != 0 {
			t.Errorf("%q ran %d times for a build refused before any work", v, n)
		}
	}
}

// A held lane with an open incident is not claimed: nothing is swapped, the lane is recycled (counted, logged, not a
// build failure) and the claim is served by the recycled lane once it is clean.
func TestBenchHeldLaneWithAnOpenIncidentIsRecycledNotClaimed(t *testing.T) {
	l := newLab(t)
	l.addReusing("pool", "hold: true\nkeep_lanes: true\n")
	l.until("pool lane held with a golden copy", func() bool {
		lf := l.lane()
		return lf.State == LaneReady && lf.Experiment == "pool" && lf.Step == nil && lf.Prepared != nil && lf.Prepared.Golden
	})
	must(t, os.WriteFile(filepath.Join(l.hooks, "busy"), []byte("alarm: incident open i-1"), 0o644))
	l.addReusing("e2", "claim: pool\nreuse_app: true\n")
	l.until("the lane is recycled", func() bool { return strings.Contains(l.readRoot("recycled.jsonl"), "incident open") })
	if n := strings.Count(l.read("hooks.log"), "swap c1 e2"); n != 0 {
		t.Fatalf("swapped a lane with an open incident %d times:\n%s", n, l.read("hooks.log"))
	}
	if lf := l.lane(); lf.BuildFailures != 0 {
		t.Fatalf("a recycle counted as %d build failures", lf.BuildFailures)
	}
	must(t, os.Remove(filepath.Join(l.hooks, "busy")))
	l.until("e2 done", func() bool { return l.exp("e2").Count(0).Done == 2 })
	st, err := StatusOf(l.root, "pool")
	if err != nil || st.Experiments[0].Recycled["alarm: incident open i-1"] < 1 {
		t.Fatalf("status does not count the recycle: %+v %v", st.Experiments, err)
	}
}

// A reset lane that is not ready cannot be taken by swap (the swap would fail "holds no prepared operator" until the
// lane is marked down); only a ready one can.
func TestBenchResetLaneNeedsAReadyOperatorToBeSwappedInto(t *testing.T) {
	for _, tc := range []struct {
		name string
		l    *LaneFile
		want bool
	}{
		{"no operator", &LaneFile{}, false},
		{"reset, not ready", &LaneFile{Prepared: &Prepared{Reset: true}}, false},
		{"ready, not reset", &LaneFile{Prepared: &Prepared{Ready: true}}, false},
		{"reset and ready", &LaneFile{Prepared: &Prepared{Reset: true, Ready: true}}, true},
	} {
		if got := resetAndReady(tc.l); got != tc.want {
			t.Errorf("%s: resetAndReady = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// A run whose collect hook says the evidence is incomplete holds its lane (no release, no teardown, no next run)
// until the manifest lists no problems.
func TestBenchIncompleteEvidenceHoldsTheLane(t *testing.T) {
	if testing.Short() {
		t.Skip("real detached steps take several seconds: nightly tier")
	}
	l := newLab(t)
	must(t, os.WriteFile(filepath.Join(l.hooks, "incomplete-evidence"), nil, 0o644))
	l.add("e1", l.hooked, "a@x", "[[1]]", 0)
	l.until("lane held", func() bool { return l.lane().State == LaneHeld })
	if lf := l.lane(); !strings.Contains(lf.Reason, "evidence incomplete: 1 problem(s): controller.log missing") {
		t.Fatalf("lane %+v", lf)
	}
	// It stays held: reconcile for a while, no teardown runs.
	for i := 0; i < 20; i++ {
		must(t, l.m.Reconcile(context.Background()))
		time.Sleep(30 * time.Millisecond)
	}
	hooks := l.read("hooks.log")
	if lf := l.lane(); lf.State != LaneHeld || strings.Contains(hooks[strings.LastIndex(hooks, "collect c1 e1"):], "teardown") {
		t.Fatalf("lane %+v\n%s", lf, hooks)
	}
	// The evidence is completed (the validator rewrote the manifest): the lane is released as usual.
	run := l.exp("e1").Runs[RunID("a@x", 1, 1, 1)]
	must(t, os.WriteFile(filepath.Join(l.root.RunDir("e1", run.ID), "operator", EvidenceManifest), []byte(`{"problems": []}`), 0o644))
	l.until("lane released", idle(l))
	// The operator reset collect skipped ran before the lane was freed.
	if b, _ := os.ReadFile(filepath.Join(l.hooks, "incident-resets")); !strings.Contains(string(b), "reset") {
		t.Fatalf("no incident reset after the hold cleared:\n%s", l.read("hooks.log"))
	}
}

// A sink in place with a gap in an earlier run's rollouts still proves the sink: the lane becomes ready.
func TestBenchSinkProofToleratesEarlierTranscriptGap(t *testing.T) {
	l := newLab(t)
	must(t, os.WriteFile(filepath.Join(l.hooks, "fail-transcripts"), []byte("transcripts missing: 1 (abc)\n"), 0o644))
	l.add("e1", l.hooked, "a@x", "[[1]]", 0)
	l.until("run done", func() bool { return l.exp("e1").Count(0).Done == 1 })
}

// A lane file that says an app is installed, on a cluster that no longer exists (a cluster-up that died and removed
// its half-built cluster), must not make the next build wait for an operator that cannot come: the build makes the
// cluster and the lane carries on.
func TestBenchStalePreparedOnAMissingClusterIsRebuilt(t *testing.T) {
	l := newLab(t)
	l.withClusters("1K")
	l.set("no-cluster", "")
	stale := LaneFile{Lane: "c1", Host: "h1", State: LaneFree, Since: time.Now().UTC(),
		Prepared: &Prepared{Experiment: "gone", Hooks: "test", HooksPath: filepath.Dir(l.hooks), Command: "false", Namespace: "app1", AppPack: "x", Ready: true, WorkloadRev: "0b528ac"}}
	must(t, writeJSON(l.root.LanePath("c1"), stale))
	l.m = &Manager{Root: l.root, Poll: 50 * time.Millisecond, Bin: benchBin}
	l.add("e1", l.hooked, "a@x", "[[1]]", 0)
	l.until("cluster made and the run ended", func() bool {
		return l.exp("e1").Progress(Seq{"a@x", 1}).Ended()
	})
	if !strings.Contains(l.read("clusters.log"), "up c1") || l.lane().State == LaneDown {
		t.Fatalf("clusters:\n%s\nlane: %+v", l.read("clusters.log"), l.lane())
	}
}

// A snapshot whose only failure is the operator not being idle after a whole copy keeps its golden copy (the hook says
// so on its last stdout line): the manager waits for the operator to be idle and then holds the lane with its golden
// copy proven, instead of releasing a lane that cannot be claimed.
func TestBenchSnapshotThatKeptItsCopyIsProvenOnceTheOperatorIsIdle(t *testing.T) {
	l := newLab(t)
	l.setHook("snapshot-kept", "")
	l.addReusing("pool", "hold: true\nkeep_lanes: true\n")
	l.until("pool lane held with its golden copy", func() bool {
		lf := l.lane()
		return lf.State == LaneReady && lf.Experiment == "pool" && lf.Step == nil && lf.Prepared != nil && lf.Prepared.Golden
	})
	if n := strings.Count(l.read("hooks.log"), "snapshot c1 pool"); n != 1 {
		t.Fatalf("%d snapshots, want 1 (the kept copy is not taken again):\n%s", n, l.read("hooks.log"))
	}
}

// ... and a lane whose operator never settles after the copy, or whose snapshot left no copy, is never ready: a held
// lane without a proven golden copy cannot be claimed (the swap's golden gate would fail it), so its build fails and
// the lane is rebuilt.
func TestBenchHeldLaneWithoutAProvenGoldenCopyIsNeverReady(t *testing.T) {
	for name, set := range map[string]struct{ file, content string }{
		"copy kept and operator never idle": {"snapshot-kept-busy", ""},
		"no copy":                           {"fail-snapshot", "error: app snapshot failed: no space"},
	} {
		t.Run(name, func(t *testing.T) {
			l := newLab(t)
			t.Cleanup(func() {
				if t.Failed() {
					t.Log("hooks.log:\n" + l.read("hooks.log"))
					m, _ := filepath.Glob(l.root.path("lanes", "*.steps", "*", "runner.log"))
					for _, f := range m {
						b, _ := os.ReadFile(f)
						t.Log(f + ":\n" + string(b))
					}
				}
			})
			l.setHook(set.file, set.content)
			l.addReusing("pool", "hold: true\nkeep_lanes: true\n")
			l.until("the lane is taken down after its builds failed", func() bool { return l.lane().State == LaneDown })
			// the first build failed for want of a proven golden copy (later builds fail for what the hook left behind)
			if b := l.readRoot("bench.log"); !strings.Contains(b, "ok=false no ") || !strings.Contains(b, "golden copy of the pool lane") {
				t.Fatalf("no build failed for want of a proven golden copy:\n%s", b)
			}
		})
	}
}

// A done run whose lane had processes OOM-killed during the run is invalid by default (its sequence breaks and is
// retried on a new cluster); a spec that accepts OOM kills keeps it done, flagged in run.json.
func TestOOMKilledRunIsInvalidUnlessTheSpecAcceptsIt(t *testing.T) {
	done := Result{Status: StatusDone, Isolation: &Isolation{Memory: "20Gi", OOMKills: 6}}
	res, oom := judgeOOM(Spec{}, done)
	if !oom || res.Status != StatusInvalid || !strings.Contains(res.Reason, "6 process(es)") || !strings.Contains(res.Reason, "20Gi") {
		t.Fatalf("default: oom=%v %+v", oom, res)
	}
	if res, oom := judgeOOM(Spec{AcceptOOMKills: true}, done); oom || res.Status != StatusDone {
		t.Fatalf("accept_oom_kills: oom=%v %+v", oom, res)
	}
	for _, r := range []Result{
		{Status: StatusDone, Isolation: &Isolation{Memory: "20Gi"}}, // no kill
		{Status: StatusDone}, // a host without lane limits
		{Status: StatusInvalid, Reason: "capped", Isolation: done.Isolation},     // already invalid: its own reason stays
		{Status: StatusNotReady, Reason: "not ready", Isolation: done.Isolation}, // nothing was injected
	} {
		if got, oom := judgeOOM(Spec{}, r); oom || got.Status != r.Status || got.Reason != r.Reason {
			t.Errorf("%+v -> oom=%v %+v", r, oom, got)
		}
	}

	// Through the manager: run 1 of [[1, 2]] is invalid with its reason, run 2 "sequence broken", and the attempt
	// is retried (retries: 1).
	l := newLab(t)
	l.add("e1", l.hooked, "a@x", "[[1, 2]]", 1)
	l.m.load()
	x := l.m.exps["e1"]
	q := Seq{"a@x", 1}
	lf := &LaneFile{Lane: "c1", Host: "h1", Experiment: "e1", Seq: &q, Attempt: 1}
	rs := &RunSpec{ID: RunID("a@x", 1, 1, 1), Fault: "a@x", Seed: 1, Position: 1}
	res, _ = judgeOOM(x.Measured(), done)
	l.m.endRun(x, lf, rs, res)
	x = l.exp("e1")
	r1, r2 := x.Runs[RunID("a@x", 1, 1, 1)], x.Runs[RunID("a@x", 1, 2, 1)]
	if r1.Status != StatusInvalid || r1.Isolation == nil || r1.Isolation.OOMKills != 6 || !strings.HasPrefix(r2.Reason, "sequence broken: oom:") {
		t.Fatalf("r1 %+v\nr2 %+v", r1, r2)
	}
	if p := x.Progress(q); p.Ended() || p.Attempt != 2 {
		t.Fatalf("the sequence is retried: %+v", p)
	}
}

// A lane goes down after DefaultMaxBuildFailures (2) failed builds in a row, or after the host's max_build_failures.
func TestBenchBuildFailuresBeforeALaneGoesDownAreTheHosts(t *testing.T) {
	for _, n := range []int{0, 1, 3} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			l := newLab(t)
			want := DefaultMaxBuildFailures
			if n > 0 {
				want = n
				inv, err := os.ReadFile(l.root.path("hosts.yaml"))
				must(t, err)
				must(t, os.WriteFile(l.root.path("hosts.yaml"), []byte(strings.Replace(string(inv), "h1: {local: true,", fmt.Sprintf("h1: {max_build_failures: %d, local: true,", n), 1)), 0o644))
			}
			l.set("bad-pack", "1")
			l.addReusing("e1", "")
			l.until("the lane is down", func() bool { return l.lane().State == LaneDown })
			if got := countLines(l.read("fuzzops.log"), "pack "); got != want {
				t.Fatalf("%d builds before the lane went down, want %d (lane %+v)", got, want, l.lane())
			}
			if r := l.lane().Reason; !strings.Contains(r, fmt.Sprintf("%d builds failed in a row", want)) {
				t.Fatalf("lane reason %q", r)
			}
		})
	}
}

// remove honors keep_lanes: the lane returns free with its app copy and its cluster stays for a later reuse_app build
// or claim; --teardown deletes it as for an experiment that does not keep its lanes.
func TestBenchRemoveKeepsTheLanesOfAKeepLanesExperiment(t *testing.T) {
	if testing.Short() {
		t.Skip("real detached steps take several seconds: nightly tier")
	}
	for _, teardown := range []bool{false, true} {
		t.Run(fmt.Sprintf("teardown=%v", teardown), func(t *testing.T) {
			l := newLab(t)
			l.withClusters("1K")
			l.set("sleep", "2")
			l.addReusing("e1", "keep_lanes: true\n")
			l.until("running", func() bool { return l.lane().State == LaneRunning })
			must(t, Remove(l.root, "e1", teardown))
			if x := l.exp("e1"); x.Spec != nil || x.Removed == nil || x.Removed.KeepLanes == teardown || x.Removed.Teardown != teardown {
				t.Fatalf("removal record %+v", x.Removed)
			}
			l.until("lane released", func() bool {
				lf := l.lane()
				return lf.Step == nil && lf.State == LaneFree && (lf.Gone || lf.Prepared == nil || lf.Prepared.Hooks == "")
			})
			lf := l.lane()
			if lf.Gone != teardown {
				t.Fatalf("cluster deleted=%v, want %v\n%s\n%s", lf.Gone, teardown, l.read("clusters.log"), l.readRoot("bench.log"))
			}
			if !teardown && (lf.Prepared == nil || !lf.Prepared.Ready || lf.Prepared.Hooks != "") {
				t.Fatalf("kept lane has no app copy to reuse: %+v", lf.Prepared)
			}
			if !teardown {
				// The kept copy is reused by the next experiment: no new app copy, no new cluster.
				ups := strings.Count(l.read("fuzzops.log"), "app up")
				l.addReusing("e2", "reuse_app: true\n")
				l.until("e2 done", func() bool { return l.exp("e2").Count(0).Done == 1 })
				if n := strings.Count(l.read("fuzzops.log"), "app up"); n != ups || strings.Contains(l.read("clusters.log"), "down c1") {
					t.Fatalf("the kept lane was rebuilt (app up %d -> %d)\n%s", ups, n, l.read("clusters.log"))
				}
			}
		})
	}
}

// A free lane that is not bound while sequences are queued says why (status and the lane file), from the manager's
// own binding checks, and the log says it once the lane has been idle that way for idleLogEvery.
func TestBenchFreeLaneSaysWhyItIsNotBound(t *testing.T) {
	l := newLab(t)
	l.withClusters("1000000T") // a disk floor above any disk: no lane is built anew
	l.add("e2", "{builtin: noop}", "a@x", "[[1]]", 0)
	l.until("the lane explains itself", func() bool { return l.lane().Idle != "" })
	lf := l.lane()
	if lf.Step != nil || !strings.Contains(lf.Idle, "host h1 has") || !strings.Contains(lf.Idle, "below its floor 1000000T [e2]") {
		t.Fatalf("idle %q (lane %+v)", lf.Idle, lf)
	}
	var out strings.Builder
	must(t, Status(l.root, "", &out))
	if !strings.Contains(out.String(), "not bound: host h1 has") {
		t.Fatalf("status:\n%s", out.String())
	}
	if strings.Contains(l.readRoot("bench.log"), "while sequences are queued") {
		t.Fatal("logged before the lane was idle for idleLogEvery")
	}

	// Idle past idleLogEvery: logged once, not at every pass.
	lf.Since = time.Now().Add(-idleLogEvery - time.Minute)
	must(t, writeJSON(l.root.LanePath("c1"), lf))
	for i := 0; i < 3; i++ {
		must(t, l.m.Reconcile(context.Background()))
	}
	if n := strings.Count(l.readRoot("bench.log"), "while sequences are queued: host h1 has"); n != 1 {
		t.Fatalf("logged %d times, want once:\n%s", n, l.readRoot("bench.log"))
	}

	// Other reasons: an experiment that is paused, another whose spec does not list the lane's host.
	must(t, Pause(l.root, "e2"))
	p := filepath.Join(l.specs, "e3.yaml")
	must(t, os.WriteFile(p, []byte(strings.Replace(fmt.Sprintf(labSpec, "e3", "{builtin: noop}", "a@x", "[[1]]", l.ready, l.cap, 0), "hosts: [h1]", "hosts: [h1, h9]", 1)), 0o644))
	must(t, Add(context.Background(), l.root, p, io.Discard))
	b, _ := os.ReadFile(l.root.SpecPath("e3"))
	must(t, os.WriteFile(l.root.SpecPath("e3"), []byte(strings.Replace(string(b), "hosts: [h1, h9]", "hosts: [h9]", 1)), 0o644))
	must(t, l.m.Reconcile(context.Background()))
	if idle := l.lane().Idle; !strings.Contains(idle, "paused [e2]") || !strings.Contains(idle, "host h1 is not in the spec's hosts [e3]") {
		t.Fatalf("idle %q", idle)
	}

	// Nothing queued: nothing to explain.
	must(t, Remove(l.root, "e2", false))
	must(t, Remove(l.root, "e3", false))
	must(t, l.m.Reconcile(context.Background()))
	if idle := l.lane().Idle; idle != "" {
		t.Fatalf("idle %q with nothing queued", idle)
	}
}

// A hosts.yaml edit that does not load does not stop the manager: it goes on with the last inventory that loaded
// (steps are adopted, runs end), says why once in bench.log, in inventory.error and in status, until the file loads again.
func TestBenchBadInventoryEditKeepsTheLastGoodOne(t *testing.T) {
	l := newLab(t)
	l.set("sleep", "1")
	l.add("e1", l.hooked, "a@x", "[[1]]", 0)
	l.until("running", func() bool { return l.lane().State == LaneRunning })
	good, err := os.ReadFile(l.root.path("hosts.yaml"))
	must(t, err)
	must(t, os.WriteFile(l.root.path("hosts.yaml"), []byte(strings.Replace(string(good), "max_builds: 1", "max_builds: 0", 1)), 0o644))
	l.until("the run ends on the last good inventory", func() bool { return l.exp("e1").Count(0).Done == 1 })
	if n := strings.Count(l.readRoot("bench.log"), "hosts.yaml refused"); n != 1 {
		t.Fatalf("refusal logged %d times, want once:\n%s", n, l.readRoot("bench.log"))
	}
	if b := l.readRoot(InventoryErrorFile); !strings.Contains(b, "max_builds") {
		t.Fatalf("inventory.error %q", b)
	}
	var out strings.Builder
	must(t, Status(l.root, "", &out))
	if !strings.Contains(out.String(), "hosts.yaml REFUSED") || !strings.Contains(out.String(), "max_builds") || !strings.Contains(out.String(), " h1 ") {
		t.Fatalf("status:\n%s", out.String())
	}
	must(t, os.WriteFile(l.root.path("hosts.yaml"), good, 0o644))
	must(t, l.m.Reconcile(context.Background()))
	if fileExists(l.root.path(InventoryErrorFile)) || !strings.Contains(l.readRoot("bench.log"), "hosts.yaml loads again") {
		t.Fatalf("not cleared:\n%s", l.readRoot("bench.log"))
	}
	l.until("lane released", idle(l))

	// A manager that never loaded an inventory still fails its pass.
	must(t, os.WriteFile(l.root.path("hosts.yaml"), []byte(strings.Replace(string(good), "max_builds: 1", "max_builds: 0", 1)), 0o644))
	if err := (&Manager{Root: l.root, Bin: benchBin}).Reconcile(context.Background()); err == nil {
		t.Fatal("a fresh manager reconciled with no inventory that loads")
	}
	must(t, os.WriteFile(l.root.path("hosts.yaml"), good, 0o644))
}

// A pool's builds go first to the hosts where it has the fewest lanes, so every host it may use gets one golden
// lane before any gets a second; other experiments keep their spec's host order.
func TestPoolBuildsOneLanePerHostFirst(t *testing.T) {
	spec := func(name string, hold bool, claim string) *Experiment {
		s := Spec{Name: name, App: "a", Fuzzops: "0b528ac", Hosts: []string{"h1", "h2", "h3"}, Hold: hold, Claim: claim, Lanes: 4}
		return &Experiment{Name: name, Spec: &s, Lock: Lock{Spec: s}}
	}
	inv := Inventory{Hosts: map[string]Host{}, Checkouts: Checkouts{Fuzzops: map[string]map[string]string{"0b528ac": {"h1": "/f", "h2": "/f", "h3": "/f"}}}}
	for _, h := range []string{"h1", "h2", "h3"} {
		inv.Hosts[h] = Host{MaxBuilds: 4, MaxLoad5: 100}
	}
	m := &Manager{inv: inv, exps: map[string]*Experiment{"pool": spec("pool", true, ""), "c": spec("c", false, "pool"), "other": spec("other", false, "")},
		lanes: map[string]*LaneFile{
			// h1: a pool lane building; h3: a pool lane a claimant holds (its golden copy is the pool's).
			"a": {Lane: "a", Host: "h1", Experiment: "pool", State: LaneBuilding},
			"b": {Lane: "b", Host: "h3", Experiment: "c", State: LaneRunning, Prepared: &Prepared{Experiment: "pool", Golden: true}},
		}}
	if got := m.buildOrder("pool", m.exps["pool"].Measured()); strings.Join(got, ",") != "h2,h1,h3" {
		t.Fatalf("pool build order %v, want h2 first (no pool lane), then h1, h3", got)
	}
	if got := m.buildOrder("other", m.exps["other"].Measured()); strings.Join(got, ",") != "h1,h2,h3" {
		t.Fatalf("a non-pool experiment's order %v, want the spec's", got)
	}
}

func TestSpecClaimOnly(t *testing.T) {
	f := false
	for _, c := range []struct {
		s    Spec
		want bool
	}{{Spec{}, false}, {Spec{Claim: "p"}, true}, {Spec{Claim: "p", ClaimOnly: &f}, false}} {
		if got := c.s.ClaimWaits(); got != c.want {
			t.Errorf("%+v: ClaimWaits %v, want %v", c.s, got, c.want)
		}
	}
	tr := true
	s := Spec{ClaimOnly: &tr}
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "claim_only needs claim") {
		t.Fatalf("claim_only without claim: %v", err)
	}
}
