package bench

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/self-defining-systems/sdo/benchmarks/lanes/internal/plan"
)

// PlansDir is <root>/plans: one directory per plan (internal/plan).
func (r Root) PlansDir() string { return r.path("plans") }

// SystemClock is the wall clock the runner gives the plan engine.
type SystemClock struct{}

// Now is the current time.
func (SystemClock) Now() time.Time { return time.Now() }

// PlanBackend runs a plan's nodes as experiments of this root: a node is one experiment (spec) added to the manager,
// and the plan reads the experiment's run files back. It holds no state of its own.
type PlanBackend struct {
	Root Root
	Inv  Inventory
	Log  io.Writer
	// Grading, when set, counts the background grading jobs so a caller can wait for them.
	Grading *sync.WaitGroup
}

// PlanAdd adds a plan from a plan file: its templates are rendered once per arm and each result is validated as a spec
// and against the inventory (the pins must be checked out on a usable host).
func PlanAdd(root Root, file string, w io.Writer) error {
	release, err := lockAdds(root)
	if err != nil {
		return err
	}
	defer release()
	inv, err := LoadInventory(string(root))
	if err != nil {
		return err
	}
	lock, err := plan.Add(root.PlansDir(), file, SystemClock{}, planSpecCheck(inv), plan.WithResolver(GitResolve))
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "plan %s added: %d nodes, workload %s, subject %s\n", lock.Plan, lock.Nodes, lock.Pins.Workload, lock.Pins.Subject)
	return nil
}

// GitResolve is the plan engine's ref resolver: the full commit a ref names in the git checkout at dir.
func GitResolve(dir, ref string) (string, error) {
	out, err := exec.Command("git", "-C", dir, "rev-parse", "--verify", "--quiet", ref+"^{commit}").Output()
	if err != nil {
		return "", fmt.Errorf("git rev-parse %s in %s failed (is it a git checkout with that ref?)", ref, dir)
	}
	return strings.TrimSpace(string(out)), nil
}

// PlanDryRun runs every check of PlanAdd and prints the expanded plan (items, arms, edges, order), as JSON when
// jsonOut. It creates nothing under the root.
func PlanDryRun(root Root, file string, jsonOut bool, w io.Writer) error {
	inv, err := LoadInventory(string(root))
	if err != nil {
		return err
	}
	pv, err := plan.DryRun(root.PlansDir(), file, SystemClock{}, planSpecCheck(inv), plan.WithResolver(GitResolve))
	if err != nil {
		return err
	}
	if jsonOut {
		b, _ := json.MarshalIndent(pv, "", " ")
		_, err = fmt.Fprintln(w, string(b))
		return err
	}
	pv.Render(w)
	return nil
}

// PlanRemove is `lanes plan remove`: it marks the plan for cleanup and, when no manager runs, does the cleanup itself.
// Cleanup cancels the plan's experiments (queued runs are dropped; a run on a lane finishes), moves them and the plan's
// directory under <root>/plans/.removed/<name>-<time>/ (results and state stay there) and so frees the plan's name. A
// run still on a lane delays the move of its experiment until it ends; the manager carries on every pass.
func PlanRemove(root Root, name string, keep bool, w io.Writer) error {
	dir := plan.Dir(root.PlansDir(), name)
	if _, err := os.Stat(dir); err != nil {
		return fmt.Errorf("no plan %q under %s", name, root.PlansDir())
	}
	if keep {
		return plan.RemovePlan(dir, false)
	}
	if err := plan.PurgePlan(dir); err != nil {
		return err
	}
	lf, err := os.OpenFile(root.path("bench.lock"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer lf.Close()
	if syscall.Flock(int(lf.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) != nil {
		fmt.Fprintf(w, "plan %s marked for removal; the manager cleans it up on its next pass\n", name)
		return nil
	}
	done, err := purgePlan(root, name)
	if err != nil {
		return err
	}
	if !done {
		fmt.Fprintf(w, "plan %s: experiments still run on lanes; the next manager pass finishes the removal when they end\n", name)
		return nil
	}
	fmt.Fprintf(w, "plan %s removed; its name is free\n", name)
	return nil
}

// purgePlan advances the cleanup of a plan marked for removal; done when the plan directory has moved to the archive.
// It is idempotent: a restart repeats whatever the last pass did not finish.
func purgePlan(root Root, name string) (done bool, err error) {
	dir := plan.Dir(root.PlansDir(), name)
	b := &PlanBackend{Root: root}
	exps := plan.AllExperiments(dir)
	for _, exp := range exps {
		if err := b.Cancel(context.Background(), exp); err != nil {
			return false, err
		}
	}
	archive, err := plan.SetArchive(dir, filepath.Join(root.PlansDir(), ".removed", name+"-"+time.Now().UTC().Format("2006-01-02_150405")))
	if err != nil {
		return false, err
	}
	if err := os.MkdirAll(filepath.Join(archive, "experiments"), 0o755); err != nil {
		return false, err
	}
	pending := false
	for _, exp := range exps {
		src := root.path(exp)
		if _, err := os.Stat(src); err != nil {
			continue
		}
		if b.laneBound(exp) {
			pending = true
			continue
		}
		if err := os.Rename(src, filepath.Join(archive, "experiments", exp)); err != nil {
			return false, err
		}
	}
	if pending {
		return false, nil
	}
	// The plan directory moves last, into the archive directory itself: its files sit beside experiments/.
	ents, err := os.ReadDir(dir)
	if err != nil {
		return false, err
	}
	for _, e := range ents {
		if err := os.Rename(filepath.Join(dir, e.Name()), filepath.Join(archive, e.Name())); err != nil {
			return false, err
		}
	}
	return true, os.Remove(dir)
}

// PlanControl applies a control request (pause, resume, remove, skip) to the plan's directory. The manager applies it
// on its next pass; the request is on disk until then, so a restart loses nothing.
func PlanControl(root Root, name string, f func(dir string) error) error {
	dir := plan.Dir(root.PlansDir(), name)
	if _, err := os.Stat(dir); err != nil {
		return fmt.Errorf("no plan %q under %s", name, root.PlansDir())
	}
	return f(dir)
}

// checkSpecInputs runs the checks `lanes add` and a submit make on the files a spec names, so a plan never finds them
// broken at release: the instances file exists and holds every fault ID, the baseline directory exists. (operator.seed
// is an artifact of an earlier arm and cannot be checked before it exists.)
func checkSpecInputs(s Spec) error {
	if s.Faults.Instances != "" {
		if err := checkIDs(s.Faults.Instances, s.Faults.IDs); err != nil {
			return fmt.Errorf("faults.instances %s: %w", s.Faults.Instances, err)
		}
	}
	if b := s.Operator.Baseline; b != "" {
		if st, err := os.Stat(b); err != nil || !st.IsDir() {
			return fmt.Errorf("operator.baseline %s: not a directory", b)
		}
	}
	return nil
}

// planSpecCheck is the check of one rendered spec: the platform's own spec validation and the inventory (the pins
// must be checked out on a usable host).
func planSpecCheck(inv Inventory) func([]byte) error {
	pins := map[string]string{}   // the pin check's answers, one per host and pin
	mem := map[string]memResult{} // the memory check's answers, one per directory and pinned SDO checkout
	return func(b []byte) error {
		s, err := ParseSpec(b)
		if err != nil {
			return err
		}
		if len(inv.UsableHosts(s)) == 0 {
			return fmt.Errorf("no host of %v has the pinned fuzzops %s%s in hosts.yaml checkouts", s.Hosts, s.Fuzzops,
				map[bool]string{true: " and hooks " + s.Operator.Hooks + " " + s.Operator.Revision}[s.Operator.Hooks != ""])
		}
		if s.Operator.Hooks != "" && inv.HookCommand[s.Operator.Hooks] == "" {
			return fmt.Errorf("hosts.yaml has no hook_command for %s", s.Operator.Hooks)
		}
		if err := checkSpecInputs(s); err != nil {
			return err
		}
		if err := checkPins(context.Background(), inv, s, nil, pins); err != nil {
			return err
		}
		return checkSpecMemory(context.Background(), inv, s, mem)
	}
}

// planNotes is where checks that could not run say so (a note is no failure).
var planNotes io.Writer = os.Stderr

// specMemoryAllow: the add-time check has no cluster, so it cannot know which telemetry sources the install will bind;
// the controller binds them at install. An unbound source is therefore no failure here (preflight takes the same
// substring with --allow-memory-diag).
var specMemoryAllow = []string{"telemetry.unbound"}

// checkSpecMemory runs SDO's memory check over the spec's seed and baseline memory at its pinned SDO revision, on the
// manager's host when it has that hooks checkout. An invalid one is refused here, naming the memory and the failed
// check, instead of crash-looping the controller on a built lane. answers caches one check per directory and checkout.
func checkSpecMemory(ctx context.Context, inv Inventory, s Spec, answers map[string]memResult) error {
	if s.Operator.Hooks == "" {
		return nil
	}
	local := inv.LocalHost()
	hooks := ""
	if local != "" {
		hooks = inv.HooksPath(s.Operator.Hooks, s.Operator.Revision, local)
	}
	for _, m := range []struct{ what, dir string }{{"operator.baseline", s.Operator.Baseline}, {"operator.seed", s.Operator.Seed}} {
		if m.dir == "" {
			continue
		}
		key := m.dir + "|" + hooks
		r, ok := answers[key]
		if !ok {
			r = checkMemoryDir(ctx, memoryRunner(inv), local, envPrefix(inv.Hosts[local]), hooks, m.dir, specMemoryAllow)
			answers[key] = r
			if r.Note != "" && planNotes != nil {
				fmt.Fprintf(planNotes, "note: %s %s: %s\n", s.Name, m.what, r.Note)
			}
		}
		if r.Err != nil {
			return fmt.Errorf("%s %s: memory check at sdo %s failed: %w", m.what, m.dir, short(s.Operator.Revision), r.Err)
		}
	}
	return nil
}

// Submit adds the experiment unless it exists.
func (b *PlanBackend) Submit(ctx context.Context, j plan.Job) error {
	if _, err := os.Stat(b.Root.LockPath(j.Exp)); err == nil {
		return plan.ErrExists
	}
	return Add(ctx, b.Root, j.SpecPath, b.Log)
}

// Observe maps the experiment's sequences to a phase: failed when a sequence ended invalid or was dropped (the reason
// is the head of the invalid run's reason), done when every sequence is done.
func (b *PlanBackend) Observe(_ context.Context, exp string) (plan.Observation, error) {
	x, err := b.Root.LoadExperiment(exp)
	if errors.Is(err, os.ErrNotExist) {
		return plan.Observation{Phase: plan.PhaseAbsent}, nil
	}
	if err != nil {
		return plan.Observation{}, err
	}
	var obs plan.Observation
	for _, rf := range x.Runs {
		if obs.Started.IsZero() || (!rf.Started.IsZero() && rf.Started.Before(obs.Started)) {
			obs.Started = rf.Started
		}
		if rf.Ended.After(obs.Ended) {
			obs.Ended = rf.Ended
		}
	}
	obs.Stages = runStages(x)
	if x.Spec == nil {
		obs.Phase, obs.Reason, obs.Detail = plan.PhaseFailed, "removed", "the experiment was removed"
		return obs, nil
	}
	allDone := true
	for _, q := range x.AllSeqs() {
		p := x.Progress(q)
		switch p.State {
		case SeqDone:
		case SeqInvalid:
			obs.Phase, obs.Reason, obs.Detail = plan.PhaseFailed, planReason(p.Reason), p.Reason
			return obs, nil
		case SeqDropped:
			obs.Phase, obs.Reason, obs.Detail = plan.PhaseFailed, "dropped", "a sequence was dropped"
			return obs, nil
		default:
			allDone = false
		}
	}
	switch {
	case allDone && len(x.AllSeqs()) > 0:
		obs.Phase = plan.PhaseDone
		return obs, nil
	case len(x.Runs) > 0 || b.laneBound(exp):
		obs.Phase = plan.PhaseRunning
	default:
		obs.Phase = plan.PhaseQueued
	}
	obs.Stage, obs.StageSince = b.currentStage(x)
	return obs, nil
}

// Stages of a real run, in the order they happen: queue (added to first run started), pre-inject (run started to the
// fault's injection: build, gate), post-inject (injection to the end: window, undo, settle). They are measured from
// run.json of the newest run that has them.
const (
	StageQueue      = "queue"
	StagePreInject  = "pre-inject"
	StagePostInject = "post-inject"
)

func runStages(x *Experiment) map[string]time.Duration {
	out := map[string]time.Duration{}
	var first, newest *RunFile
	for _, rf := range sortedRuns(x) {
		rf := rf
		if !rf.Started.IsZero() && (first == nil || rf.Started.Before(first.Started)) {
			first = &rf
		}
		if rf.InjectAt != nil && !rf.Ended.IsZero() && (newest == nil || rf.Ended.After(newest.Ended)) {
			newest = &rf
		}
	}
	if first != nil && !x.Lock.Added.IsZero() && first.Started.After(x.Lock.Added) {
		out[StageQueue] = first.Started.Sub(x.Lock.Added)
	}
	if newest != nil {
		out[StagePreInject] = max(newest.InjectAt.Sub(newest.Started), 0)
		out[StagePostInject] = max(newest.Ended.Sub(*newest.InjectAt), 0)
	}
	return out
}

// currentStage is the stage the experiment is in now: the phase of the step on its lane (gate, window, prepare, ...),
// the queue before it has a lane, or the gap between its runs.
func (b *PlanBackend) currentStage(x *Experiment) (string, time.Time) {
	lanes, _ := b.Root.Lanes()
	for _, l := range lanes {
		if l.Experiment != x.Name {
			continue
		}
		if l.Step != nil {
			if l.Step.Phase != nil {
				return l.Step.Phase.Name, l.Step.Phase.Since
			}
			return l.Step.Kind, l.Step.Started
		}
		if l.Seq != nil {
			return "lane-bound", l.Since
		}
	}
	if len(x.Runs) == 0 {
		return StageQueue, x.Lock.Added
	}
	return "between-runs", time.Time{}
}

// Capacity reads the lanes, the hosts and the queue as `status` does. A lane counts as free when no experiment holds
// it and nothing runs on it.
func (b *PlanBackend) Capacity(_ context.Context) (plan.Capacity, error) {
	st, err := StatusOf(b.Root, "")
	if err != nil {
		return plan.Capacity{}, err
	}
	var c plan.Capacity
	for _, l := range st.Lanes {
		if l.State == LaneFree && l.Step == nil && l.Seq == nil {
			c.Lanes = append(c.Lanes, plan.LaneInfo{Host: l.Host, Lane: l.Lane, Free: true, Since: l.Since})
		}
	}
	for _, h := range st.Hosts {
		if h.Lanes == 0 {
			continue // no lanes listed (drained or kept for later): nothing of ours runs there, so its pressure is no alarm
		}
		minFree, _ := ParseSize(h.MinFree)
		c.Hosts = append(c.Hosts, plan.HostInfo{Name: h.Name, Load5: h.Load5, MaxLoad5: h.MaxLoad5, Free: h.Free, MinFree: minFree, Error: h.Error})
	}
	for _, e := range st.Experiments {
		if e.Error == "" && e.State == ExpActive {
			c.Waiting += e.Counts.Queued
		}
	}
	return c, nil
}

// planReason is the failure class of an invalid run: the head of its reason, as `status` counts it.
func planReason(r string) string {
	c := strings.ToLower(strings.ReplaceAll(reasonClass(r), " ", "-"))
	if c == "" {
		return "invalid"
	}
	return c
}

func (b *PlanBackend) laneBound(exp string) bool {
	lanes, _ := b.Root.Lanes()
	for _, l := range lanes {
		if l.Experiment == exp && (l.Seq != nil || l.Step != nil) {
			return true
		}
	}
	return false
}

// newestDone is the run directory of the experiment's newest done run.
func (b *PlanBackend) newestDone(exp string) (string, error) {
	x, err := b.Root.LoadExperiment(exp)
	if err != nil {
		return "", err
	}
	var best *RunFile
	for _, rf := range sortedRuns(x) {
		rf := rf
		if rf.Status == StatusDone && (best == nil || rf.Ended.After(best.Ended)) {
			best = &rf
		}
	}
	if best == nil {
		return "", fmt.Errorf("experiment %s has no done run", exp)
	}
	return b.Root.RunDir(exp, best.ID), nil
}

// Export resolves the path template: ${run} the newest done run of the node, ${exp_dir} its experiment directory,
// ${step_dir} a step's directory.
func (b *PlanBackend) Export(_ context.Context, src plan.Source, kind, tmpl string) (plan.Artifact, error) {
	path := tmpl
	if strings.Contains(path, "${run}") {
		run, err := b.newestDone(src.Exp)
		if err != nil {
			return plan.Artifact{}, err
		}
		path = strings.ReplaceAll(path, "${run}", run)
	}
	path = strings.ReplaceAll(strings.ReplaceAll(path, "${exp_dir}", b.Root.path(src.Exp)), "${step_dir}", src.Dir)
	if strings.Contains(path, "${") {
		return plan.Artifact{}, fmt.Errorf("artifact %s: unresolved variable in %q", kind, path)
	}
	return plan.Artifact{Kind: kind, Path: path}, nil
}

// Verify: exists (the path exists and a directory is not empty), cmd:<shell> (exit 0), or subject (the subject's
// verify-artifact verb; unsupported falls back to exists).
func (b *PlanBackend) Verify(ctx context.Context, src plan.Source, a plan.Artifact, mode string) (plan.Verdict, error) {
	exists := func() plan.Verdict {
		fi, err := os.Stat(a.Path)
		switch {
		case err != nil:
			return plan.Verdict{Detail: err.Error()}
		case fi.IsDir():
			if ents, _ := os.ReadDir(a.Path); len(ents) == 0 {
				return plan.Verdict{Detail: a.Path + " is an empty directory"}
			}
		case fi.Size() == 0:
			return plan.Verdict{Detail: a.Path + " is empty"}
		}
		return plan.Verdict{OK: true}
	}
	env := []string{"ARTIFACT_PATH=" + a.Path, "ARTIFACT_KIND=" + a.Kind, "BENCH_EXPERIMENT=" + src.Exp, "BENCH_ROOT=" + string(b.Root)}
	switch {
	case mode == plan.VerifyExists:
		return exists(), nil
	case strings.HasPrefix(mode, plan.VerifyCmd):
		return runVerifyCmd(ctx, strings.TrimPrefix(mode, plan.VerifyCmd), "", env), nil
	case mode == plan.VerifySubject:
		if src.Exp == "" {
			return exists(), nil
		}
		x, err := b.Root.LoadExperiment(src.Exp)
		if err != nil {
			return plan.Verdict{}, err
		}
		s := x.Lock.Spec
		host := b.Inv.LocalHost()
		dir, cmd := b.Inv.HooksPath(s.Operator.Hooks, s.Operator.Revision, host), b.Inv.HookCommand[s.Operator.Hooks]
		if s.Operator.Hooks == "" || dir == "" || cmd == "" {
			return exists(), nil
		}
		if run, err := b.newestDone(src.Exp); err == nil {
			env = append(env, "BENCH_RUN_DIR="+run)
		}
		res := Hook{Dir: dir, Command: cmd}.Run(ctx, "verify-artifact", env, nil)
		switch {
		case res.Unsupported():
			return exists(), nil
		case res.Exit == -1:
			return plan.Verdict{}, errors.New(res.Why("verify-artifact"))
		case !res.OK():
			return plan.Verdict{Detail: res.Why("verify-artifact")}, nil
		}
		return plan.Verdict{OK: true}, nil
	}
	return plan.Verdict{}, fmt.Errorf("verify mode %q", mode)
}

func runVerifyCmd(ctx context.Context, command, dir string, env []string) plan.Verdict {
	ctx, cancel := context.WithTimeout(ctx, hookTimeouts["verify-artifact"])
	defer cancel()
	cmd := exec.CommandContext(ctx, "sh", "-c", command)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), env...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	out, err := cmd.CombinedOutput()
	if err == nil {
		return plan.Verdict{OK: true}
	}
	return plan.Verdict{Detail: fmt.Sprintf("%v: %s", err, tailLine(string(out)))}
}

func tailLine(s string) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	return strings.TrimSpace(lines[len(lines)-1])
}

// StartStep starts the step detached (its own session): it runs `timeout` around the command and writes its exit
// status to <dir>/exit, so a restart of the runner adopts a step that is still running or has finished.
func (b *PlanBackend) StartStep(_ context.Context, j plan.StepJob) error {
	if err := os.MkdirAll(j.Dir, 0o755); err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(j.Dir, "started.json")); err == nil {
		return nil // already started (a restart between the start and the state write)
	}
	in, _ := json.Marshal(j.Inputs)
	inFile := filepath.Join(j.Dir, "inputs.json")
	if err := os.WriteFile(inFile, in, 0o644); err != nil {
		return err
	}
	script := fmt.Sprintf("timeout %d sh -c %s > out.log 2>&1; echo $? > exit.tmp; mv exit.tmp exit", int(j.Timeout.Seconds()), shellQuote(j.Run))
	cmd := exec.Command("setsid", "sh", "-c", script)
	cmd.Dir = j.Dir
	cmd.Env = append(os.Environ(), "STEP_DIR="+j.Dir, "PLAN_DIR="+filepath.Dir(filepath.Dir(j.Dir)), "LANES_INPUTS="+string(in), "LANES_INPUTS_FILE="+inFile,
		"LANES_ROOT="+string(b.Root), "LANES_PLAN="+j.Plan)
	if err := cmd.Start(); err != nil {
		return err
	}
	go func() { _ = cmd.Wait() }()
	started, _ := json.Marshal(map[string]any{"at": time.Now().UTC(), "timeout_s": int(j.Timeout.Seconds()), "pid": cmd.Process.Pid})
	return os.WriteFile(filepath.Join(j.Dir, "started.json"), started, 0o644)
}

// ObserveStep reads the step's records. A step with no exit file past its timeout (and a grace minute) is lost: the
// host went away under it. No process table is read.
func (b *PlanBackend) ObserveStep(_ context.Context, dir string) (plan.StepObservation, error) {
	if exit, err := os.ReadFile(filepath.Join(dir, "exit")); err == nil {
		out, _ := os.ReadFile(filepath.Join(dir, "out.log"))
		if strings.TrimSpace(string(exit)) == "0" {
			return plan.StepObservation{Phase: plan.PhaseDone}, nil
		}
		return plan.StepObservation{Phase: plan.PhaseFailed, Detail: fmt.Sprintf("exit %s: %s", strings.TrimSpace(string(exit)), tailLine(string(out)))}, nil
	}
	var st struct {
		At       time.Time `json:"at"`
		TimeoutS int       `json:"timeout_s"`
	}
	if err := readJSON(filepath.Join(dir, "started.json"), &st); err != nil {
		return plan.StepObservation{Phase: plan.PhaseAbsent}, nil
	}
	if time.Since(st.At) > time.Duration(st.TimeoutS)*time.Second+time.Minute {
		return plan.StepObservation{Phase: plan.PhaseFailed, Detail: "no exit record past the step's timeout (lost)"}, nil
	}
	return plan.StepObservation{Phase: plan.PhaseRunning}, nil
}

// Cancel removes the experiment: its queued runs are dropped; a run already on a lane finishes and its results stay
// (the platform has no way to stop one mid-run).
func (b *PlanBackend) Cancel(_ context.Context, exp string) error {
	err := Remove(b.Root, exp, false)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

// CancelStep kills the step's process group (setsid made it a group of its own) and records exit 143, as a killed
// step would.
func (b *PlanBackend) CancelStep(_ context.Context, dir string) error {
	var st struct {
		PID int `json:"pid"`
	}
	if err := readJSON(filepath.Join(dir, "started.json"), &st); err == nil && st.PID > 1 {
		if err := syscall.Kill(-st.PID, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
			return err
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "exit")); err == nil {
		return nil
	}
	return os.WriteFile(filepath.Join(dir, "exit"), []byte("143\n"), 0o644)
}

var _ plan.Backend = (*PlanBackend)(nil)

// OpenPlan opens a plan for reading (status, results); it asks no backend.
func OpenPlan(root Root, name string) (*plan.Engine, error) {
	return plan.Open(plan.Dir(root.PlansDir(), name), &PlanBackend{Root: root}, SystemClock{})
}

// PlanStatus prints the status of plan name, or of every plan when name is empty; asJSON prints one JSON document.
func PlanStatus(root Root, name string, asJSON bool, w io.Writer) error {
	names := plan.Names(root.PlansDir())
	if name != "" {
		names = []string{name}
	}
	if len(names) == 0 {
		fmt.Fprintln(w, "no plans")
		return nil
	}
	var all []*plan.Status
	for _, n := range names {
		e, err := OpenPlan(root, n)
		if err != nil {
			return err
		}
		s, err := e.Status()
		if err != nil {
			return err
		}
		all = append(all, s)
		if !asJSON {
			s.Render(w)
		}
	}
	if asJSON {
		b, _ := json.MarshalIndent(all, "", " ")
		fmt.Fprintln(w, string(b))
	}
	return nil
}

// PlansSummary is the plan lines of `lanes status`: one line per plan that is not complete.
func PlansSummary(root Root, w io.Writer) {
	for _, n := range plan.Names(root.PlansDir()) {
		e, err := OpenPlan(root, n)
		if err != nil {
			fmt.Fprintf(w, "plan %s: %v\n", n, err)
			continue
		}
		s, err := e.Status()
		if err != nil {
			fmt.Fprintf(w, "plan %s: %v\n", n, err)
			continue
		}
		var done, fly, queued, blocked, failed int
		for _, a := range s.Arms {
			done, fly, queued, blocked, failed = done+a.Done, fly+a.Inflight, queued+a.Queued, blocked+a.Blocked, failed+a.Failed+a.Skipped+a.Parked
		}
		state := "running"
		switch {
		case s.Complete:
			state = "complete"
		case s.Removed:
			state = "removed"
		case s.Paused:
			state = "paused"
		}
		fmt.Fprintf(w, "plan %-24s %-8s nodes done %d, in flight %d, queued %d, blocked %d, failed %d; items %d/%d done, %d parked, %d alarm(s) (lanes plan status %s)\n",
			n, state, done, fly, queued, blocked, failed, s.Items.Done, s.Items.Total, s.Items.Parked, len(s.Alarms), n)
	}
}

// gradingStale is how long a grading marker counts as alive: a manager that restarted mid-grade grades again after it.
const gradingStale = 20 * time.Minute

// RunData reads the newest run of the experiment (a done one when there is one). The grade is the workload's:
// the line of results/runs.jsonl for the run's operator label, produced by the workload's grade verb (`Results`),
// which RunData starts in the background the first time a done run has none and reports as Pending until it is there.
// Empty and blank lines in runs.jsonl are not a grade.
func (b *PlanBackend) RunData(_ context.Context, exp string) (plan.RunData, error) {
	x, err := b.Root.LoadExperiment(exp)
	if err != nil {
		return plan.RunData{}, err
	}
	var rf *RunFile
	for _, r := range sortedRuns(x) {
		r := r
		better := rf == nil || (r.Status == StatusDone && rf.Status != StatusDone) || (r.Status == rf.Status && r.Ended.After(rf.Ended))
		if better {
			rf = &r
		}
	}
	if rf == nil {
		return plan.RunData{}, nil
	}
	rd := plan.RunData{Run: rf.ID, Host: rf.Host, Lane: rf.Lane, WorkloadObserved: rf.Fuzzops.Revision}
	if iso := rf.Isolation; iso != nil {
		rd.Isolation = plan.Isolation{Mode: iso.Mode, OOMKills: iso.OOMKills, Reserved: iso.Reserved, CPUs: iso.CPUs, Memory: iso.Memory}
	}
	var res Result
	if readJSON(filepath.Join(b.Root.RunDir(exp, rf.ID), "result.json"), &res) == nil && res.Prepared != nil {
		rd.SubjectObserved, rd.App = res.Prepared.Version, res.Prepared.AppPack
	}
	if rf.Status != StatusDone {
		return rd, nil
	}
	if g, ok := b.gradeLine(exp, rf.OperatorLabel); ok {
		rd.Grade = g
		return rd, nil
	}
	dir := b.Root.path(exp, "results")
	if msg, err := os.ReadFile(filepath.Join(dir, "grade.error")); err == nil {
		rd.GradeError = "grading failed: " + tailLine(string(msg))
		return rd, nil
	}
	marker := filepath.Join(dir, "grading")
	if fi, err := os.Stat(marker); err == nil && time.Since(fi.ModTime()) < gradingStale {
		rd.Pending = true
		return rd, nil
	}
	if _, err := os.Stat(filepath.Join(dir, "runs.jsonl")); err == nil && b.graded(dir) {
		rd.GradeError = "no graded line for operator " + rf.OperatorLabel
		return rd, nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return rd, err
	}
	if err := os.WriteFile(marker, []byte(time.Now().UTC().Format(time.RFC3339)), 0o644); err != nil {
		return rd, err
	}
	if b.Grading != nil {
		b.Grading.Add(1)
	}
	go func() {
		if b.Grading != nil {
			defer b.Grading.Done()
		}
		b.grade(exp, dir, marker)
	}()
	rd.Pending = true
	return rd, nil
}

// graded: grading ran to its end for this experiment (the marker is gone and no error was left).
func (b *PlanBackend) graded(dir string) bool {
	_, err := os.Stat(filepath.Join(dir, "graded"))
	return err == nil
}

func (b *PlanBackend) grade(exp, dir, marker string) {
	ctx, cancel := context.WithTimeout(context.Background(), gradingStale)
	defer cancel()
	err := Results(ctx, b.Root, exp, nil, io.Discard)
	if err != nil {
		_ = os.WriteFile(filepath.Join(dir, "grade.error"), []byte(err.Error()), 0o644)
	} else {
		_ = os.WriteFile(filepath.Join(dir, "graded"), []byte("ok"), 0o644)
	}
	_ = os.Remove(marker)
}

// gradeLine finds the line of results/runs.jsonl for an operator label. Blank and damaged lines are skipped.
func (b *PlanBackend) gradeLine(exp, label string) (json.RawMessage, bool) {
	f, err := os.Open(b.Root.path(exp, "results", "runs.jsonl"))
	if err != nil {
		return nil, false
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 256<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var head struct {
			Operator string `json:"operator"`
		}
		if json.Unmarshal([]byte(line), &head) == nil && head.Operator == label {
			return json.RawMessage(line), true
		}
	}
	return nil, false
}

// AlarmLog is the engine's alarm sink: a line in <root>/alarms.jsonl (what `lanes status` shows) and, when the plan
// names a hook, the hook command with the alarm in its environment so a notifier can push it.
type AlarmLog struct {
	Root Root
	Hook string
	Logf func(format string, a ...any)
}

// Raise appends the alarm and runs the hook (bounded; a failing hook is logged and never stops the engine).
func (a *AlarmLog) Raise(al plan.Alarm) {
	if err := appendJSONL(a.Root.path("alarms.jsonl"), al); err != nil && a.Logf != nil {
		a.Logf("alarms.jsonl: %v", err)
	}
	if a.Hook == "" {
		return
	}
	line, _ := json.Marshal(al)
	ctx, cancel := context.WithTimeout(context.Background(), alarmHookTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "sh", "-c", a.Hook)
	cmd.Env = append(os.Environ(), "ALARM_JSON="+string(line), "ALARM_KIND="+al.Alarm, "ALARM_PLAN="+al.Plan, "ALARM_KEY="+al.Key,
		"ALARM_SUBJECT="+al.Subject, "ALARM_DETAIL="+al.Detail, "BENCH_ROOT="+string(a.Root))
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	if out, err := cmd.CombinedOutput(); err != nil && a.Logf != nil {
		a.Logf("alarm hook for %s: %v: %s", al.Key, err, tailLine(string(out)))
	}
}

const alarmHookTimeout = 30 * time.Second

// RecentAlarms are the alarms of alarms.jsonl raised within the last window, newest last. A damaged line is skipped.
func RecentAlarms(root Root, window time.Duration, now time.Time) []plan.Alarm {
	lines, _ := readJSONL[plan.Alarm](root.path("alarms.jsonl"))
	var out []plan.Alarm
	for _, a := range lines {
		if a.Alarm != "" && now.Sub(a.At) <= window {
			out = append(out, a)
		}
	}
	return out
}

// AlarmWindow is how far back `lanes status` looks for alarms.
const AlarmWindow = 24 * time.Hour

// printAlarms writes the alarms of the last day (at most the last 10) under a heading, or nothing.
func printAlarms(root Root, w io.Writer) {
	as := RecentAlarms(root, AlarmWindow, time.Now())
	if len(as) == 0 {
		return
	}
	if len(as) > 10 {
		as = as[len(as)-10:]
	}
	fmt.Fprintf(w, "alarms (last %s):\n", AlarmWindow)
	for _, a := range as {
		name := a.Plan
		if name == "" {
			name = a.Host
		}
		fmt.Fprintf(w, "  %s ago  %-13s %s %s: %s\n", time.Since(a.At).Round(time.Minute), a.Alarm, name, a.Subject, a.Detail)
	}
}

// ResultsOptions are the flags of `lanes plan results`.
type ResultsOptions struct {
	TSV, Final, Done bool
	GradeFields      string
}

// PlanResults prints a plan's rows: JSON lines, or TSV with a header. A plan with no rows yet (or a damaged results
// file) prints just the header or nothing, and says so on errw; it never fails for it.
func PlanResults(root Root, name string, o ResultsOptions, w, errw io.Writer) error {
	dir := plan.Dir(root.PlansDir(), name)
	if _, err := os.Stat(filepath.Join(dir, "lock.json")); err != nil {
		return fmt.Errorf("no plan %s", name)
	}
	rows, skipped, err := plan.Results(dir, plan.RowFilter{Final: o.Final, Done: o.Done})
	if err != nil {
		return err
	}
	if skipped > 0 {
		fmt.Fprintf(errw, "plan %s: skipped %d damaged line(s) of results.jsonl\n", name, skipped)
	}
	if o.TSV {
		var fields []string
		for _, f := range strings.Split(o.GradeFields, ",") {
			if f = strings.TrimSpace(f); f != "" {
				fields = append(fields, f)
			}
		}
		return plan.TSV(w, rows, fields)
	}
	enc := json.NewEncoder(w)
	for _, r := range rows {
		if err := enc.Encode(r); err != nil {
			return err
		}
	}
	return nil
}
