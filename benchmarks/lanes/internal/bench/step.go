package bench

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/self-defining-systems/sdo/benchmarks/lanes/internal/lanelock"
)

// Step kinds. A step runs detached on the lane's host, holding the lane's
// step lock, and writes its events and result to its own staging directory;
// it finishes whether or not the manager is there to watch.
const (
	StepBuild   = "build"   // tear down the last operator, fresh app copy, prepare, build check
	StepRun     = "run"     // one fuzzops run, then settle, collect and the safety check
	StepRelease = "release" // tear down the operator, the lane becomes free
	StepUnhold  = "unhold"  // a held lane's evidence is complete: reset the operator's incidents that collect skipped
	StepRecover = "recover" // after a step died: undo any fault left, check nothing is active
)

// Step is everything one step needs, with every path as seen on its host
// (<staging>/<lane>/<id>/step.json).
type Step struct {
	ID         string `json:"id"`
	Kind       string `json:"kind"`
	Lane       string `json:"lane"`
	Host       string `json:"host"`
	Experiment string `json:"experiment,omitempty"`
	Fuzzops    string `json:"fuzzops"`            // the pinned fuzzops checkout
	Workload   string `json:"workload,omitempty"` // the workload adapter ("": fuzzops)
	// IsolationMode is how the lane is isolated on its host (two-way, one-way, one-way-ok); recorded in the row.
	IsolationMode string `json:"isolation_mode,omitempty"`
	FuzzopsRev    string `json:"fuzzops_rev,omitempty"`
	Kubeconfig    string `json:"kubeconfig"`
	LaneDir       string `json:"lane_dir"` // BENCH_LANE_DIR: kept from one build to the next

	// New is the operator and app to build (build), or the ones on the lane (run).
	New *Prepared `json:"new,omitempty"`
	// OperatorFile is the BENCH_OPERATOR content for New.
	OperatorFile *HookOperator `json:"operator_file,omitempty"`
	// Prev is the operator last prepared on the lane (build, release, recover).
	Prev *Prepared `json:"prev,omitempty"`
	// ReuseApp (build): keep the app copy of the last sequence instead of putting down a new one.
	ReuseApp bool `json:"reuse_app,omitempty"`
	// Swap (build): the lane is a held pool lane with a prepared operator; give it New's sources by the hooks' swap.
	Swap bool `json:"swap,omitempty"`
	// Hold (build): the lane is a pool lane: once its build check passed the hooks' snapshot is taken, so a
	// release can return it to this state by reset instead of tearing the operator down.
	Hold bool `json:"hold,omitempty"`

	ReadyFor     Seconds `json:"ready_for,omitempty"`
	ReadyTimeout Seconds `json:"ready_timeout,omitempty"`
	Cap          Seconds `json:"cap,omitempty"`
	// GateAfterCheck: the run gate within gateFreshness of the build check (0: ready_for).
	GateAfterCheck Seconds `json:"gate_after_check,omitempty"`
	Poll           Seconds `json:"poll,omitempty"` // idle polls (default 10 s)
	// BuildBad is the bad seconds the build check tolerates (nil: DefaultBuildBadSeconds); runs never use it.
	BuildBad *int `json:"build_bad,omitempty"`

	Run *RunSpec `json:"run,omitempty"`

	// The lane's cluster: the host's cluster_up and cluster_down commands for it, and whether this
	// build creates it (Create: it is gone; Recreate: delete it first) or this release deletes it.
	HostEnv       []string `json:"host_env,omitempty"` // KEY=VALUE of the host's env
	ClusterUp     string   `json:"cluster_up,omitempty"`
	ClusterDown   string   `json:"cluster_down,omitempty"`
	Create        bool     `json:"create,omitempty"`
	Recreate      bool     `json:"recreate,omitempty"`
	DeleteCluster bool     `json:"delete_cluster,omitempty"`
	// Evidence is the lane's evidence directory on the host, mounted on every node at EvidenceNode.
	Evidence string `json:"evidence,omitempty"`
	// Provider is the host's lane provider ("" or "kind", "vcluster").
	Provider string `json:"provider,omitempty"`
	// Limits is the lane's share of the host's cores and memory (nil: the host has no lane limits).
	Limits *StepLimits `json:"limits,omitempty"`
}

// EvidenceNode is where every lane node mounts the lane's evidence directory (BENCH_EVIDENCE_NODE).
const EvidenceNode = "/bench-evidence"

// RunSpec is the run of a run step (and of a recover step after a run died).
type RunSpec struct {
	ID        string   `json:"id"`
	Fault     string   `json:"fault"`
	Instances string   `json:"instances,omitempty"`
	Seed      int      `json:"seed"`
	Position  int      `json:"position"`
	Label     string   `json:"label"`
	Flags     []string `json:"flags,omitempty"`
}

// Seconds is a duration stored as seconds.
type Seconds float64

// D is s as a duration.
func (s Seconds) D() time.Duration { return time.Duration(float64(s) * float64(time.Second)) }

// Result is <staging>/<lane>/<id>/result.json, written last.
type Result struct {
	Kind    string    `json:"kind"`
	OK      bool      `json:"ok"`
	Reason  string    `json:"reason,omitempty"`
	Started time.Time `json:"started"`
	Ended   time.Time `json:"ended"`
	Fuzzops Version   `json:"fuzzops"`
	// LaneDown, when set, takes the lane out of use until a human looks.
	LaneDown string `json:"lane_down,omitempty"`
	// Prepared is the operator on the lane after the step (nil: none).
	Prepared *Prepared `json:"prepared,omitempty"`
	// Config, when set, says the step cannot succeed on any lane because of its spec (a pack that is not there or
	// does not parse): it names the pack and why. It is not a failure of the lane: the lane is not recreated and
	// the failure does not count against it.
	Config string `json:"config,omitempty"`
	// Recycle, when set, says a held lane was not claimable (for example an incident open on it): nothing was
	// swapped, the lane is recycled and the failure does not count against it.
	Recycle string `json:"recycle,omitempty"`
	// Isolation is what a run was measured under (lane limits, host reservation, OOM kills); nil on a host without them.
	Isolation *Isolation `json:"isolation,omitempty"`
	// Gate is the failed ready gate or build check, when there was one.
	Gate *Gate `json:"gate,omitempty"`

	// A run step's outcome: done, invalid or not_ready (a lane event: nothing was injected).
	Status    string     `json:"status,omitempty"`
	InjectAt  *time.Time `json:"inject_at,omitempty"`
	SettleEnd *time.Time `json:"settle_end,omitempty"`
	Capped    bool       `json:"capped,omitempty"`
	// OtherIncidents are incidents other than the cell's own that were open while the settle ran (not waited for).
	OtherIncidents []string `json:"other_incidents,omitempty"`
	// Residue lists cluster-scoped objects the run left (not there after the build): the lane's
	// cluster must be made anew before it is used again.
	Residue []string `json:"residue,omitempty"`
	// Fresh: this build made a new cluster. Gone: this release deleted the cluster.
	Fresh bool `json:"fresh,omitempty"`
	Gone  bool `json:"gone,omitempty"`
	// EvidenceIncomplete is the operator's own verdict on the cell's evidence after collect: the problems in
	// operator/evidence-manifest.json ("" when none or no manifest). The lane is held, not recycled, until it clears.
	EvidenceIncomplete string `json:"evidence_incomplete,omitempty"`
	// Transcripts is a failed transcripts check after a teardown ("" when complete or not checked).
	Transcripts string `json:"transcripts,omitempty"`
}

// StatusNotReady is a run step whose gate never passed: nothing was injected.
const StatusNotReady = "not_ready"

// Interval is one span in a lane's timeline.
type Interval struct {
	Lane   string    `json:"lane"`
	Step   string    `json:"step"`
	Kind   string    `json:"kind"`
	Start  time.Time `json:"start"`
	End    time.Time `json:"end"`
	OK     bool      `json:"ok"`
	Detail string    `json:"detail,omitempty"`
}

// stepRunner runs one step in dir.
type stepRunner struct {
	s       Step
	dir     string
	log     io.Writer // runner.log
	res     Result
	poll    time.Duration
	hookLog io.Writer
	// wl is the workload adapter (nil: the one the step names; tests set it).
	wl Workload
	// exists answers whether the lane's cluster exists (nil: probe it; tests set it).
	exists func(context.Context) (bool, error)
	// topology reads the host's cores, live the clusters that exist, oom the lane's OOM kill count (nil: the host's;
	// tests set them).
	topology func() (Topology, error)
	live     func() (map[string]bool, error)
	foreign  func() ([]ContainerPin, error) // nil: docker on the host
	oom      func() (int, error)
}

// RunStep runs the step in step.json at path to completion: the step's lock
// is held for its whole life, and result.json and manifest.json are written
// last. It returns an error only when the step could not start at all.
func RunStep(ctx context.Context, path string) error {
	var s Step
	if err := readJSON(path, &s); err != nil {
		return err
	}
	dir := filepath.Dir(path)
	lock, err := os.OpenFile(filepath.Join(filepath.Dir(dir), "step.lock"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fmt.Errorf("lane %s: another step holds it", s.Lane)
	}
	logf, err := os.OpenFile(filepath.Join(dir, "runner.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer logf.Close()
	hooks, err := os.OpenFile(filepath.Join(dir, "hooks.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer hooks.Close()
	r := &stepRunner{s: s, dir: dir, log: logf, hookLog: hooks, poll: max(s.Poll.D(), 0)}
	if r.poll == 0 {
		r.poll = 10 * time.Second
	}
	r.res = Result{Kind: s.Kind, Started: time.Now().UTC(), Prepared: s.Prev}
	if s.IsolationMode != "" {
		r.res.Isolation = &Isolation{Mode: s.IsolationMode}
	}
	// A step is cancelled only by its own deadline: the manager going away must not stop it.
	ctx = context.WithoutCancel(ctx)
	r.res.Fuzzops, err = r.version(ctx)
	switch {
	case err != nil:
		r.fail("fuzzops version: %v", err)
	case s.FuzzopsRev != "" && r.res.Fuzzops.Modified:
		r.fail("fuzzops binary was built from a modified checkout (%s): runs must be measured by the pinned revision", r.res.Fuzzops.Revision)
	case s.FuzzopsRev != "" && !strings.HasPrefix(r.res.Fuzzops.Revision, s.FuzzopsRev):
		r.fail("fuzzops binary is %s, not the pinned %s (rebuild bin/fuzzops)", r.res.Fuzzops.Revision, s.FuzzopsRev)
	default:
		switch s.Kind {
		case StepBuild:
			r.build(ctx)
		case StepRun:
			r.run(ctx)
		case StepRelease:
			r.release(ctx)
		case StepUnhold:
			r.unhold(ctx)
		case StepRecover:
			r.recover(ctx)
		default:
			r.fail("unknown step kind %q", s.Kind)
		}
	}
	r.res.Ended = time.Now().UTC()
	if err := writeJSON(filepath.Join(dir, "result.json"), r.res); err != nil {
		return err
	}
	m, err := StepManifest(dir)
	if err != nil {
		return err
	}
	return writeJSON(filepath.Join(dir, "manifest.json"), m)
}

func (r *stepRunner) logf(f string, a ...any) {
	fmt.Fprintf(r.log, "%s %s\n", time.Now().UTC().Format("15:04:05"), fmt.Sprintf(f, a...))
}

func (r *stepRunner) fail(f string, a ...any) {
	r.res.OK = false
	r.res.Reason = fmt.Sprintf(f, a...)
	r.logf("failed: %s", r.res.Reason)
}

// config fails the step for a reason in its spec that no retry, lane or cluster changes.
func (r *stepRunner) config(f string, a ...any) {
	r.fail(f, a...)
	r.res.Config = r.res.Reason
}

func (r *stepRunner) down(f string, a ...any) {
	r.fail(f, a...)
	r.res.LaneDown = r.res.Reason
}

// interval records one span of the lane's timeline.
func (r *stepRunner) interval(kind string, start time.Time, ok bool, detail string) {
	_ = appendJSONL(filepath.Join(r.dir, "timeline.jsonl"), Interval{Lane: r.s.Lane, Step: r.s.ID, Kind: kind,
		Start: start.UTC(), End: time.Now().UTC(), OK: ok, Detail: detail})
}

// laneEnv is the environment of every fuzzops call and hook on the lane.
func (r *stepRunner) laneEnv(p *Prepared) []string {
	e := []string{"KUBECONFIG=" + r.s.Kubeconfig, "FUZZOPS_KIND_CLUSTER=" + r.s.Lane, "KIND_CLUSTER=" + r.s.Lane,
		"KUBE_CONTEXT=kind-" + r.s.Lane, "FUZZOPS_KUBE_CONTEXT=kind-" + r.s.Lane,
		"BENCH_LANE=" + r.s.Lane, "BENCH_HOST=" + r.s.Host, "BENCH_LANE_DIR=" + r.s.LaneDir, "BENCH_FRESH_FINGERPRINT=" + r.freshFingerprintFile(),
		"BENCH_EVIDENCE_HOST=" + r.s.Evidence, "BENCH_EVIDENCE_NODE=" + EvidenceNode}
	e = append(e, r.s.HostEnv...)
	if p != nil {
		if p.EvidenceRun != "" {
			e = append(e, "BENCH_EVIDENCE_RUN="+p.EvidenceRun)
		}
		e = append(e, "BENCH_EXPERIMENT="+p.Experiment, "FUZZOPS_NAMESPACE="+p.Namespace, "BENCH_PACK="+p.AppPack)
		// The pinned checkout's host registry script (deploy/registry.sh): `push IMAGE...` puts an image where
		// every lane cluster of the host pulls it from.
		e = append(e, "BENCH_REGISTRY="+filepath.Join(r.s.Fuzzops, "deploy", "registry.sh"))
		if p.Operator != "" {
			e = append(e, "BENCH_OPERATOR="+p.Operator)
		}
	}
	return e
}

func (r *stepRunner) hook(p *Prepared) Hook {
	return Hook{Dir: p.HooksPath, Command: p.Command, Env: r.laneEnv(p)}
}

// callHook runs a hook verb as a timeline interval.
func (r *stepRunner) callHook(ctx context.Context, p *Prepared, verb string, extra ...string) HookResult {
	start := time.Now()
	if verb != "idle" {
		r.phase(verb, "")
	}
	fmt.Fprintf(r.hookLog, "--- %s %s %s\n", time.Now().UTC().Format(time.RFC3339), p.Hooks, verb)
	res := r.hook(p).Run(ctx, verb, extra, r.hookLog)
	if verb != "idle" {
		r.interval(verb, start, res.OK(), res.Why(verb))
	}
	return res
}

// call runs one verb of the step's workload; out gets its result stream (nil: runner.log). code is the adapter's
// exit status and why the end of its stderr.
func (r *stepRunner) call(ctx context.Context, env []string, out io.Writer, verb, action string, args ...string) (int, string, error) {
	return r.workload().Call(ctx, WorkloadCall{Verb: verb, Action: action, Args: args, Env: env, Stdout: out, Log: r.log})
}

// workload is the adapter of the step's spec (the built-in fuzzops one by default).
func (r *stepRunner) workload() Workload {
	if r.wl != nil {
		return r.wl
	}
	return NewWorkload(r.s.Workload, r.s.Fuzzops)
}

// tailWriter keeps the last 512 bytes written to it, trimmed to whole lines' text: the reason a command gave.
type tailWriter struct{ b []byte }

func (t *tailWriter) Write(p []byte) (int, error) {
	t.b = append(t.b, p...)
	if len(t.b) > 512 {
		t.b = t.b[len(t.b)-512:]
	}
	return len(p), nil
}

func (t *tailWriter) String() string { return strings.TrimSpace(string(t.b)) }

func (r *stepRunner) version(ctx context.Context) (Version, error) {
	var out bytes.Buffer
	code, _, err := r.call(ctx, nil, &out, VerbIdentity, "")
	var v Version
	if err == nil && code != 0 {
		err = fmt.Errorf("exit %d", code)
	}
	if err == nil {
		err = json.Unmarshal(out.Bytes(), &v)
	}
	return v, err
}

// teardown removes p's operator; false (and the lane down) when that failed.
//
// It is the one way an operator leaves a lane (a build for the next sequence or experiment, a cluster made
// anew or deleted, a release): first wait until the operator is idle (no agent still writing; at most the
// cap), then the hooks' teardown (which deletes the operator so that its evidence sink makes its final
// pass), then the hooks' transcripts check of the sink on the host. A failed check is recorded and the
// reset goes on: what reached the sink is already on the host, and the bench never cleans it.
func (r *stepRunner) teardown(ctx context.Context, p *Prepared) bool {
	if p == nil || p.Hooks == "" {
		return true
	}
	if p.Ready && !r.waitIdle(ctx, p) {
		return false
	}
	if res := r.callHook(ctx, p, "teardown"); !res.OK() {
		r.down("teardown of %s (%s): %s", p.Hooks, p.Experiment, res.Why("teardown"))
		return false
	}
	if p.EvidenceRun != "" {
		if res := r.callHook(ctx, p, "transcripts"); !res.OK() {
			why := res.First
			if why == "" {
				why = res.Why("transcripts")
			}
			r.res.Transcripts = fmt.Sprintf("%s (%s, sink run %s)", why, p.Experiment, p.EvidenceRun)
			r.logf("transcripts: %s", r.res.Transcripts)
		}
	}
	return true
}

// waitIdle polls the operator's idle until it is idle twice in a row, at most for the cap.
func (r *stepRunner) waitIdle(ctx context.Context, p *Prepared) bool {
	start := time.Now()
	limit := r.s.Cap.D()
	if limit <= 0 {
		limit = 30 * time.Minute
	}
	calm, why := 0, ""
	for calm < 2 && time.Since(start) < limit {
		if res := r.callHook(ctx, p, "idle"); res.OK() {
			calm++
		} else {
			calm, why = 0, res.First
			if r.clusterVanished(ctx, "the operator to be idle") {
				return false
			}
		}
		if calm < 2 {
			time.Sleep(r.poll)
		}
	}
	r.interval("idle", start, calm >= 2, why)
	return true
}

// packLoads is the build's pre-flight: the pinned fuzzops loads the spec's pack before anything is made or torn
// down, so a pack that is not there (the spec's app is a path inside the pinned checkout) or does not parse fails
// the step in seconds, naming the pack, instead of at a later command that retries. A fuzzops that cannot say
// (it predates `pack load`, or does not run) is not an objection: the build goes on as before.
func (r *stepRunner) packLoads(ctx context.Context, p *Prepared) bool {
	if p == nil || p.AppPack == "" {
		return true
	}
	code, why, err := r.call(ctx, nil, io.Discard, VerbAppUp, "check-pack", "--pack", p.AppPack)
	if err != nil || code != lanelock.ExitInvalid {
		return true
	}
	r.config("app pack %s does not load (spec app is a path inside the pinned fuzzops checkout %s): %s", p.AppPack, r.s.Fuzzops, why)
	return false
}

// recoverLane undoes a fault a dead run left in ns; false (lane down) when it may still be in place.
func (r *stepRunner) recoverLane(ctx context.Context, p *Prepared) bool {
	if p == nil {
		return true
	}
	start := time.Now()
	r.phase(PhaseRecover, "")
	// ExitBusy is a lane another fuzzops holds: a run orphaned by a killed step,
	// which undoes its own fault when it ends. Wait for it (at most 30 min).
	// ExitInvalid is input no retry can fix (the pack does not load): fail at once with the reason.
	var code int
	var why string
	var err error
	for {
		code, why, err = r.call(ctx, r.laneEnv(p), nil, VerbAppReset, "recover", "--namespace", p.Namespace, "--pack", p.AppPack)
		if code == lanelock.ExitInvalid && err == nil {
			r.interval("recover", start, false, "invalid: "+why)
			r.config("fuzzops recover cannot run on pack %s (exit %d, not retried): %s", p.AppPack, code, why)
			return false
		}
		if err != nil || code != lanelock.ExitBusy {
			break
		}
		if time.Since(start) > 30*time.Minute {
			r.logf("recover: another fuzzops still holds the lane after 30m")
			break
		}
		if r.clusterVanished(ctx, "another fuzzops to release the lane") {
			return false
		}
		time.Sleep(r.poll)
	}
	ok := err == nil && code == 0
	r.interval("recover", start, ok, fmt.Sprintf("exit %d %v", code, err))
	if !ok {
		r.down("fault still active: fuzzops recover in %s exit %d %v", p.Namespace, code, errOrEmpty(err))
	}
	return ok
}

func errOrEmpty(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// build: tear down the operator last prepared on the lane (whatever
// experiment), undo any fault left, a fresh app copy, prepare the new
// operator from its baseline, then the build's own check.
func (r *stepRunner) build(ctx context.Context) {
	// The lane file may say an app is installed on a cluster that is gone: nothing there is reusable.
	if r.distrustPrepared(ctx) {
		r.s.Prev, r.s.Create, r.s.Recreate, r.s.Swap, r.s.ReuseApp = nil, true, false, false, false
	}
	if r.s.Create {
		r.s.Prev, r.res.Prepared = nil, nil
	}
	s, p := r.s, r.s.New
	if !r.packLoads(ctx, p) {
		return
	}
	if !s.Create && !s.Recreate {
		r.reapplyLimits(ctx) // a claim or a reuse: the lane's cpuset may have been undone since it was built
	}
	if s.Swap {
		r.swapIn(ctx)
		return
	}
	// The operator leaves through the teardown path even when the cluster is deleted next.
	if !s.Create && !r.teardown(ctx, s.Prev) {
		return
	}
	if s.Create || s.Recreate {
		if !r.makeCluster(ctx, p) {
			return
		}
	}
	r.res.Prepared = nil
	if !r.recoverLane(ctx, s.Prev) || !r.recoverLane(ctx, p) {
		return
	}
	if p.Hooks != "" {
		// The new experiment's operator may be half there (a build that died): idempotent.
		if !r.teardown(ctx, p) {
			return
		}
	}
	// A rewind keeps the app copy of the last sequence: the lane was just recovered, so the fault is undone, and
	// the bare-app gate below decides whether the copy is still clean (it is rebuilt when it is not).
	rewind := s.ReuseApp && p.Hooks != ""
	if rewind {
		// The recover above undid the fault, but a repair or the fault's own undo can leave the data stores different
		// from a fresh deploy (a migration record gone, a role setting, rows lost). Journeys do not see that, and the
		// golden copy taken next would carry it into every later claim: reuse the copy only when it is the fresh one.
		if why := r.appMatchesFresh(ctx, p); why != "" {
			r.logf("reused app copy is not the fresh deploy's data: %s; rebuilding it", why)
			rewind = false
		}
	}
	if rewind {
		r.interval("app", time.Now(), true, "reused copy")
	} else if !r.freshApp(ctx) {
		return
	}
	pre := *p
	pre.Pack = p.AppPack
	if p.Hooks != "" {
		// The operator comes to a lane whose app has been clean on its own: one that watched an app still warming
		// up would act on a real failing probe.
		if !r.appHealthy(ctx, p, !rewind) {
			if !rewind || ctx.Err() != nil || !r.freshApp(ctx) || !r.appHealthy(ctx, p, true) {
				return
			}
		}
		if err := checkRevision(ctx, p.HooksPath, s.OperatorFile.Revision); err != nil {
			r.fail("hooks %s: %v", p.Hooks, err)
			return
		}
		_ = os.RemoveAll(s.LaneDir)
		if err := os.MkdirAll(s.LaneDir, 0o755); err != nil {
			r.fail("lane dir: %v", err)
			return
		}
		pre.Operator = filepath.Join(s.LaneDir, "operator.json")
		pre.EvidenceRun = s.Experiment + "-" + s.Lane + "-" + s.ID
		if err := writeJSON(pre.Operator, s.OperatorFile); err != nil {
			r.fail("operator file: %v", err)
			return
		}
		// From here the operator may be (partly) on the lane: the next build tears it down.
		r.res.Prepared = &pre
		out := filepath.Join(r.dir, "prepare")
		_ = os.MkdirAll(out, 0o755)
		res := r.callHook(ctx, &pre, "prepare", "BENCH_OUT="+out)
		if !res.OK() {
			r.fail("%s", res.Why("prepare"))
			return
		}
		po, err := parsePrepare(res.Last)
		if err != nil {
			r.fail("%v", err)
			return
		}
		pre.Pack, pre.Version, pre.Memory = po.Pack, po.OperatorVersion, po.Memory
	}
	r.res.Prepared = &pre
	if !r.check(ctx, &pre) {
		return
	}
	if pre.Hooks != "" && !r.proveSink(ctx, &pre) {
		return
	}
	// What is cluster-scoped now is the lane's baseline: a run must leave nothing beyond it.
	objs, err := r.clusterObjects(ctx, &pre)
	if err == nil {
		err = writeFile(filepath.Join(s.LaneDir, "cluster-objects.txt"), []byte(strings.Join(objs, "\n")+"\n"))
	}
	if err != nil {
		r.fail("cluster-scoped objects: %v", err)
		return
	}
	if !r.snapshotLane(ctx, &pre) {
		return
	}
	pre.Ready = true
	pre.CheckedAt = time.Now().UTC()
	r.res.OK = true
}

// gateFreshness bounds "since the build check" for the shorter run gate (spec gate_after_check).
const gateFreshness = 5 * time.Minute

// A pool lane keeps the writes its build check confirmed in a ledger file (ledgerFile; ledgerGolden is its copy
// taken with the app's golden copy, and put back by a reset). A run that finds at least ledgerMin of them starts with
// them (fuzzops run --ledger-in), so the pre-inject data it verifies exists without a long ready gate, and a swap
// needs no journeys span of its own.
const (
	ledgerFile   = "ledger.json"
	ledgerGolden = "ledger.golden.json"
	ledgerUsed   = "ledger.used.json"
	ledgerMin    = 150
	ledgerMargin = 20 // the build check collects ledgerMin+ledgerMargin writes, so no lane lands just under the floor
	ledgerGate   = 20 * time.Second
)

// ledgerWrites is how many confirmed writes the lane's ledger file holds (0 when there is none or it is unreadable).
func ledgerWrites(dir string) int {
	var f struct {
		Version int `json:"version"`
		Count   int `json:"count"`
	}
	b, err := os.ReadFile(filepath.Join(dir, ledgerFile))
	if err != nil || json.Unmarshal(b, &f) != nil || f.Version != 1 {
		return 0
	}
	return f.Count
}

func (r *stepRunner) gateSpan(p *Prepared) time.Duration {
	full, short := r.s.ReadyFor.D(), r.s.GateAfterCheck.D()
	if ledgerWrites(r.s.LaneDir) >= ledgerMin {
		span := min(ledgerGate, full)
		if short > 0 {
			span = min(span, short)
		}
		return span
	}
	if short > 0 && short < full && !p.CheckedAt.IsZero() && time.Since(p.CheckedAt) < gateFreshness {
		return short
	}
	return full
}

// swapIn: a held lane already has the app and a prepared, idle operator on the experiment's baseline. The
// experiment's sources reach it by the hooks' swap (which asserts the lane is still the baseline's), then the
// build's own check decides as for any build.
func (r *stepRunner) swapIn(ctx context.Context) {
	s, p := r.s, r.s.New
	prev := s.Prev
	if prev == nil || !prev.Ready || prev.Hooks == "" || p.Hooks != prev.Hooks {
		r.fail("swap: the lane holds no prepared operator of these hooks")
		return
	}
	// A held lane with an open incident is not a clean start: it is not claimed, it is recycled.
	if res := r.callHook(ctx, prev, "idle"); !res.OK() && strings.HasPrefix(res.First, "alarm:") {
		r.res.Prepared, r.res.Recycle = prev, res.First
		r.fail("pool lane not claimable: %s", res.First)
		return
	}
	pre := *p
	pre.Pack, pre.Version, pre.Memory, pre.EvidenceRun = prev.Pack, prev.Version, prev.Memory, prev.EvidenceRun
	pre.Golden = prev.Golden
	pre.Operator = filepath.Join(s.LaneDir, "operator.json")
	if err := writeJSON(pre.Operator, s.OperatorFile); err != nil {
		r.fail("operator file: %v", err)
		return
	}
	if err := checkRevision(ctx, p.HooksPath, s.OperatorFile.Revision); err != nil {
		r.fail("hooks %s: %v", p.Hooks, err)
		return
	}
	// From here the lane's operator runs this experiment's sources: a failure leaves it to the next build's teardown.
	r.res.Prepared = &pre
	out := filepath.Join(r.dir, "swap")
	_ = os.MkdirAll(out, 0o755)
	start := time.Now()
	res := r.callHook(ctx, &pre, "swap", "BENCH_OUT="+out)
	if !res.OK() {
		r.interval("swap", start, false, res.Why("swap"))
		r.fail("%s", res.Why("swap"))
		return
	}
	po, err := parsePrepare(res.Last)
	if err != nil {
		r.fail("%v", err)
		return
	}
	pre.Pack, pre.Version, pre.Memory = po.Pack, po.OperatorVersion, po.Memory
	r.interval("swap", start, true, "sources swapped")
	if !r.swapCheck(ctx, &pre) || !r.proveSink(ctx, &pre) {
		return
	}
	if !r.snapshotLane(ctx, &pre) {
		return
	}
	pre.Ready = true
	pre.CheckedAt = time.Now().UTC()
	r.res.OK = true
}

// swapCheck is the build check of a swap. The lane passed the full check when it was built and the swap asserted it is
// still the baseline's and idle, so a lane with a ledger of ledgerMin writes only waits for an idle operator; the
// journeys of the run's gate cover the app. Without such a ledger it is the full check, which writes one.
func (r *stepRunner) swapCheck(ctx context.Context, p *Prepared) bool {
	if n := ledgerWrites(r.s.LaneDir); n >= ledgerMin {
		start := time.Now()
		r.phase(PhaseCheck, "")
		if reasons, _ := r.waitOperatorIdle(ctx, p, start); len(reasons) == 0 {
			r.interval("check", start, true, fmt.Sprintf("skipped: the lane's ledger holds %d confirmed writes", n))
			return true
		}
	}
	return r.check(ctx, p)
}

// freshApp replaces the app copy of the last sequence and puts down a new one from the pack's seed.
func (r *stepRunner) freshApp(ctx context.Context) bool {
	s, p := r.s, r.s.New
	env := r.laneEnv(p)
	start := time.Now()
	r.phase(PhaseApp, "")
	if q := s.Prev; q != nil && (q.Namespace != p.Namespace || q.AppPack != p.AppPack) {
		if code, _, err := r.call(ctx, r.laneEnv(q), nil, VerbAppUp, "down", "--pack", q.AppPack, "--namespace", q.Namespace); err != nil || code != 0 {
			r.interval("app", start, false, "down of the last app copy")
			r.fail("app down %s: exit %d %v", q.Namespace, code, errOrEmpty(err))
			return false
		}
	}
	for _, verb := range []string{"down", "up"} {
		if code, _, err := r.call(ctx, env, nil, VerbAppUp, verb, "--pack", p.AppPack, "--namespace", p.Namespace); err != nil || code != 0 {
			r.interval("app", start, false, "app "+verb)
			r.fail("app %s: exit %d %v", verb, code, errOrEmpty(err))
			return false
		}
	}
	if !r.recordFresh(ctx, p) {
		r.interval("app", start, false, "fingerprint of the fresh copy")
		return false
	}
	r.interval("app", start, true, freshCopyDetail)
	return true
}

// freshFingerprintFile keeps the data stores' fingerprint of the lane's last fresh app copy, beside the lane
// directory (a build empties that).
func (r *stepRunner) freshFingerprintFile() string {
	return filepath.Join(filepath.Dir(r.s.LaneDir), "fresh-fingerprint.json")
}

// freshCopyDetail marks the app interval of a build that put down a fresh copy.
const freshCopyDetail = "fresh copy"

// recordFresh takes the fingerprint of a copy that was just put down (`fuzzops app fingerprint`; a few tries while
// the stores settle).
func (r *stepRunner) recordFresh(ctx context.Context, p *Prepared) bool {
	file := r.freshFingerprintFile()
	_ = os.Remove(file)
	var code int
	var err error
	for try := 0; try < 3; try++ {
		if code, _, err = r.call(ctx, r.laneEnv(p), io.Discard, VerbAppSnapshot, "write", "--pack", p.AppPack, "--namespace", p.Namespace, "--out", file); err == nil && code == 0 {
			return true
		}
		time.Sleep(r.poll)
	}
	r.fail("fingerprint of the fresh app copy: exit %d %v", code, errOrEmpty(err))
	return false
}

// appMatchesFresh says why the lane's current app copy cannot be shown to hold a fresh deploy's data ("" when it
// can): the fingerprint differs or there is no fresh one to compare with.
func (r *stepRunner) appMatchesFresh(ctx context.Context, p *Prepared) string {
	file := r.freshFingerprintFile()
	if _, err := os.Stat(file); err != nil {
		return "no fingerprint of a fresh copy on this lane"
	}
	code, _, err := r.call(ctx, r.laneEnv(p), io.Discard, VerbAppSnapshot, "verify", "--pack", p.AppPack, "--namespace", p.Namespace, "--against", file)
	if err != nil || code != 0 {
		return fmt.Sprintf("fingerprint check exit %d %s (see the runner log)", code, errOrEmpty(err))
	}
	return ""
}

// appHealthy waits, before any operator is restored, until the pack's journeys have been clean for ready_for on
// the bare app (retrying until ready_timeout); the lane is rebuilt otherwise. It tolerates the same few bad seconds as
// the build check: the pack's concurrent writes to one branch occasionally conflict (a lone failed request, steady for
// as long as the app runs), which says nothing about the app's health.
// With final false a failure is only logged and returned: the caller rebuilds the app copy and asks again.
func (r *stepRunner) appHealthy(ctx context.Context, p *Prepared, final bool) bool {
	start := time.Now()
	r.phase(PhaseAppHealth, "")
	for try := 1; ; try++ {
		code, _, err := r.call(ctx, r.laneEnv(p), nil, VerbGate, "check", "--pack", p.AppPack, "--namespace", p.Namespace,
			"--duration", r.s.ReadyFor.D().String(), "--clean", "--max-bad-seconds", fmt.Sprint(r.s.buildBad()))
		if err == nil && code == 0 {
			r.interval("app-health", start, true, fmt.Sprintf("clean on try %d", try))
			return true
		}
		r.logf("app check (no operator) try %d: exit %d %s", try, code, errOrEmpty(err))
		if r.clusterVanished(ctx, "the app to be healthy") {
			return false
		}
		if time.Since(start)+r.s.ReadyFor.D() > r.s.ReadyTimeout.D() {
			r.interval("app-health", start, false, fmt.Sprintf("exit %d %s", code, errOrEmpty(err)))
			if final {
				r.fail("app not clean for %s within %s before the operator was restored", r.s.ReadyFor.D(), r.s.ReadyTimeout.D())
			} else {
				r.logf("reused app copy not clean: rebuilding it")
			}
			return false
		}
		time.Sleep(r.poll)
	}
}

// proveSink: a lane is ready only when the hooks' transcripts check finds the operator's sink on the host
// (a mount that resolves inside a node instead of the host fails here). The sink may take a few seconds.
func (r *stepRunner) proveSink(ctx context.Context, p *Prepared) bool {
	start := time.Now()
	var res HookResult
	limit := min(2*time.Minute, r.s.ReadyTimeout.D())
	for first := true; first || time.Since(start) < limit; first = false {
		if res = r.callHook(ctx, p, "transcripts"); res.OK() {
			return true
		}
		if why := res.Why("transcripts"); strings.Contains(why, "transcripts missing") {
			// The sink is in place; an earlier incident of its run lacks a rollout. That is a coverage gap of that
			// run (recorded at its teardown and held by its evidence check), not a reason to fail this lane.
			r.logf("sink proven; earlier run: %s", why)
			return true
		}
		time.Sleep(r.poll)
	}
	r.fail("evidence sink not on the host (%s/%s): %s", r.s.Evidence, p.EvidenceRun, res.Why("transcripts"))
	return false
}

// shell runs a host command (cluster_up, cluster_down) in the pinned fuzzops checkout.
func (r *stepRunner) shell(ctx context.Context, kind, cmdline string, env []string) bool {
	start := time.Now()
	r.phase(kind, "")
	ctx, cancel := context.WithTimeout(ctx, 45*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "sh", "-c", cmdline)
	cmd.Dir = r.s.Fuzzops
	cmd.Env = append(append(os.Environ(), r.s.HostEnv...), env...)
	cmd.Stdout, cmd.Stderr = r.log, r.log
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	r.logf("%s: %s", kind, cmdline)
	err := cmd.Run()
	r.interval(kind, start, err == nil, errOrEmpty(err))
	if err != nil {
		r.fail("%s: %v", kind, err)
	}
	return err == nil
}

// makeClusterUp takes the lane's cores (when the host limits lanes) and runs the host's cluster_up with them.
func (r *stepRunner) makeClusterUp(ctx context.Context) bool {
	env, ok := r.laneEnvLimits(ctx)
	if !ok {
		return false
	}
	_ = os.RemoveAll(r.s.LaneDir)
	if !r.shell(ctx, "cluster-up", r.s.ClusterUp, env) {
		// A failed cluster-up removes its half-built cluster: nothing the lane file recorded is installed.
		r.res.Prepared = nil
		r.laneCreated(ctx, false)
		return false
	}
	r.laneCreated(ctx, true)
	r.res.Fresh = true
	return true
}

// makeCluster deletes (Recreate) and creates the lane's cluster, then puts the app's images where the
// nodes find them (the pack's images script, `fuzzops app images`).
func (r *stepRunner) makeCluster(ctx context.Context, p *Prepared) bool {
	s := r.s
	if s.ClusterUp == "" {
		r.down("the lane's cluster must be made anew, but the host has no cluster_up command")
		return false
	}
	if s.Recreate && !r.shell(ctx, "cluster-down", s.ClusterDown, nil) {
		r.res.LaneDown = r.res.Reason
		return false
	}
	if !r.makeClusterUp(ctx) {
		return false
	}
	if s.Provider == ProviderVcluster {
		r.logf("images: not loaded into nodes (a virtual cluster pulls from registries; the host's mirror serves local images)")
		return true
	}
	start := time.Now()
	r.phase(PhaseImages, "")
	code, _, err := r.call(ctx, r.laneEnv(p), nil, VerbAppUp, "images", "--pack", p.AppPack, "--cluster", s.Lane)
	r.interval("images", start, err == nil && code == 0, fmt.Sprintf("exit %d %s", code, errOrEmpty(err)))
	if err != nil || code != 0 {
		r.fail("app images: exit %d %s", code, errOrEmpty(err))
		return false
	}
	return true
}

// clusterKinds are the cluster-scoped kinds a fault could leave behind.
var clusterKinds = "namespaces,validatingwebhookconfigurations,mutatingwebhookconfigurations,clusterroles,clusterrolebindings," +
	"customresourcedefinitions,priorityclasses,storageclasses,ingressclasses,runtimeclasses,apiservices,persistentvolumes"

// clusterObjects lists the cluster's cluster-scoped objects (kind/name), sorted.
func (r *stepRunner) clusterObjects(ctx context.Context, p *Prepared) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", "--context", "kind-"+r.s.Lane, "get", clusterKinds, "-o", "name")
	cmd.Env = append(os.Environ(), r.laneEnv(p)...)
	out, err := cmd.Output()
	if err != nil {
		return nil, err
	}
	var objs []string
	for _, l := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		// A persistent volume follows its claim; an app's claims come and go with the app.
		if l != "" && !strings.HasPrefix(l, "persistentvolume/") {
			objs = append(objs, l)
		}
	}
	sort.Strings(objs)
	return objs, nil
}

// residue lists cluster-scoped objects that are there now and were not after the build.
func (r *stepRunner) residue(ctx context.Context, p *Prepared) ([]string, error) {
	b, err := os.ReadFile(filepath.Join(r.s.LaneDir, "cluster-objects.txt"))
	if err != nil {
		return nil, err
	}
	base := map[string]bool{}
	for _, l := range strings.Split(string(b), "\n") {
		base[l] = true
	}
	now, err := r.clusterObjects(ctx, p)
	if err != nil {
		return nil, err
	}
	var extra []string
	for _, o := range now {
		if !base[o] {
			extra = append(extra, o)
		}
	}
	return extra, nil
}

// check is the build's own check: one span of ready_for in which the journeys
// were clean (`fuzzops check --clean`) and the operator idle on every poll,
// tried again until ready_timeout (a freshly prepared operator takes a while
// to start). Failed tries with an alarm are kept as gate-time alarms.
func (r *stepRunner) check(ctx context.Context, p *Prepared) bool {
	start := time.Now()
	r.phase(PhaseCheck, "")
	all, alarm := r.waitOperatorIdle(ctx, p, start)
	if r.res.LaneDown != "" {
		return false
	}
	for try := 1; ; try++ {
		reasons, alarmed := r.checkOnce(ctx, p)
		alarm = alarm || alarmed
		if len(reasons) == 0 {
			r.interval("check", start, true, fmt.Sprintf("clean on try %d", try))
			if alarm {
				r.res.Gate = &Gate{Lane: r.s.Lane, Host: r.s.Host, Where: "build", At: time.Now().UTC(), Reasons: all, Alarm: true}
			}
			return true
		}
		for _, w := range reasons {
			if len(all) == 0 || all[len(all)-1] != w {
				all = append(all, w)
			}
		}
		r.logf("build check try %d: %s", try, strings.Join(reasons, "; "))
		if time.Since(start)+r.s.ReadyFor.D() > r.s.ReadyTimeout.D() {
			break
		}
	}
	r.interval("check", start, false, strings.Join(all, "; "))
	r.res.Gate = &Gate{Lane: r.s.Lane, Host: r.s.Host, Where: "build", At: time.Now().UTC(), Reasons: all, Alarm: alarm}
	r.fail("build check: not clean for %s within %s: %s", r.s.ReadyFor.D(), r.s.ReadyTimeout.D(), strings.Join(all, "; "))
	return false
}

// waitOperatorIdle polls the hooks' idle until the operator is idle (or ready_timeout leaves no room for a
// ready_for span), before the journeys span starts: a controller that is still starting would otherwise fail
// the first span after it had run for ready_for. It returns the distinct reasons seen and whether one was an alarm.
func (r *stepRunner) waitOperatorIdle(ctx context.Context, p *Prepared, start time.Time) ([]string, bool) {
	var reasons []string
	alarm := false
	if p.Hooks == "" {
		return nil, false
	}
	for {
		res := r.callHook(ctx, p, "idle")
		if res.OK() {
			return nil, alarm
		}
		why := res.Why("idle")
		if res.Exit == 1 && res.First != "" {
			why = res.First
		}
		if len(reasons) == 0 || reasons[len(reasons)-1] != why {
			reasons = append(reasons, why)
		}
		alarm = alarm || strings.HasPrefix(why, "alarm:")
		if r.clusterVanished(ctx, "the operator to be idle") {
			return reasons, alarm
		}
		if ctx.Err() != nil || time.Since(start)+r.s.ReadyFor.D() > r.s.ReadyTimeout.D() {
			return reasons, alarm
		}
		time.Sleep(r.poll)
	}
}

// checkOnce is one try of the build check; it returns why it failed (nothing: clean).
func (r *stepRunner) checkOnce(ctx context.Context, p *Prepared) ([]string, bool) {
	type out struct {
		code int
		err  error
	}
	done := make(chan out, 1)
	go func() {
		code, _, err := r.call(ctx, r.laneEnv(p), nil, VerbGate, "check", "--pack", p.Pack, "--namespace", p.Namespace,
			"--duration", r.s.ReadyFor.D().String(), "--clean", "--max-bad-seconds", fmt.Sprint(r.s.buildBad()),
			"--ledger-out", filepath.Join(r.s.LaneDir, ledgerFile),
			"--min-confirmed", fmt.Sprint(ledgerMin+ledgerMargin), "--max-duration", (3 * r.s.ReadyFor.D()).String())
		done <- out{code, err}
	}()
	var reasons []string
	alarm := false
	busy := func() {
		if p.Hooks == "" {
			return
		}
		if res := r.callHook(ctx, p, "idle"); !res.OK() {
			why := res.Why("idle")
			if res.Exit == 1 && res.First != "" {
				why = res.First
			}
			if len(reasons) == 0 || reasons[len(reasons)-1] != why {
				reasons = append(reasons, why)
			}
			alarm = alarm || strings.HasPrefix(why, "alarm:")
		}
	}
	busy()
	var o out
	for wait := true; wait; {
		select {
		case o = <-done:
			wait = false
		case <-time.After(r.poll):
			busy()
		}
	}
	busy()
	if o.err != nil || o.code != 0 {
		reasons = append(reasons, fmt.Sprintf("journeys not clean (fuzzops check exit %d %s)", o.code, errOrEmpty(o.err)))
	}
	if len(reasons) > 0 {
		// Wait a poll before the next try: a check that failed at once would otherwise spin.
		time.Sleep(r.poll)
	}
	return reasons, alarm
}

// record is the part of a fuzzops run record the bench reads.
type record struct {
	Valid         bool   `json:"valid"`
	InvalidReason string `json:"invalid_reason"`
	Error         string `json:"error"`
	Life          *struct {
		InjectAt time.Time `json:"inject_at"`
		Said     []struct {
			Report struct {
				Claimed map[string]time.Time `json:"claimed"`
			} `json:"report"`
		} `json:"said"`
	} `json:"lifecycle"`
	Ready *struct {
		Misses []struct {
			Why string `json:"why"`
		} `json:"misses"`
	} `json:"ready"`
}

var labelChars = regexp.MustCompile(`[^A-Za-z0-9]`)

// run: one fuzzops run on the attached app (the gate inside it), then settle
// (operator idle twice in a row, at most until cap after the inject),
// collect, and the safety check (recover, then the fault must not be active).
func (r *stepRunner) run(ctx context.Context) {
	s, p, rs := r.s, r.s.New, r.s.Run
	defer r.isolate(ctx)()
	r.res.Prepared = p
	env := append(r.laneEnv(p), "FUZZOPS_EXEC_OUT="+filepath.Join(r.dir, "exec"), "BENCH_RUN="+rs.ID, "BENCH_FAULT="+faultArg(rs.Fault))
	span := r.gateSpan(p)
	args := []string{"--skip-reset", "--pack", p.Pack, "--namespace", p.Namespace, "--operator", rs.Label,
		"--ready-for", span.String(), "--ready-timeout", s.ReadyTimeout.D().String(), "--seed", fmt.Sprint(rs.Seed)}
	if p.Hooks != "" {
		hookCmd := "cd " + shellQuote(p.HooksPath) + " && " + p.Command
		args = append(args, "--ready-cmd", hookCmd+" idle")
		if slices.ContainsFunc(rs.Flags, func(f string) bool { return strings.HasPrefix(f, "--window-end-confirm") }) {
			// The operator's idle verdict ends the window once it dealt with the fault (run: {window-end-confirm: 30s}).
			args = append(args, "--window-end-cmd", hookCmd+" idle")
		}
		env = append(env, "FUZZOPS_EXEC_CMD_"+strings.ToUpper(labelChars.ReplaceAllString(strings.TrimPrefix(rs.Label, "exec:"), "_"))+"="+hookCmd+" watch")
	}
	if rs.Fault == Healthy {
		args = append(args, "--fault", "none")
	} else {
		args = append(args, "--instance-file", rs.Instances, "--instance", rs.Fault)
	}
	if ledgerWrites(s.LaneDir) >= ledgerMin {
		// The run changes the app's data: the ledger is spent until a reset puts the golden copy (and its ledger) back.
		used := filepath.Join(s.LaneDir, ledgerUsed)
		if err := os.Rename(filepath.Join(s.LaneDir, ledgerFile), used); err == nil {
			args = append(args, "--ledger-in", used)
		}
	}
	args = append(args, rs.Flags...)
	start := time.Now()
	recPath := filepath.Join(r.dir, "record.json")
	f, err := os.Create(recPath)
	if err != nil {
		r.fail("record: %v", err)
		return
	}
	logf, _ := os.Create(filepath.Join(r.dir, "fuzzops.log"))
	r.log = io.MultiWriter(r.log, logf)
	r.phase(PhaseGate, "")
	plain := r.log
	r.log = io.MultiWriter(plain, &phaseWriter{r: r, last: PhaseGate})
	code, _, err := r.call(ctx, env, f, VerbRun, "run", args...)
	r.log = plain
	f.Close()
	var rec record
	rerr := readJSON(recPath, &rec)
	switch {
	case err != nil || (code != 0 && code != 1) || rerr != nil:
		r.res.Status = StatusInvalid
		r.fail("lane lost: fuzzops run exit %d %s %s", code, errOrEmpty(err), errOrEmpty(rerr))
	case rec.InvalidReason == "not ready":
		r.res.Status = StatusNotReady
		g := &Gate{Run: rs.ID, Lane: s.Lane, Host: s.Host, Where: "gate", At: time.Now().UTC()}
		if rec.Ready != nil {
			for _, m := range rec.Ready.Misses {
				g.Reasons = append(g.Reasons, m.Why)
				g.Alarm = g.Alarm || strings.Contains(m.Why, "operator: alarm:")
			}
		}
		r.res.Gate = g
		r.fail("not ready: %s", rec.Error)
	case !rec.Valid:
		r.res.Status = StatusInvalid
		r.fail("%s", strings.TrimSpace(rec.InvalidReason+": "+rec.Error))
	default:
		r.res.Status, r.res.OK = StatusDone, true
	}
	r.interval("run", start, r.res.OK, r.res.Reason)
	from := start
	if rec.Life != nil && !rec.Life.InjectAt.IsZero() {
		at := rec.Life.InjectAt.UTC()
		r.res.InjectAt, from = &at, at
		r.settle(ctx, p, at, ownDetected(rec))
	}
	if p.Hooks != "" {
		out := filepath.Join(r.dir, "operator")
		_ = os.MkdirAll(out, 0o755)
		end := time.Now().UTC()
		if r.res.SettleEnd != nil {
			end = *r.res.SettleEnd
		}
		res := r.callHook(ctx, p, "collect", "BENCH_OUT="+out, "BENCH_RUN="+rs.ID, "BENCH_FAULT="+faultArg(rs.Fault),
			"BENCH_INTERVAL_START="+from.UTC().Format(time.RFC3339), "BENCH_INTERVAL_END="+end.Format(time.RFC3339))
		if !res.OK() {
			r.logf("collect failed (recorded, the run stands): %s", res.Why("collect"))
		}
		r.res.EvidenceIncomplete = evidenceProblems(filepath.Join(out, EvidenceManifest))
	}
	r.safety(ctx, p, rs)
}

func faultArg(f string) string {
	if f == Healthy {
		return "none"
	}
	return f
}

// ownDetected is the time the operator first said it detected the fault (zero: it never did). The incident it
// opened is the cell's own; the settle waits for that one and only records any other.
func ownDetected(rec record) time.Time {
	if rec.Life == nil {
		return time.Time{}
	}
	var first time.Time
	for _, st := range rec.Life.Said {
		if t := st.Report.Claimed["detected"]; !t.IsZero() && (first.IsZero() || t.Before(first)) {
			first = t
		}
	}
	return first
}

// ignoredPrefix starts the idle hook's first line when it answered idle although a different incident than the
// cell's own is open (BenchOwnDetectedEnv set): the rest of the line names it.
const ignoredPrefix = "ignored: "

// BenchOwnDetectedEnv tells the idle hook which incident is the cell's own (the one detected at this RFC 3339 time).
const BenchOwnDetectedEnv = "BENCH_OWN_DETECTED_AT"

// settle waits until the operator is idle twice in a row, at most until cap after the inject. With the cell's own
// incident known (own), the operator is idle once that incident is closed and its closing work done; any other open
// incident is recorded in the result (OtherIncidents) and not waited for.
func (r *stepRunner) settle(ctx context.Context, p *Prepared, injectAt, own time.Time) {
	start := time.Now()
	r.phase(PhaseSettle, "")
	if p.Hooks == "" {
		now := time.Now().UTC()
		r.res.SettleEnd = &now
		return
	}
	deadline := injectAt.Add(r.s.Cap.D())
	calm, why := 0, ""
	var extra []string
	if !own.IsZero() {
		extra = append(extra, BenchOwnDetectedEnv+"="+own.UTC().Format(time.RFC3339Nano))
	}
	for {
		res := r.callHook(ctx, p, "idle", extra...)
		if res.OK() {
			if other, ok := strings.CutPrefix(res.First, ignoredPrefix); ok && !slices.Contains(r.res.OtherIncidents, other) {
				r.res.OtherIncidents = append(r.res.OtherIncidents, other)
			}
			calm++
		} else {
			calm, why = 0, res.First
			if why == "" || res.Exit != 1 {
				why = res.Why("idle")
			}
		}
		if calm >= 2 {
			break
		}
		if time.Now().After(deadline) {
			r.res.Capped = true
			if r.res.Status == StatusDone {
				r.res.Status, r.res.OK = StatusInvalid, false
			}
			r.res.Reason = "capped: " + why
			break
		}
		time.Sleep(r.poll)
	}
	now := time.Now().UTC()
	r.res.SettleEnd = &now
	r.interval("settle", start, !r.res.Capped, r.res.Reason)
}

// safety: `fuzzops recover`, then the fault must not be active; one undo is
// tried, else the lane goes down with the fault named.
func (r *stepRunner) safety(ctx context.Context, p *Prepared, rs *RunSpec) {
	start := time.Now()
	r.phase(PhaseSafety, "")
	if p == nil {
		return
	}
	if !r.recoverLane(ctx, p) {
		return
	}
	if rs == nil || rs.Fault == Healthy || rs.Instances == "" {
		r.interval("safety", start, true, "")
		if rs != nil {
			r.checkResidue(ctx, p)
		}
		return
	}
	env := r.laneEnv(p)
	active := func(verb string) (bool, error) {
		var out bytes.Buffer
		code, _, err := r.call(ctx, env, &out, VerbRun, verb, "--file", rs.Instances, "--id", rs.Fault, "--pack", p.AppPack, "--namespace", p.Namespace)
		var answer struct {
			Active *bool `json:"active"`
		}
		switch {
		case err != nil:
			return true, err
		case code != 0 || json.Unmarshal([]byte(lastNonEmpty(out.Bytes())), &answer) != nil || answer.Active == nil:
			return true, fmt.Errorf("%s exit %d: %s", verb, code, strings.TrimSpace(out.String()))
		}
		return *answer.Active, nil
	}
	on, err := active("active")
	if on || err != nil {
		r.logf("fault %s active after the run (%v): undo", rs.Fault, err)
		on, err = active("undo")
	}
	r.interval("safety", start, !on && err == nil, errOrEmpty(err))
	if on || err != nil {
		r.down("fault still active: %s %s", rs.Fault, errOrEmpty(err))
		return
	}
	r.checkResidue(ctx, p)
}

// checkResidue records cluster-scoped objects left beyond the lane's baseline.
func (r *stepRunner) checkResidue(ctx context.Context, p *Prepared) {
	start := time.Now()
	r.phase(PhaseResidue, "")
	extra, err := r.residue(ctx, p)
	if err != nil {
		r.logf("cluster-scoped residue not checked: %v", err)
		extra = []string{"unchecked: " + err.Error()}
	}
	r.res.Residue = extra
	r.interval("residue", start, len(extra) == 0, strings.Join(extra, ", "))
}

// release tears the lane's operator down.
func (r *stepRunner) release(ctx context.Context) {
	if r.distrustPrepared(ctx) {
		// The cluster is gone, and with it the operator: there is nothing to tear down or reset.
		r.res.OK, r.res.Prepared, r.res.Gone = true, nil, r.s.ClusterUp != ""
		r.freeLaneCores()
		return
	}
	if r.s.DeleteCluster {
		if !r.teardown(ctx, r.s.Prev) {
			return
		}
		if r.shell(ctx, "cluster-down", r.s.ClusterDown, nil) {
			_ = os.RemoveAll(r.s.LaneDir)
			r.freeLaneCores()
			r.res.OK, r.res.Prepared, r.res.Gone = true, nil, true
		} else {
			r.res.LaneDown = r.res.Reason
		}
		return
	}
	if p := r.s.Prev; p != nil && p.Golden && p.Hooks != "" {
		if r.resetLane(ctx, p) {
			kept := *p
			kept.Reset = true
			r.res.OK, r.res.Prepared = true, &kept
			return
		}
	}
	if r.teardown(ctx, r.s.Prev) {
		r.res.OK, r.res.Prepared = true, nil
	}
}

// resetLane returns a pool lane that has a golden copy to its prepared state by the hooks' reset (app, accepted
// repository, controller) instead of tearing the operator down. A failure is logged and the caller tears down as
// before: the next build is the ordinary full one.
func (r *stepRunner) resetLane(ctx context.Context, p *Prepared) bool {
	out := filepath.Join(r.dir, "reset")
	_ = os.MkdirAll(out, 0o755)
	start := time.Now()
	res := r.callHook(ctx, p, "reset", "BENCH_OUT="+out)
	if !res.OK() {
		r.interval("reset", start, false, res.Why("reset"))
		r.logf("reset of the pool lane failed, falling back to teardown: %s", res.Why("reset"))
		return false
	}
	r.interval("reset", start, true, "returned to the golden copy")
	// The app is back to the data the ledger describes.
	if err := copyFile(filepath.Join(r.s.LaneDir, ledgerGolden), filepath.Join(r.s.LaneDir, ledgerFile), 0o644); err != nil {
		_ = os.Remove(filepath.Join(r.s.LaneDir, ledgerFile))
	}
	return true
}

// snapshotLane takes the golden copy of a pool lane that just passed its build check. A failure only means the
// lane is released by teardown, as before.
//
// It returns false, with the build failed, when the app differs from a fresh deploy: such a lane is never handed on
// as ready (a ready lane without a golden copy cannot be claimed, and one with a contaminated copy must not be).
func (r *stepRunner) snapshotLane(ctx context.Context, p *Prepared) bool {
	if !r.s.Hold || p.Hooks == "" || p.Golden {
		return true
	}
	// The golden copy is what every later claim returns to: take it only of an app that is the fresh deploy's.
	if why := r.appMatchesFresh(ctx, p); why != "" {
		r.fail("no golden copy of an app that differs from a fresh deploy: %s", why)
		return false
	}
	out := filepath.Join(r.dir, "snapshot")
	_ = os.MkdirAll(out, 0o755)
	start := time.Now()
	if res := r.callHook(ctx, p, "snapshot", "BENCH_OUT="+out); !res.OK() {
		r.interval("snapshot", start, false, res.Why("snapshot"))
		// A held lane is claimed by swap, whose golden gate needs a proven golden copy: a lane without one is not
		// left ready (the claim would fail on it and the lane be torn down then). The one failure that keeps the
		// copy is the operator not being idle after a whole copy: the lane waits for the operator, then it is proven.
		if !snapshotKeptCopy(res) {
			r.fail("no golden copy of the pool lane: %s", res.Why("snapshot"))
			return false
		}
		r.logf("snapshot of the pool lane kept its copy, waiting for the operator to be idle: %s", res.Why("snapshot"))
		if reasons, _ := r.waitOperatorIdle(ctx, p, time.Now()); len(reasons) > 0 {
			r.fail("no proven golden copy of the pool lane: the operator is not idle after the snapshot: %s", strings.Join(reasons, "; "))
			return false
		}
	}
	r.interval("snapshot", start, true, "golden copy taken")
	if err := copyFile(filepath.Join(r.s.LaneDir, ledgerFile), filepath.Join(r.s.LaneDir, ledgerGolden), 0o644); err != nil {
		_ = os.Remove(filepath.Join(r.s.LaneDir, ledgerGolden))
	}
	p.Golden = true
	return true
}

// snapshotKeptCopy reports whether a failed snapshot hook says, on its last stdout line, that the copy is whole and
// was kept: {"golden": "kept", ...} (the hooks' contract: only the operator was not idle after the copy).
func snapshotKeptCopy(res HookResult) bool {
	var v struct {
		Golden string `json:"golden"`
	}
	return json.Unmarshal([]byte(res.Last), &v) == nil && v.Golden == "kept"
}

// recover follows a step that died: undo what it may have left.
func (r *stepRunner) recover(ctx context.Context) {
	p := r.s.Prev
	// A lane whose cluster is gone has no fault active: there is nothing to undo, and the next build starts from nothing.
	if r.distrustPrepared(ctx) {
		r.res.OK, r.res.Gone = true, true
		return
	}
	r.safety(ctx, p, r.s.Run)
	if r.res.LaneDown == "" {
		r.res.OK = true
	}
}

// unhold runs the hook's reset-incidents verb on a lane held for incomplete evidence: the operator reset that collect
// skipped, so the lane returns to use instead of being rebuilt by the next gate's alarm.
func (r *stepRunner) unhold(ctx context.Context) {
	p := r.s.Prev
	if p == nil || p.Hooks == "" {
		r.res.OK = true
		return
	}
	if res := r.callHook(ctx, p, "reset-incidents", "BENCH_OUT="+r.dir); !res.OK() {
		r.fail("reset-incidents: %s", res.Why("reset-incidents"))
		return
	}
	r.res.OK = true
}

func shellQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'" }

// checkRevision checks that the git checkout at dir is at rev (a prefix of HEAD).
func checkRevision(ctx context.Context, dir, rev string) error {
	out, err := exec.CommandContext(ctx, "git", "-C", dir, "rev-parse", "HEAD").Output()
	if err != nil {
		return fmt.Errorf("%s: not a git checkout: %v", dir, err)
	}
	if head := strings.TrimSpace(string(out)); !strings.HasPrefix(head, rev) {
		return fmt.Errorf("%s is at %s, not the pinned %s", dir, head, rev)
	}
	return nil
}

// buildBad is the bad seconds the build check tolerates.
func (s Step) buildBad() int {
	if s.BuildBad == nil {
		return DefaultBuildBadSeconds
	}
	return *s.BuildBad
}

// EvidenceManifest is the file an operator's collect hook writes in the run's operator directory to say whether the
// cell's evidence is complete. The bench reads only its "problems" list (strings): empty means complete or waived.
const EvidenceManifest = "evidence-manifest.json"

// evidenceProblems is the manifest's problems joined for one line ("" when the file is absent, unreadable or lists none).
func evidenceProblems(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	var m struct {
		Problems []string `json:"problems"`
	}
	if json.Unmarshal(b, &m) != nil || len(m.Problems) == 0 {
		return ""
	}
	out := strings.Join(m.Problems, "; ")
	if len(out) > 300 {
		out = out[:300] + "..."
	}
	return fmt.Sprintf("%d problem(s): %s", len(m.Problems), out)
}
