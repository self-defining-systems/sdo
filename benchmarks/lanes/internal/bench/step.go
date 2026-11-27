package bench

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
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
	StepVerify  = "verify"  // a held lane's subject is asked again whether the run's evidence is verified
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
	Fuzzops    string `json:"fuzzops"`            // the pinned workload checkout (workload verbs and the workload's own files only)
	Platform   string `json:"platform,omitempty"` // the host's lanes install (<staging>/lanes-<rev>): deploy/ scripts and the step binary
	Workload   string `json:"workload,omitempty"` // the workload adapter ("": fuzzops)
	// IsolationMode is how the lane is isolated on its host (two-way, one-way, one-way-ok); recorded in the row.
	IsolationMode string `json:"isolation_mode,omitempty"`
	WorkloadRev   string `json:"workload_rev,omitempty"`
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
	// ResetInPlace (build): take the golden copy as for Hold (the spec's reset_in_place), without keeping the ledger.
	ResetInPlace bool `json:"reset_in_place,omitempty"`

	ReadyFor     Seconds `json:"ready_for,omitempty"`
	ReadyTimeout Seconds `json:"ready_timeout,omitempty"`
	Cap          Seconds `json:"cap,omitempty"`
	// GateAfterCheck: the run gate within gateFreshness of the build check (0: ready_for).
	GateAfterCheck Seconds `json:"gate_after_check,omitempty"`
	Poll           Seconds `json:"poll,omitempty"` // idle polls (default 10 s)
	// BuildBad is the bad seconds the build check tolerates (nil: DefaultBuildBadSeconds); runs never use it.
	BuildBad *int `json:"build_bad,omitempty"`

	Run *RunSpec `json:"run,omitempty"`
	// VerifyEvidence (run): the subject declares verify-evidence. Window (verify): the span of Run.
	VerifyEvidence bool    `json:"verify_evidence,omitempty"`
	Window         *Window `json:"window,omitempty"`

	// The lane's cluster: the host's cluster_up and cluster_down commands for it, and whether this
	// build creates it (Create: it is gone; Recreate: delete it first) or this release deletes it.
	HostEnv       []string `json:"host_env,omitempty"` // KEY=VALUE of the host's env
	ClusterUp     string   `json:"cluster_up,omitempty"`
	ClusterDown   string   `json:"cluster_down,omitempty"`
	Create        bool     `json:"create,omitempty"`
	Recreate      bool     `json:"recreate,omitempty"`
	DeleteCluster bool     `json:"delete_cluster,omitempty"`
	ResumeDelete  bool     `json:"resume_delete,omitempty"`
	// Virtual: the host is an eks host whose lanes are vclusters (eks_vcluster.go).
	Virtual bool `json:"virtual,omitempty"`
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
	// TeardownFailed, when set, says a release that deletes the cluster could not tear the lane's app down first (the
	// cluster was deleted all the same): the reason, for the record.
	TeardownFailed string `json:"teardown_failed,omitempty"`
	// Recycle, when set, says a held lane was not claimable (for example an incident open on it): nothing was
	// swapped, the lane is recycled and the failure does not count against it.
	Recycle string `json:"recycle,omitempty"`
	// Restored, when set, says a swap failed and the lane was returned to its golden copy in place (Prepared is the
	// reset, ready operator): the failure is the claimant's, not the lane's, and counts nothing against the lane.
	Restored bool `json:"restored,omitempty"`
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
	// CollectFailed is why the collect hook failed or was killed at its limit ("" when it succeeded or was not called).
	CollectFailed string `json:"collect_failed,omitempty"`
	// Evidence is the subject's answer to verify-evidence (a run or verify step of a subject that declares it), and
	// Window the span collected.
	Evidence *Evidence `json:"evidence,omitempty"`
	Window   *Window   `json:"window,omitempty"`
	// Transcripts is a failed transcripts check after a teardown ("" when complete or not checked).
	Transcripts string `json:"transcripts,omitempty"`
	// Exit, Tail and Log describe the command that failed the step: its exit code, the last lines it wrote (at most
	// tailMaxLines lines, tailMaxBytes bytes) and the log file with the whole output. Reason carries the same.
	Exit int    `json:"exit,omitempty"`
	Tail string `json:"tail,omitempty"`
	Log  string `json:"log,omitempty"`
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
	// note is the detail every phase carries until the step ends (shapeNote: the build rebuilds its lane).
	note string
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
	// psi reads the lane's cgroup pressure (nil: the host's); iso is the run's isolation record while it is open.
	psi func() (LanePSI, bool)
	iso *Isolation
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
	// Deleting a lane's cluster needs no workload: a lane that never got a build has no checkout to ask. A verify step
	// asks only the subject.
	noWorkload := s.Kind == StepVerify || s.Kind == StepRelease && s.Fuzzops == "" && s.DeleteCluster
	if !noWorkload {
		if err = r.writeSpecFile(); err == nil {
			r.res.Fuzzops, err = r.version(ctx)
		}
	}
	switch {
	case err != nil:
		r.fail("workload identity: %v", err)
	case s.Kind != StepVerify && pinMismatch(r.res.Fuzzops, s.WorkloadRev) != "":
		r.fail("%s", pinMismatch(r.res.Fuzzops, s.WorkloadRev))
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
		case StepVerify:
			r.verify(ctx)
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

// hookInputDirName is a lane staging entry, separate from both the step's
// collected output and the scratch directory emptied during a build.
func hookInputDirName(stepID string) string { return stepID + ".hook-input" }

// specFileFor uses the call's target. Identity has no target of its own, so
// it uses New for a build or run and Prev for steps servicing the lane.
func (r *stepRunner) specFileFor(p *Prepared) string {
	if p == nil {
		p = r.s.New
		if p == nil {
			p = r.s.Prev
		}
	}
	if p == nil {
		return ""
	}
	return p.Operator
}

// writeSpecFile binds New to its own file before the first workload call.
// Prev keeps the file recorded by its build.
func (r *stepRunner) writeSpecFile() error {
	block := r.s.OperatorFile
	if block == nil {
		return nil
	}
	if r.s.New == nil {
		return errors.New("hook input has no target")
	}
	path := filepath.Join(filepath.Dir(r.s.LaneDir), hookInputDirName(r.s.ID), "operator.json")
	if err := writeJSON(path, block); err != nil {
		return err
	}
	r.s.New.Operator = path
	return nil
}

// resetLaneDir clears the lane's scratch data. Older prepared records may
// still point to operator.json here, including if this build dies before its
// result is adopted.
func (r *stepRunner) resetLaneDir() error {
	if err := os.MkdirAll(r.s.LaneDir, 0o755); err != nil {
		return err
	}
	entries, err := os.ReadDir(r.s.LaneDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.Name() != "operator.json" {
			if err := os.RemoveAll(filepath.Join(r.s.LaneDir, entry.Name())); err != nil {
				return err
			}
		}
	}
	return nil
}

// laneEnv is the environment of every fuzzops call and hook on the lane.
func (r *stepRunner) laneEnv(p *Prepared) []string {
	e := []string{"KUBECONFIG=" + r.s.Kubeconfig, "FUZZOPS_KIND_CLUSTER=" + r.s.Lane, "KIND_CLUSTER=" + r.s.Lane,
		"KUBE_CONTEXT=kind-" + r.s.Lane, "FUZZOPS_KUBE_CONTEXT=kind-" + r.s.Lane,
		"BENCH_LANE=" + r.s.Lane, "BENCH_HOST=" + r.s.Host, "BENCH_LANE_DIR=" + r.s.LaneDir, "BENCH_FRESH_FINGERPRINT=" + r.freshFingerprintFile(),
		"BENCH_EVIDENCE_HOST=" + r.s.Evidence, "BENCH_EVIDENCE_NODE=" + EvidenceNode}
	// The spec's bounds, for a workload that bounds its own waits: a run that outlives ready_timeout + cap (plus its
	// own close-out margin) is the adapter's to kill, whatever wait it is stuck in (docs/contract.md).
	if r.s.Cap > 0 {
		e = append(e, "BENCH_CAP_SECONDS="+strconv.FormatInt(int64(r.s.Cap.D().Seconds()), 10))
	}
	if r.s.ReadyTimeout > 0 {
		e = append(e, "BENCH_READY_TIMEOUT_SECONDS="+strconv.FormatInt(int64(r.s.ReadyTimeout.D().Seconds()), 10))
	}
	e = append(e, r.s.HostEnv...)
	if r.s.Provider == ProviderEks {
		if r.s.Virtual {
			e = append(e, "BENCH_LANE_VIRTUAL=1") // kube-system and the nodes are the host cluster's, not the lane's
		}
		// The platform script that registers the images the lane will run in the host's pre-pull list (`prepull
		// KUBECONFIG IMAGE...`): the host cluster's list for a virtual lane, the lane's own cluster's otherwise.
		prepull := "eks-lane.sh"
		if r.s.Virtual {
			prepull = "eks-vcluster.sh"
		}
		e = append(e, "BENCH_PREPULL="+filepath.Join(r.platform(), "deploy", prepull))
		// The lane's entry address (a node the manager reaches on NodePorts) is known once its cluster is up.
		if b, err := os.ReadFile(r.s.Kubeconfig + ".entry"); err == nil && strings.TrimSpace(string(b)) != "" {
			e = append(e, "BENCH_ENTRY_HOST="+strings.TrimSpace(string(b)))
		}
	}
	if p != nil {
		if p.EvidenceRun != "" {
			e = append(e, "BENCH_EVIDENCE_RUN="+p.EvidenceRun)
		}
		e = append(e, "BENCH_EXPERIMENT="+p.Experiment, "FUZZOPS_NAMESPACE="+p.Namespace, "BENCH_PACK="+p.AppPack)
		// The lanes install's host registry script (deploy/registry.sh): `push IMAGE...` puts an image where
		// every lane cluster of the host pulls it from.
		registry := "registry.sh"
		if r.s.Provider == ProviderEks {
			registry = "eks-lane.sh" // `push IMAGE...` goes to ECR and the nodes' mirror
		}
		e = append(e, "BENCH_REGISTRY="+filepath.Join(r.platform(), "deploy", registry))
	}
	if f := r.specFileFor(p); f != "" {
		e = append(e, "BENCH_OPERATOR="+f, "BENCH_OPERATOR_BLOCK=1")
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
// exit status and why the end of its stderr. It is the only place a step builds a workload call, and the call's env is
// always laneEnv(p) (the host's env with the workload's workload_env, and the lane's facts) with extra on top: a step
// cannot hand a verb a bare env, so a verb added to a step gets the workload's configuration without asking. p is the
// state of the lane the call is about (nil before the step has one).
func (r *stepRunner) call(ctx context.Context, p *Prepared, extra []string, out io.Writer, verb, action string, args ...string) (int, string, error) {
	env := append(r.laneEnv(p), extra...)
	return r.workload().Call(ctx, WorkloadCall{Verb: verb, Action: action, Args: args, Env: env, Stdout: out, Log: r.log})
}

// workload is the adapter of the step's spec (the built-in fuzzops one by default).
func (r *stepRunner) workload() Workload {
	if r.wl != nil {
		return r.wl
	}
	return NewWorkload(r.s.Workload, r.s.Fuzzops)
}

// Failure detail caps: a failed command's reason carries its exit code and the end of its output, bounded.
const (
	tailMaxLines = 40
	tailMaxBytes = 4096
)

// tailWriter keeps the end of what is written to it (at most tailMaxLines lines and tailMaxBytes bytes): the reason a
// command gave.
type tailWriter struct{ b []byte }

func (t *tailWriter) Write(p []byte) (int, error) {
	t.b = append(t.b, p...)
	if len(t.b) > 4*tailMaxBytes {
		t.b = t.b[len(t.b)-4*tailMaxBytes:]
	}
	return len(p), nil
}

func (t *tailWriter) String() string { return tailText(string(t.b)) }

// tailText is the last tailMaxLines non-empty lines of s, at most tailMaxBytes bytes (cut at a line start when it can).
func tailText(s string) string {
	var lines []string
	for _, l := range strings.Split(s, "\n") {
		if l = strings.TrimRight(l, " \t\r"); strings.TrimSpace(l) != "" {
			lines = append(lines, l)
		}
	}
	if len(lines) > tailMaxLines {
		lines = lines[len(lines)-tailMaxLines:]
	}
	out := strings.Join(lines, "\n")
	if len(out) > tailMaxBytes {
		out = out[len(out)-tailMaxBytes:]
		if i := strings.IndexByte(out, '\n'); i >= 0 && i < len(out)-1 {
			out = out[i+1:]
		}
	}
	return out
}

// oneLine joins a tail's lines for a one-line reason.
func oneLine(tail string) string { return strings.ReplaceAll(strings.TrimSpace(tail), "\n", " | ") }

// failText is "exit N", the error when there is one, and the end of the command's output.
func failText(code int, tail string, err error) string {
	out := fmt.Sprintf("exit %d", code)
	if err != nil {
		out += " " + err.Error()
	}
	if t := oneLine(lastLines(tail, reasonLines)); t != "" {
		out += ": " + t
	}
	return out
}

// reasonLines is how many of the last output lines the one-line reason of a failed call carries: a status line shows
// the head of the reason, and the cause is at the end of the output (a traceback's exception, a command's last
// error), after any preamble (git's detached-HEAD note was all `lane status` showed of a failed `app up`). The whole
// tail stays in the step result and the log.
const reasonLines = 3

// lastLines is the last n non-empty lines of s.
func lastLines(s string, n int) string {
	var lines []string
	for _, l := range strings.Split(s, "\n") {
		if strings.TrimSpace(l) != "" {
			lines = append(lines, l)
		}
	}
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

// failCall fails the step for a workload call that exited non-zero: the reason, the step result (Exit, Tail, Log) and
// so the lane status and plan status carry the exit code and the end of the output.
func (r *stepRunner) failCall(what string, code int, tail string, err error) {
	r.res.Exit, r.res.Tail, r.res.Log = code, tail, filepath.Join(r.dir, "runner.log")
	r.fail("%s: %s", what, failText(code, tail, err))
}

// failHook is failCall for a hook verb.
func (r *stepRunner) failHook(res HookResult, verb string) {
	r.res.Exit, r.res.Tail, r.res.Log = res.Exit, tailText(res.Output), filepath.Join(r.dir, "hooks.log")
	r.fail("%s", res.Why(verb))
}

func (r *stepRunner) version(ctx context.Context) (Version, error) {
	var out bytes.Buffer
	code, _, err := r.call(ctx, nil, nil, &out, VerbIdentity, "")
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
		r.res.Exit, r.res.Tail, r.res.Log = res.Exit, tailText(res.Output), filepath.Join(r.dir, "hooks.log")
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

// BenchIdleForEnv tells the idle hook why it is asked when that is not a gate before an inject: IdleForTeardown says
// what it answers for is about to be deleted, so the hook asks only whether work has stopped, not for what an inject
// needs. Unset is the pre-inject gate and the build check, as before.
const (
	BenchIdleForEnv = "BENCH_IDLE_FOR"
	IdleForTeardown = "teardown"
)

// waitIdle polls the operator's idle until it is idle twice in a row, at most for the cap.
func (r *stepRunner) waitIdle(ctx context.Context, p *Prepared) bool {
	start := time.Now()
	limit := r.s.Cap.D()
	if limit <= 0 {
		limit = 30 * time.Minute
	}
	calm, why := 0, ""
	progress := start
	for calm < 2 && time.Since(start) < limit {
		if res := r.callHook(ctx, p, "idle", BenchIdleForEnv+"="+IdleForTeardown); res.OK() {
			calm++
		} else {
			calm, why = 0, res.First
			if strings.HasPrefix(why, "gone:") {
				// The subject says it is already gone: there is nothing to wait for, and the teardown goes on.
				r.logf("idle: %s", why)
				r.interval("idle", start, true, why)
				return true
			}
			if r.clusterVanished(ctx, "the operator to be idle") {
				return false
			}
		}
		if calm < 2 && time.Since(progress) >= idleProgressEvery {
			progress = time.Now()
			r.logf("idle: still waiting after %s of at most %s: %s", time.Since(start).Round(time.Second), limit, why)
		}
		if calm < 2 {
			time.Sleep(r.idlePause(calm))
		}
	}
	if calm < 2 {
		r.logf("idle: gave up after %s (the cap): %s; the teardown goes on", time.Since(start).Round(time.Second), why)
	}
	r.interval("idle", start, calm >= 2, why)
	return true
}

// idleProgressEvery is how often a teardown's idle wait says what it still waits for.
var idleProgressEvery = 5 * time.Minute

// packLoads is the build's pre-flight: the pinned fuzzops loads the spec's pack before anything is made or torn
// down, so a pack that is not there (the spec's app is a path inside the pinned checkout) or does not parse fails
// the step in seconds, naming the pack, instead of at a later command that retries. A fuzzops that cannot say
// (it predates `pack load`, or does not run) is not an objection: the build goes on as before.
func (r *stepRunner) packLoads(ctx context.Context, p *Prepared) bool {
	if p == nil || p.AppPack == "" {
		return true
	}
	code, why, err := r.call(ctx, p, nil, io.Discard, VerbAppUp, "check-pack", "--pack", p.AppPack)
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
		code, why, err = r.call(ctx, p, nil, nil, VerbAppReset, "recover", "--namespace", p.Namespace, "--pack", p.AppPack)
		if errors.Is(err, ErrUnsupported) {
			// The workload leaves nothing to undo between runs (docs/contract.md: unsupported is skipped).
			r.interval("recover", start, true, "unsupported by the workload")
			return true
		}
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
	r.interval("recover", start, ok, failText(code, why, err))
	if !ok && r.s.ClusterUp != "" && r.clusterMissing(ctx) {
		// No cluster, so no fault in place: a build killed before its cluster was made leaves nothing to undo.
		r.logf("recover: cluster %s does not exist, nothing to undo (fuzzops recover in %s exit %d)", r.s.Lane, p.Namespace, code)
		return true
	}
	if !ok {
		r.res.Exit, r.res.Tail, r.res.Log = code, why, filepath.Join(r.dir, "runner.log")
		r.down("fault still active: fuzzops recover in %s %s", p.Namespace, failText(code, why, err))
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
	if !s.Create && !s.Recreate && r.shapeChanged(ctx) {
		// The lane's cores and memory are part of its identity: the cluster is made anew with the new shape, never
		// reused (or swapped into) with the old one.
		r.s.Recreate, r.s.Swap, r.s.ReuseApp = true, false, false
		s = r.s
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
		if err := r.resetLaneDir(); err != nil {
			r.fail("lane dir: %v", err)
			return
		}
		pre.EvidenceRun = s.Experiment + "-" + s.Lane + "-" + s.ID
		// From here the operator may be (partly) on the lane: the next build tears it down.
		r.res.Prepared = &pre
		out := filepath.Join(r.dir, "prepare")
		_ = os.MkdirAll(out, 0o755)
		res := r.callHook(ctx, &pre, "prepare", "BENCH_OUT="+out)
		if !res.OK() {
			r.failHook(res, "prepare")
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
	r.swapInto(ctx)
	if !r.res.OK && r.res.LaneDown == "" && r.res.Config == "" && r.res.Recycle == "" && r.res.Prepared != nil {
		r.restoreAfterFailedSwap(ctx)
	}
}

// restoreAfterFailedSwap returns the lane to its golden copy in place after a swap failed past the point where the
// claimant's sources reached it: the hooks' reset (app, accepted repository, controller), then the same fresh-app
// check a snapshot is taken under. The claim keeps its failure reason; the lane goes back to the pool ready, with no
// teardown and no new cluster. If the restore itself fails, the result stays as it was and the release path deals
// with the lane (a reset again, else a teardown).
func (r *stepRunner) restoreAfterFailedSwap(ctx context.Context) {
	cur := *r.res.Prepared
	prev := r.s.Prev
	if !cur.Golden || prev == nil {
		return
	}
	reason := r.res.Reason
	if !r.resetLane(ctx, &cur) {
		return
	}
	if why := r.appMatchesFresh(ctx, &cur); why != "" {
		r.logf("pool lane not restored after a failed swap: %s", why)
		return
	}
	kept := *prev
	kept.Reset, kept.Ready, kept.Golden = true, true, true
	r.res.Prepared, r.res.Restored = &kept, true
	r.res.Reason = reason
	r.logf("pool lane restored to its golden copy after a failed swap: %s", reason)
}

func (r *stepRunner) swapInto(ctx context.Context) {
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
		r.failHook(res, "swap")
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
	start := time.Now()
	r.phase(PhaseApp, "")
	if q := s.Prev; q != nil && (q.Namespace != p.Namespace || q.AppPack != p.AppPack) {
		if code, why, err := r.call(ctx, q, nil, nil, VerbAppUp, "down", "--pack", q.AppPack, "--namespace", q.Namespace); err != nil || code != 0 {
			r.interval("app", start, false, "down of the last app copy: "+failText(code, why, err))
			r.failCall("app down "+q.Namespace, code, why, err)
			return false
		}
	}
	for _, verb := range []string{"down", "up"} {
		if code, why, err := r.call(ctx, p, nil, nil, VerbAppUp, verb, "--pack", p.AppPack, "--namespace", p.Namespace); err != nil || code != 0 {
			r.interval("app", start, false, "app "+verb+": "+failText(code, why, err))
			r.failCall("app "+verb, code, why, err)
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
	return filepath.Join(filepath.Dir(r.s.LaneDir), freshFingerprintName)
}

// freshFingerprintName is the fingerprint's file in the lane's staging directory; the staging gc keeps it by this name.
const freshFingerprintName = "fresh-fingerprint.json"

// freshCopyDetail marks the app interval of a build that put down a fresh copy.
const freshCopyDetail = "fresh copy"

// recordFresh takes the fingerprint of a copy that was just put down (`fuzzops app fingerprint`; a few tries while
// the stores settle).
func (r *stepRunner) recordFresh(ctx context.Context, p *Prepared) bool {
	file := r.freshFingerprintFile()
	_ = os.Remove(file)
	var code int
	var why string
	var err error
	for try := 0; try < 3; try++ {
		if code, why, err = r.call(ctx, p, nil, io.Discard, VerbAppSnapshot, "write", "--pack", p.AppPack, "--namespace", p.Namespace, "--out", file); err == nil && code == 0 {
			return true
		}
		if errors.Is(err, ErrUnsupported) {
			// No fingerprint: a later reuse finds none and rebuilds the app copy (appMatchesFresh).
			r.logf("app-snapshot write unsupported by the workload: no fingerprint of the fresh copy")
			return true
		}
		time.Sleep(r.poll)
	}
	r.failCall("fingerprint of the fresh app copy", code, why, err)
	return false
}

// appMatchesFresh says why the lane's current app copy cannot be shown to hold a fresh deploy's data ("" when it
// can): the fingerprint differs or there is no fresh one to compare with.
func (r *stepRunner) appMatchesFresh(ctx context.Context, p *Prepared) string {
	file := r.freshFingerprintFile()
	if _, err := os.Stat(file); err != nil {
		return "no fingerprint of a fresh copy on this lane"
	}
	code, why, err := r.call(ctx, p, nil, io.Discard, VerbAppSnapshot, "verify", "--pack", p.AppPack, "--namespace", p.Namespace, "--against", file)
	if err != nil || code != 0 {
		return fmt.Sprintf("fingerprint check %s (see the runner log)", failText(code, why, err))
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
		code, why, err := r.call(ctx, p, nil, nil, VerbGate, "check", "--pack", p.AppPack, "--namespace", p.Namespace,
			"--duration", r.s.ReadyFor.D().String(), "--clean", "--max-bad-seconds", fmt.Sprint(r.s.buildBad()))
		if err == nil && code == 0 {
			r.interval("app-health", start, true, fmt.Sprintf("clean on try %d", try))
			return true
		}
		r.logf("app check (no operator) try %d: %s", try, failText(code, why, err))
		if r.clusterVanished(ctx, "the app to be healthy") {
			return false
		}
		if time.Since(start)+r.s.ReadyFor.D() > r.s.ReadyTimeout.D() {
			r.interval("app-health", start, false, failText(code, why, err))
			if final {
				r.res.Exit, r.res.Tail, r.res.Log = code, why, filepath.Join(r.dir, "runner.log")
				r.fail("app not clean for %s within %s before the operator was restored: %s", r.s.ReadyFor.D(), r.s.ReadyTimeout.D(), failText(code, why, err))
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

// platform is the lanes install on this host, where every platform script is run from. A step the manager started
// carries it; one built without it (a test) installs the scripts under its own directory.
func (r *stepRunner) platform() string {
	if r.s.Platform != "" {
		return r.s.Platform
	}
	dir, err := InstallPlatformScripts(filepath.Join(r.dir, "platform"))
	if err != nil {
		r.logf("lanes platform install: %v", err)
		return filepath.Join(r.dir, "no-platform")
	}
	r.s.Platform = dir
	return dir
}

// shellWaitDelay is how long a killed host command may keep the step waiting for its output to close.
var shellWaitDelay = 10 * time.Second

// shell runs a host-configured command (cluster_up, cluster_down) as the workload/operator command it is: in the pinned
// workload checkout, with LANES_INSTALL (the platform install) and BENCH_WORKLOAD_DIR (the checkout) in its environment.
// The platform's own default commands name their scripts through $LANES_INSTALL, so none depends on the directory.
func (r *stepRunner) shell(ctx context.Context, kind, cmdline string, env []string) bool {
	start := time.Now()
	r.phase(kind, "")
	ctx, cancel := context.WithTimeout(ctx, 45*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "sh", "-c", cmdline)
	cmd.Dir = r.s.Fuzzops
	cmd.Env = append(append(os.Environ(), r.s.HostEnv...), env...)
	cmd.Env = append(cmd.Env, "LANES_INSTALL="+r.platform(), "BENCH_WORKLOAD_DIR="+r.s.Fuzzops)
	var tail tailWriter
	w := io.MultiWriter(r.log, &tail)
	cmd.Stdout, cmd.Stderr = w, w
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = shellWaitDelay // a descendant outside the group that holds the output open must not outlive the limit
	r.logf("%s: %s", kind, cmdline)
	err := cmd.Run()
	code := 0
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		code = ee.ExitCode()
	} else if err != nil {
		code = -1
	}
	if err != nil {
		r.interval(kind, start, false, failText(code, tail.String(), err))
		r.failCall(kind, code, tail.String(), err)
	} else {
		r.interval(kind, start, true, "")
	}
	return err == nil
}

// makeClusterUp takes the lane's cores (when the host limits lanes) and runs the host's cluster_up with them.
func (r *stepRunner) makeClusterUp(ctx context.Context) bool {
	env, ok := r.laneEnvLimits(ctx)
	if !ok {
		return false
	}
	if err := r.resetLaneDir(); err != nil {
		r.fail("lane dir: %v", err)
		return false
	}
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
	code, why, err := r.call(ctx, p, nil, nil, VerbAppUp, "images", "--pack", p.AppPack, "--cluster", s.Lane)
	if errors.Is(err, ErrUnsupported) {
		r.interval("images", start, true, "unsupported by the workload")
		return true
	}
	r.interval("images", start, err == nil && code == 0, failText(code, why, err))
	if err != nil || code != 0 {
		r.failCall("app images", code, why, err)
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
	if len(extra) == 0 {
		return nil, nil
	}
	// A workload that installs its own services at every row start and removes them at the next one (its reset) leaves
	// those objects at the end of every row: it names them (`app-reset owned`), and a fault's object is none of them.
	owned, err := r.workloadOwned(ctx, p)
	if err != nil {
		return nil, err
	}
	var left []string
	for _, o := range extra {
		if !owned[o] {
			left = append(left, o)
		}
	}
	return left, nil
}

// workloadOwned is the cluster-scoped objects (kind/name, as `kubectl get -o name` prints them) the workload's own
// row-start reset removes and its row recreates (docs/contract.md, `app-reset owned`). A workload that does not
// implement the verb owns none.
func (r *stepRunner) workloadOwned(ctx context.Context, p *Prepared) (map[string]bool, error) {
	var out bytes.Buffer
	code, tail, err := r.call(ctx, p, nil, &out, VerbAppReset, "owned", "--namespace", p.Namespace)
	if errors.Is(err, ErrUnsupported) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var answer struct {
		Objects []string `json:"objects"`
	}
	if code != 0 {
		return nil, fmt.Errorf("app-reset owned exit %d: %s", code, tail)
	}
	if err := json.Unmarshal([]byte(lastNonEmpty(out.Bytes())), &answer); err != nil {
		return nil, fmt.Errorf("app-reset owned: last line is not JSON: %q", lastNonEmpty(out.Bytes()))
	}
	owned := map[string]bool{}
	for _, o := range answer.Objects {
		owned[o] = true
	}
	return owned, nil
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
	idleAfter := time.Since(start).Round(time.Second)
	for try := 1; ; try++ {
		spanStart := time.Now()
		reasons, alarmed := r.checkOnce(ctx, p)
		alarm = alarm || alarmed
		if len(reasons) == 0 {
			r.logf("build check phases: hooks idle after %s, clean journeys span %s", idleAfter, time.Since(spanStart).Round(time.Second))
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
		why  string
		err  error
	}
	done := make(chan out, 1)
	go func() {
		code, why, err := r.call(ctx, p, nil, nil, VerbGate, "check", "--pack", p.Pack, "--namespace", p.Namespace,
			"--duration", r.s.ReadyFor.D().String(), "--clean", "--max-bad-seconds", fmt.Sprint(r.s.buildBad()),
			"--ledger-out", filepath.Join(r.s.LaneDir, ledgerFile),
			"--min-confirmed", fmt.Sprint(ledgerMin+ledgerMargin), "--max-duration", (3 * r.s.ReadyFor.D()).String())
		done <- out{code, why, err}
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
		reasons = append(reasons, fmt.Sprintf("journeys not clean (fuzzops check %s)", failText(o.code, o.why, o.err)))
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
	env := append([]string{}, "FUZZOPS_EXEC_OUT="+filepath.Join(r.dir, "exec"), "BENCH_RUN="+rs.ID, "BENCH_FAULT="+faultArg(rs.Fault))
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
	faultArgs, err := rs.faultFlags()
	if err != nil {
		r.fail("select fault: %v", err)
		return
	}
	args = append(args, faultArgs...)
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
	code, why, err := r.call(ctx, p, env, f, VerbRun, "run", args...)
	r.log = plain
	f.Close()
	var rec record
	rerr := readJSON(recPath, &rec)
	switch {
	case err != nil || (code != 0 && code != 1) || rerr != nil:
		r.res.Status = StatusInvalid
		r.res.Exit, r.res.Tail, r.res.Log = code, why, filepath.Join(r.dir, "runner.log")
		r.fail("lane lost: fuzzops run %s %s", failText(code, why, err), errOrEmpty(rerr))
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
		r.recordPSIAtInject()
		r.settle(ctx, p, at, ownDetected(rec))
	}
	if p.Hooks != "" {
		out := filepath.Join(r.dir, "operator")
		_ = os.MkdirAll(out, 0o755)
		end := time.Now().UTC()
		if r.res.SettleEnd != nil {
			end = *r.res.SettleEnd
		}
		w := Window{from, end}
		res := r.callHook(ctx, p, "collect", append(w.env(), "BENCH_OUT="+out, "BENCH_RUN="+rs.ID, "BENCH_FAULT="+faultArg(rs.Fault))...)
		if !res.OK() {
			r.res.CollectFailed = clip(res.Why("collect"), 300)
			r.logf("collect failed (recorded, the run stands): %s", res.Why("collect"))
		}
		if s.VerifyEvidence {
			r.res.Window, r.res.Evidence = &w, r.verifyEvidence(ctx, p, r.dir, rs, w)
		} else {
			r.res.EvidenceIncomplete = evidenceProblems(filepath.Join(out, EvidenceManifest))
		}
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
// settleRecheck is the pause between the two calm answers that end a settle or a teardown's idle wait.
const settleRecheck = 2 * time.Second

// idlePause is how long to wait before asking idle again, after calm calm answers in a row: one calm answer only
// has to be confirmed after a short pause, a busy one is polled at the step's poll.
func (r *stepRunner) idlePause(calm int) time.Duration {
	if calm == 1 {
		return min(r.poll, settleRecheck)
	}
	return r.poll
}

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
		time.Sleep(r.idlePause(calm))
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
	active := func(verb string) (bool, error) {
		var out bytes.Buffer
		code, _, err := r.call(ctx, p, nil, &out, VerbRun, verb, "--file", rs.Instances, "--id", rs.Fault, "--pack", p.AppPack, "--namespace", p.Namespace)
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
	missing := r.distrustPrepared(ctx)
	if missing && !r.s.ResumeDelete {
		// The cluster is gone, and with it the operator: there is nothing to tear down or reset.
		r.res.OK, r.res.Prepared, r.res.Gone = true, nil, r.s.ClusterUp != ""
		r.freeLaneCores()
		return
	}
	if r.s.DeleteCluster {
		// The cluster is deleted whether or not the teardown hook worked: a broken cluster (its API or nodes
		// failing) is the one that most needs deleting, and a release that stopped at a failed teardown left it billing
		// with the lane Down and, after two resets, Retired and forgotten. What the teardown could not do is recorded.
		if !missing && !r.teardown(ctx, r.s.Prev) {
			r.logf("teardown failed (%s): the cluster is deleted all the same", r.res.Reason)
			r.res.TeardownFailed = r.res.Reason
			r.res.OK, r.res.Reason, r.res.LaneDown = true, "", ""
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
	if !(r.s.Hold || r.s.ResetInPlace) || p.Hooks == "" || p.Golden {
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
	// A pool's claimants start from its build check's writes, which the app's golden copy restores with the stores it
	// declares. A lane reset in place between experiments keeps none: the reset leaves no ledger, so the next swap
	// runs the full build check and each row verifies writes confirmed on its own lane state, as a rebuilt lane does.
	if err := copyFile(filepath.Join(r.s.LaneDir, ledgerFile), filepath.Join(r.s.LaneDir, ledgerGolden), 0o644); err != nil || !r.s.Hold {
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

// unhold gives the subject a chance to reset incidents after a hold for incomplete evidence.
// A subject without this optional verb has nothing to reset.
func (r *stepRunner) unhold(ctx context.Context) {
	p := r.s.Prev
	if p == nil || p.Hooks == "" {
		r.res.OK = true
		return
	}
	if res := r.callHook(ctx, p, verbUnhold, "BENCH_OUT="+r.dir); !res.OK() && !res.Unsupported() {
		r.failHook(res, verbUnhold)
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
