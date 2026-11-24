package bench

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// The evidence rule (docs/contract.md): a lane is never reset, released, rebuilt or torn down for a run whose evidence
// its subject has not verified. The platform knows one thing per run, "verified" or the reasons why not; what evidence
// is stays behind the subject's verify-evidence verb. A subject binds itself to the rule by declaring the verb
// (describe): then only a readable "verified" frees the lane, and anything else holds it, however the run ended. The
// hold is in the lane file, so a restart keeps it, and start refuses every step that would erase the lane, whoever asks.

// verbVerifyEvidence answers {"verified":bool,"reasons":[...]} for the run whose directory is BENCH_RUN_DIR.
const verbVerifyEvidence = "verify-evidence"

// Evidence is what is known of a run's evidence.
type Evidence struct {
	Verified bool     `json:"verified"`
	Reasons  []string `json:"reasons,omitempty"`
}

// missing says why the evidence is not verified ("" when it is).
func (e *Evidence) missing() string {
	switch {
	case e == nil:
		return "not verified: the step left no answer"
	case len(e.Reasons) > 0:
		return clip(strings.Join(e.Reasons, "; "), 400)
	case !e.Verified:
		return "not verified: no reason given"
	}
	return ""
}

// readEvidence is the hook's answer. Only a hook that ended well and said so in words that can be read has verified.
func readEvidence(r HookResult) *Evidence {
	var a struct {
		Verified *bool    `json:"verified"`
		Reasons  []string `json:"reasons"`
	}
	switch {
	case r.Unsupported():
		return &Evidence{Reasons: []string{"the subject declares " + verbVerifyEvidence + " and does not implement it"}}
	case !r.OK():
		return &Evidence{Reasons: []string{r.Why(verbVerifyEvidence)}}
	case json.Unmarshal([]byte(r.Last), &a) != nil || a.Verified == nil:
		return &Evidence{Reasons: []string{fmt.Sprintf("%s: unreadable answer %q", verbVerifyEvidence, clip(r.Last, 80))}}
	case !*a.Verified && len(a.Reasons) == 0:
		a.Reasons = []string{"not verified: no reason given"}
	}
	return &Evidence{Verified: *a.Verified && len(a.Reasons) == 0, Reasons: a.Reasons}
}

// Window is the span of a run whose evidence is collected.
type Window struct{ From, To time.Time }

func (w Window) env() []string {
	return []string{"BENCH_INTERVAL_START=" + w.From.UTC().Format(time.RFC3339), "BENCH_INTERVAL_END=" + w.To.UTC().Format(time.RFC3339)}
}

func (r *stepRunner) verifyEvidence(ctx context.Context, p *Prepared, dir string, rs *RunSpec, w Window, extra ...string) *Evidence {
	env := append([]string{"BENCH_RUN_DIR=" + dir, "BENCH_RUN=" + rs.ID, "BENCH_FAULT=" + faultArg(rs.Fault)}, w.env()...)
	return readEvidence(r.callHook(ctx, p, verbVerifyEvidence, append(env, extra...)...))
}

// verify asks the subject of a held lane again, about a copy of the run's directory (run/ in the step's): the subject
// may collect again into it, and only what it then verifies goes home.
func (r *stepRunner) verify(ctx context.Context) {
	var w Window
	if r.s.Window != nil {
		w = *r.s.Window
	}
	r.res.Evidence = r.verifyEvidence(ctx, r.s.Prev, filepath.Join(r.dir, "run"), r.s.Run, w, "BENCH_EVIDENCE_RECHECK=1")
	r.res.OK = true
}

// Hold is why a lane is kept as it is: the run whose evidence its subject has not verified.
type Hold struct {
	Experiment string    `json:"experiment"`
	Run        *RunSpec  `json:"run"`
	Dir        string    `json:"dir"` // the run's directory
	Window     Window    `json:"window"`
	Prepared   *Prepared `json:"prepared"`
	// Step is a dead step whose files are not home yet: they are salvaged before anything is checked.
	Step     string    `json:"step,omitempty"`
	Evidence *Evidence `json:"evidence,omitempty"`
	Checks   int       `json:"checks,omitempty"`
	Checked  time.Time `json:"checked,omitempty"`
}

// owes says why the hold stands ("" when there is none, or the evidence is verified).
func (h *Hold) owes() string {
	switch {
	case h == nil:
		return ""
	case h.Step != "":
		return "the files of step " + h.Step + " are not home"
	}
	return h.Evidence.missing()
}

// wait is how long after its last check a held lane is checked again.
func (h *Hold) wait() time.Duration { return min(time.Minute<<min(h.Checks, 4), 15*time.Minute) }

func (h *Hold) reason() string {
	return fmt.Sprintf("evidence of %s not verified: %s (checked again %d times; way out: it is checked again at least every %s, or waive there what cannot be had; to give the run up: lanes remove %s --teardown)",
		h.Run.ID, h.owes(), h.Checks, 15*time.Minute, h.Experiment)
}

// declaresEvidence: the experiment's subject declares verify-evidence.
func (x *Experiment) declaresEvidence() bool {
	return x != nil && x.Lock.Spec.Described != nil && x.Lock.Spec.Described.VerifyEvidence
}

// givenUp: a person gave the experiment's runs up by name; their evidence goes with them.
func (x *Experiment) givenUp() bool { return x != nil && x.Removed != nil && x.Removed.GiveUp }

// hold keeps lane l for run step ref of x. dir is the run's directory; dead says the step's files are still on the host.
func (m *Manager) hold(l *LaneFile, x *Experiment, ref *StepRef, dir string, w Window, ev *Evidence, dead bool) {
	l.Hold = &Hold{Experiment: x.Name, Run: ref.Run, Dir: dir, Window: w, Prepared: l.Prepared, Evidence: ev, Checked: m.now().UTC()}
	if dead {
		l.Hold.Step = ref.ID
	}
	if why := l.Hold.owes(); why != "" {
		m.raiseLaneAlarm("evidence-hold", l, why)
		m.setState(l, LaneHeld, l.Hold.reason())
		return
	}
	m.setState(l, LaneHeld, "evidence verified; the reset after its run is still to be done")
}

// holdDead holds the lane of a run step that ended without a result (dir "": its run is not recorded); it says whether
// the subject binds it to.
func (m *Manager) holdDead(l *LaneFile, x *Experiment, ref *StepRef, dir string) bool {
	if ref.Kind != StepRun || ref.Run == nil || !x.declaresEvidence() || x.givenUp() {
		return false
	}
	if dir == "" {
		dir = m.lost(l, x, ref, "lost with its lane")
	}
	m.hold(l, x, ref, dir, Window{ref.Started, m.now().UTC()}, &Evidence{Reasons: []string{"the run's step died before it was checked"}}, true)
	return true
}

// lost is the directory of a run that is not recorded among x's (its item runs again under the same ID), with the
// run.json a recorded one has.
func (m *Manager) lost(l *LaneFile, x *Experiment, ref *StepRef, why string) string {
	dir := m.Root.path(x.Name, "lost", ref.ID)
	res := Result{Started: ref.Started, Ended: m.now().UTC()}
	if err := writeJSON(filepath.Join(dir, "run.json"), m.runFile(x, l, ref.Run, res, StatusInvalid, why)); err != nil {
		m.logf("%s: lost run %s: %v", x.Name, ref.Run.ID, err)
	}
	return dir
}

// salvage brings the files of the hold's dead step home, then removes them from the host.
func (m *Manager) salvage(ctx context.Context, l *LaneFile) {
	h, ops := l.Hold, m.ops(l.Host)
	if h.Step == "" || l.Orphan != nil {
		return
	}
	if err := ops.Salvage(ctx, l.Lane, h.Step, h.Dir); err != nil {
		m.logf("lane %s: salvage %s: %v (tried again)", l.Lane, h.Step, err)
		return
	}
	if err := ops.Clean(ctx, l.Lane, h.Step); err != nil {
		m.logf("lane %s: clean %s: %v (staging left for gc)", l.Lane, h.Step, err)
	}
	h.Step = ""
}

// checkHold keeps a lane that owes evidence out of use, whatever state a step left it in, and asks its subject again
// when it is time. It says whether the lane is still held.
func (m *Manager) checkHold(ctx context.Context, l *LaneFile) bool {
	h := l.Hold
	if h == nil {
		return false
	}
	if m.exps[h.Experiment].givenUp() && h.owes() != "" {
		m.logf("lane %s: run %s was given up by name, its evidence (%s) with it", l.Lane, h.Run.ID, h.owes())
		h.Step, h.Evidence = "", &Evidence{Verified: true}
	}
	m.salvage(ctx, l)
	if h.owes() == "" {
		if l.State != LaneHeld { // down, or gone from hosts.yaml: nothing to resume
			m.logf("lane %s: evidence verified (lane is %s)", l.Lane, l.State)
			l.Hold = nil
		}
		return false
	}
	if l.State == LaneFree || l.State == LaneReady || l.State == LaneHeld && l.Reason != h.reason() {
		m.setState(l, LaneHeld, h.reason())
	}
	if h.Step == "" && l.Orphan == nil && m.hosts[l.Host].Error == "" && m.now().Sub(h.Checked) >= h.wait() {
		h.Checks, h.Checked = h.Checks+1, m.now().UTC()
		if !m.start(ctx, l, Step{Kind: StepVerify, Prev: h.Prepared, Run: h.Run, Window: &h.Window}) {
			h.Evidence = &Evidence{Reasons: []string{"the check did not start (see bench.log)"}}
		}
	}
	return true
}

// checked takes the answer of a finished check (its files are in dir).
func (m *Manager) checked(l *LaneFile, res Result, dir string) {
	run := filepath.Join(dir, "run")
	if l.Hold != nil {
		if l.Hold.Evidence = res.Evidence; res.Evidence == nil {
			l.Hold.Evidence = &Evidence{Reasons: []string{"the check left no answer: " + res.Reason}}
		}
		if l.Hold.owes() == "" {
			if err := overlay(run, l.Hold.Dir); err != nil {
				l.Hold.Evidence = &Evidence{Reasons: []string{"verified, not home: " + err.Error()}}
			}
		}
	}
	_ = os.RemoveAll(run)
}

// overlay moves every file of src over dst; what dst has beyond them stays.
func overlay(src, dst string) error {
	return filepath.WalkDir(src, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, p)
		if d.IsDir() {
			return os.MkdirAll(filepath.Join(dst, rel), 0o755)
		}
		return os.Rename(p, filepath.Join(dst, rel))
	})
}

// erases: a step of this kind changes or deletes what the lane's cluster still holds of a run's evidence. Recover only
// undoes a fault.
func erases(kind string) bool { return kind == StepBuild || kind == StepRun || kind == StepRelease }
