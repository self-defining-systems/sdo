package bench

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A lane is never reset, released, rebuilt or torn down for a run whose evidence the subject has not verified. A collect
// hook that was killed at its limit left nothing; the platform read that as "nothing is wrong", reset the lane to its
// golden copy and the store that held the run's evidence was gone. The tests drive whole manager passes against a host
// that is not real and read what is on disk, so they say nothing of how the hold is kept.

const (
	evRun    = "run-1"
	evVerify = "verify" // the step that asks a held lane's subject again
)

// evWorld is a host whose one lane has a step; the test says how it ended. No process, clock or cluster is real.
type evWorld struct {
	HostOps
	state    map[string]string // step ID -> its state
	res      map[string]any    // the run step's result.json
	check    map[string]any    // what a later check answers (the "evidence" of its result.json)
	checks   string            // how a later check ends (StepFinished)
	files    map[string]string // what the run step's directory holds besides result.json
	away     bool              // the host does not answer a salvage
	started  []Step
	salvaged []string
	cleaned  []string
	put      []string // the directories sent to the host
}

func (w *evWorld) Probe(_ context.Context, steps map[string]string, _ []string) (Probe, error) {
	p := Probe{Steps: map[string]string{}}
	for lane, id := range steps {
		p.Steps[lane] = w.state[id]
	}
	return p, nil
}

func (w *evWorld) Start(_ context.Context, s Step) error {
	w.started, w.state[s.ID] = append(w.started, s), StepRunning
	return nil
}

func (w *evWorld) Kill(_ context.Context, _, id string) error { w.state[id] = StepDead; return nil }

func (w *evWorld) GC(context.Context, []string) ([]string, error) { return nil, nil }

func (w *evWorld) Put(_ context.Context, _, _, src string) error {
	w.put = append(w.put, src)
	return nil
}

func (w *evWorld) write(dest string) error {
	for name, text := range w.files {
		if err := writeFile(filepath.Join(dest, name), []byte(text)); err != nil {
			return err
		}
	}
	return nil
}

func (w *evWorld) Fetch(_ context.Context, _, id, dest string) error {
	res := map[string]any{"ok": true, "prepared": evPrepared()}
	switch {
	case id == evRun:
		res = w.res
		if err := w.write(dest); err != nil {
			return err
		}
	case strings.HasPrefix(id, evVerify):
		res["evidence"] = w.check
		if err := writeFile(filepath.Join(dest, "run", "collected-again.txt"), []byte("x")); err != nil {
			return err
		}
	}
	return writeJSON(filepath.Join(dest, "result.json"), res)
}

func (w *evWorld) Salvage(_ context.Context, _, id, dest string) error {
	if w.away {
		return errors.New("no answer")
	}
	w.salvaged = append(w.salvaged, id)
	return w.write(dest)
}

func (w *evWorld) Clean(_ context.Context, _, id string) error {
	w.cleaned = append(w.cleaned, id)
	return nil
}

func evPrepared() *Prepared {
	return &Prepared{Experiment: "e1", Namespace: "ns", Hooks: "h", HooksPath: "/hooks", Command: "true", WorkloadRev: "abcdef1", Ready: true}
}

type evLab struct {
	t   *testing.T
	m   *Manager
	w   *evWorld
	now time.Time
}

const evInventory = "hosts:\n  h1: {local: true, isolation: one-way-ok, staging: /tmp/s, max_builds: 2, max_load5: 100, kubeconfigs: /tmp/k, evidence: /tmp/e, clusters: [c1]}\n" +
	"checkouts:\n  fuzzops: {abcdef1: {h1: /tmp/fz}}\n  hooks: {h: {abcdef0: {h1: /hooks}}}\nhook_command: {h: \"true\"}\n"

// newEvLab is a root with lane c1 whose run step is in flight for experiment e1, whose subject declares the evidence
// verb or does not.
func newEvLab(t *testing.T, declares bool) *evLab {
	t.Helper()
	l := &evLab{t: t, now: time.Date(2026, 11, 24, 8, 0, 0, 0, time.UTC),
		w: &evWorld{state: map[string]string{evRun: StepRunning}, files: map[string]string{}, checks: StepFinished}}
	root := Root(t.TempDir())
	evOK(t, writeFile(filepath.Join(string(root), "hosts.yaml"), []byte(evInventory)))
	var lock map[string]any
	b, _ := json.Marshal(Lock{Spec: Spec{Name: "e1", Fuzzops: "abcdef1", Namespace: "ns", Sequences: [][]int{{1}}}})
	evOK(t, json.Unmarshal(b, &lock))
	if declares {
		lock["spec"].(map[string]any)["described"] = map[string]any{"verify_evidence": true}
	}
	evOK(t, writeJSON(root.LockPath("e1"), lock))
	rs := &RunSpec{ID: "r1", Fault: "a@x", Seed: 1, Position: 1, Label: "op"}
	evOK(t, writeJSON(root.LanePath("c1"), LaneFile{Lane: "c1", Host: "h1", State: LaneRunning, Since: l.now, Experiment: "e1",
		Seq: &Seq{Fault: "a@x", Q: 1}, Attempt: 1, Position: 1, Prepared: evPrepared(), Step: &StepRef{ID: evRun, Kind: StepRun, RunID: "r1", Run: rs, Started: l.now}}))
	l.m = &Manager{Root: root}
	l.restart()
	return l
}

// restart replaces the manager by a new one on the same root.
func (l *evLab) restart() {
	l.m = &Manager{Root: l.m.Root, Ops: func(string, Host) HostOps { return l.w }, Now: func() time.Time { return l.now }}
}

// pass runs one pass of the manager 20 minutes on; every step but the run that was started before it has ended.
func (l *evLab) pass() {
	l.t.Helper()
	l.now = l.now.Add(20 * time.Minute)
	for id, st := range l.w.state {
		if id != evRun && st == StepRunning {
			l.w.state[id] = StepFinished
			if strings.HasPrefix(id, evVerify) {
				l.w.state[id] = l.w.checks
			}
		}
	}
	evOK(l.t, l.m.Reconcile(context.Background()))
}

func (l *evLab) lane() LaneFile {
	l.t.Helper()
	var lf LaneFile
	evOK(l.t, readJSON(l.m.Root.LanePath("c1"), &lf))
	return lf
}

// kinds are the steps started, in order.
func (l *evLab) kinds() string {
	var out []string
	for _, s := range l.w.started {
		out = append(out, s.Kind)
	}
	return strings.Join(out, " ")
}

// erased: a step was started that resets, rebuilds, runs on or releases the lane.
func (l *evLab) erased() bool {
	return strings.Contains(l.kinds(), StepBuild) || strings.Contains(l.kinds(), StepRun) || strings.Contains(l.kinds(), StepRelease)
}

// end ends the run step with result.json res.
func (l *evLab) end(res map[string]any) {
	res["kind"], res["prepared"] = StepRun, evPrepared()
	l.w.res, l.w.state[evRun] = res, StepFinished
}

// staysHeld runs passes (with a manager restart) and fails when the lane leaves its hold or anything erases it.
func (l *evLab) staysHeld(reasons ...string) {
	l.t.Helper()
	for i := 0; i < 8; i++ {
		if i == 3 {
			l.restart()
		}
		l.pass()
	}
	l.heldNow(reasons...)
}

func (l *evLab) heldNow(reasons ...string) {
	l.t.Helper()
	var st strings.Builder
	evOK(l.t, Status(l.m.Root, "", &st))
	for _, r := range append(reasons, "way out: ") {
		if !strings.Contains(st.String(), r) {
			l.t.Errorf("status does not say %q:\n%s", r, st.String())
		}
	}
	if lf := l.lane(); lf.State != LaneHeld && lf.Step == nil || l.erased() {
		l.t.Fatalf("lane %s (%s), steps %q: want held for %q and left alone\n%s", lf.State, lf.Reason, l.kinds(), reasons, readLog(l.m))
	}
}

func readLog(m *Manager) string {
	b, _ := os.ReadFile(m.Root.path("bench.log"))
	return string(b)
}

// verifies: the subject's next answer is "verified". Nobody does anything: the lane's incidents are reset and it goes
// back to use.
func (l *evLab) verifies() {
	l.t.Helper()
	l.w.check = map[string]any{"verified": true}
	l.w.checks = StepFinished
	for i := 0; i < 6; i++ {
		l.pass()
	}
	if got := l.kinds(); !strings.Contains(got, evVerify+" "+StepUnhold+" "+StepRelease) {
		l.t.Fatalf("steps %q: want the check, the reset the subject owes, then the release\n%s", got, readLog(l.m))
	}
	if st := l.lane().State; st != LaneFree {
		l.t.Fatalf("lane %s", st)
	}
}

func done(extra map[string]any) map[string]any {
	res := map[string]any{"ok": true, "status": StatusDone}
	for k, v := range extra {
		res[k] = v
	}
	return res
}

func missing(reasons ...string) map[string]any {
	return map[string]any{"verified": false, "reasons": reasons}
}

func TestARunHoldsItsLaneUntilItsSubjectVerifiesItsEvidence(t *testing.T) {
	for name, tc := range map[string]struct {
		res    map[string]any
		reason string
	}{
		"missing evidence":            {done(map[string]any{"evidence": missing("store x empty", "log y cut")}), "store x empty; log y cut"},
		"no answer, collect killed":   {done(map[string]any{"collect_failed": "collect: timed out after 10m0s"}), "collect: timed out after 10m0s"},
		"no answer":                   {done(nil), "not verified"},
		"an invalid run":              {map[string]any{"status": StatusInvalid, "reason": "capped", "evidence": missing("store x empty")}, "store x empty"},
		"residue":                     {done(map[string]any{"residue": []string{"clusterrole/x"}, "evidence": missing("store x empty")}), "store x empty"},
		"verified and still reasons":  {done(map[string]any{"evidence": map[string]any{"verified": true, "reasons": []string{"z"}}}), "z"},
		"a manifest says it is whole": {done(nil), "not verified"},
	} {
		t.Run(name, func(t *testing.T) {
			l := newEvLab(t, true)
			l.w.files["operator/"+EvidenceManifest] = `{"problems": []}` // the subject's own file: not the platform's to read
			l.w.check = missing("still missing")
			l.end(tc.res)
			l.pass()
			l.heldNow(tc.reason)
			l.staysHeld("still missing")
			l.verifies()
		})
	}
}

func TestAVerifiedRunIsReleasedAtOnce(t *testing.T) {
	l := newEvLab(t, true)
	l.w.files["operator/"+EvidenceManifest] = `{"problems": ["the subject's own file"]}`
	l.end(done(map[string]any{"evidence": map[string]any{"verified": true}, "evidence_incomplete": "1 problem(s): the subject's own file"}))
	l.pass()
	if got := l.kinds(); got != StepRelease {
		t.Fatalf("steps %q, lane %+v", got, l.lane())
	}
}

// What a check collected again goes to the run's directory only with a "verified".
func TestACheckBringsHomeWhatItCollectedOnlyWhenItIsVerified(t *testing.T) {
	l := newEvLab(t, true)
	l.w.files["record.json"] = "{}"
	l.w.check = missing("x")
	l.end(done(nil))
	l.staysHeld()
	home := filepath.Join(l.m.Root.RunDir("e1", "r1"), "collected-again.txt")
	if fileExists(home) || len(l.w.put) == 0 || l.w.put[0] != l.m.Root.RunDir("e1", "r1") {
		t.Fatalf("home has it: %v; sent %v", fileExists(home), l.w.put)
	}
	l.verifies()
	if !fileExists(home) || !fileExists(filepath.Join(filepath.Dir(home), "record.json")) || !fileExists(filepath.Join(filepath.Dir(home), "run.json")) {
		t.Fatal("the run directory lacks what was verified, or lost what it had")
	}
}

// A check whose hook hangs is killed at the step's bound, and one that dies is asked again: neither frees the lane nor
// needs a person.
func TestACheckThatDiesIsAskedAgain(t *testing.T) {
	l := newEvLab(t, true)
	l.end(done(nil))
	l.w.checks = StepDead
	l.staysHeld("the check died")
	if strings.Contains(l.kinds(), StepRecover) || strings.Count(l.kinds(), evVerify) < 2 {
		t.Fatalf("steps %q", l.kinds())
	}
	l.verifies()
}

// A collect that failed after the evidence was verified (its last part is the reset between runs) goes on through that
// reset.
func TestAFailedCollectWithVerifiedEvidenceIsResetAndGoesOn(t *testing.T) {
	l := newEvLab(t, true)
	l.end(done(map[string]any{"evidence": map[string]any{"verified": true}, "collect_failed": "collect exit 1: failed reset"}))
	for i := 0; i < 4; i++ {
		l.pass()
	}
	if got := l.kinds(); got != StepUnhold+" "+StepRelease {
		t.Fatalf("steps %q", got)
	}
}

func TestARunStepThatDiesHoldsTheLane(t *testing.T) {
	l := newEvLab(t, true)
	l.w.files["record.json"], l.w.away = "{}", true
	l.w.state[evRun] = StepDead
	l.pass()
	if l.kinds() != StepRecover {
		t.Fatalf("steps %q, want the recover step", l.kinds())
	}
	if len(l.w.cleaned) != 0 {
		t.Fatalf("cleaned %v before its files were home", l.w.cleaned)
	}
	l.staysHeld("the files of step " + evRun + " are not home")
	if strings.Contains(l.kinds(), evVerify) {
		t.Fatal("checked before its files were home")
	}
	l.w.away, l.w.check = false, missing("no record")
	l.staysHeld("no record")
	if len(l.w.salvaged) != 1 || l.w.cleaned[0] != evRun && l.w.cleaned[len(l.w.cleaned)-1] == "" {
		t.Fatalf("salvaged %v, cleaned %v", l.w.salvaged, l.w.cleaned)
	}
	if b, err := os.ReadFile(filepath.Join(l.m.Root.RunDir("e1", "r1"), "record.json")); err != nil || string(b) != "{}" {
		t.Fatalf("what the dead step left is not in the run directory: %v", err)
	}
	l.verifies()
}

// A run given up with its host (its item runs elsewhere under the same ID) keeps its evidence apart and holds its lane.
func TestARunLostWithItsHostHoldsTheLane(t *testing.T) {
	l := newEvLab(t, true)
	evOK(t, l.m.Reconcile(context.Background()))
	l.m.hostLost("h1", l.now.Add(-time.Hour), errors.New("no route"))
	evOK(t, writeJSON(l.m.Root.LanePath("c1"), l.m.lanes["c1"]))
	l.w.state[evRun], l.w.check = StepDead, missing("no record")
	l.restart()
	l.staysHeld("no record")
	if want := l.m.Root.path("e1", "lost", evRun); l.w.put[0] != want || !fileExists(filepath.Join(want, "run.json")) {
		t.Fatalf("checked %v, want %s", l.w.put, want)
	}
	l.verifies()
}

// A lane that goes down, is reset by hand or leaves hosts.yaml (with or without its host) keeps its cluster while it
// owes evidence.
func TestNoPathErasesALaneThatOwesEvidence(t *testing.T) {
	l := newEvLab(t, true)
	l.w.check = missing("x")
	l.end(done(map[string]any{"lane_down": "the fault is still active"}))
	for i := 0; i < 6; i++ {
		l.pass()
	}
	if lf := l.lane(); lf.State != LaneDown || l.erased() || !strings.Contains(l.kinds(), evVerify) {
		t.Fatalf("lane %s (%s), steps %q", lf.State, lf.Reason, l.kinds())
	}
	if err := LaneReset(l.m.Root, "c1"); err == nil || !strings.Contains(err.Error(), "evidence") {
		t.Fatalf("reset by hand: %v", err)
	}
	lf := l.lane()
	lf.Step = nil
	for _, kind := range []string{StepBuild, StepRun, StepRelease} {
		if l.m.start(context.Background(), &lf, Step{Kind: kind, Prev: evPrepared(), DeleteCluster: kind == StepRelease}) {
			t.Fatalf("%s started on a lane that owes evidence", kind)
		}
	}
	up := strings.Replace(evInventory, "clusters: [c1]", "cluster_up: \"true {evidence}\", cluster_down: \"true\", clusters: [c2]", 1)
	for _, inv := range []string{up, strings.Replace(up, "  h1: {", "  h9: {", 1)} {
		evOK(t, writeFile(l.m.Root.path("hosts.yaml"), []byte(inv)))
		for i := 0; i < 4; i++ {
			l.pass()
		}
		if l.erased() || !fileExists(l.m.Root.LanePath("c1")) {
			t.Fatalf("lane file %v, steps %q\n%s", fileExists(l.m.Root.LanePath("c1")), l.kinds(), readLog(l.m))
		}
	}
	evOK(t, writeFile(l.m.Root.path("hosts.yaml"), []byte(evInventory)))
	l.w.check = map[string]any{"verified": true}
	for i := 0; i < 8 && !l.erased(); i++ {
		l.pass()
	}
	if strings.Contains(l.kinds(), evVerify) && l.lane().State == LaneDown {
		t.Fatalf("verified and still down: %q", l.kinds())
	}
}

// A pool that refills takes any free lane it can, never one that owes evidence.
func TestAPoolRefillLeavesAHeldLaneAlone(t *testing.T) {
	l := newEvLab(t, true)
	spec := "name: pool\napp: examples/shop\nfuzzops: abcdef1\noperator: {hooks: h, revision: abcdef0}\nnamespace: ns\nfaults: {instances: i.json, ids: [none]}\n" +
		"sequences: [[1]]\nrun: {window: 1s}\nready_for: 1s\nready_timeout: 1s\ncap: 10m\nlanes: 1\nhosts: [h1]\nhold: true\nretries: 0\n"
	evOK(t, writeFile(l.m.Root.SpecPath("pool"), []byte(spec)))
	s, err := LoadSpec(l.m.Root.SpecPath("pool"))
	evOK(t, err)
	evOK(t, writeJSON(l.m.Root.LockPath("pool"), Lock{Spec: s}))
	l.w.check = missing("x")
	l.end(done(nil))
	l.staysHeld("x")
	evOK(t, writeJSON(l.m.Root.LanePath("c1"), LaneFile{Lane: "c1", Host: "h1", State: LaneFree, Since: l.now, Prepared: evPrepared()}))
	for i := 0; i < 3; i++ {
		l.pass()
	}
	if !strings.Contains(l.kinds(), StepBuild) {
		t.Fatalf("the pool would not have taken the lane anyway: steps %q\n%s", l.kinds(), readLog(l.m))
	}
}

// Removing the experiment, also with its runs in flight killed (what `plan remove` does on its own), clears no hold;
// giving the run up by name does.
func TestOnlyGivingTheRunUpClearsAHold(t *testing.T) {
	l := newEvLab(t, true)
	l.w.check = missing("x")
	l.end(done(nil))
	l.pass()
	evOK(t, writeJSON(l.m.Root.RemovedPath("e1"), map[string]any{"cancel": true}))
	l.staysHeld("x", "lanes remove e1 --teardown")
	if b := (&PlanBackend{Root: l.m.Root}); !b.laneBound("e1") {
		t.Fatal("a removed plan would move the run's directory away while its lane is held for it")
	}
	evOK(t, Remove(l.m.Root, "e1", true))
	for i := 0; i < 4; i++ {
		l.pass()
	}
	if !strings.HasSuffix(l.kinds(), StepRelease) || l.lane().State != LaneFree {
		t.Fatalf("steps %q, lane %+v", l.kinds(), l.lane())
	}
}

// A try that was no run (its gate never passed) holds nothing.
func TestATryThatWasNoRunHoldsNothing(t *testing.T) {
	l := newEvLab(t, true)
	l.end(map[string]any{"status": StatusNotReady, "reason": "not ready", "collect_failed": "collect exit 1"})
	l.pass()
	if l.kinds() != StepRelease {
		t.Fatalf("steps %q", l.kinds())
	}
}

// A subject that does not declare the verb behaves as before it existed, whatever the ending, and is never asked.
func TestAnUndeclaredSubjectBehavesAsBefore(t *testing.T) {
	for name, tc := range map[string]struct {
		end  func(l *evLab)
		want string
	}{
		"done":           {func(l *evLab) { l.end(done(nil)) }, StepRelease},
		"invalid":        {func(l *evLab) { l.end(map[string]any{"status": StatusInvalid, "reason": "capped"}) }, StepRelease},
		"collect failed": {func(l *evLab) { l.end(done(map[string]any{"collect_failed": "collect: timed out after 10m0s"})) }, StepRelease},
		"killed step":    {func(l *evLab) { l.w.state[evRun] = StepDead }, StepRecover + " " + StepRelease},
		"residue":        {func(l *evLab) { l.end(done(map[string]any{"residue": []string{"clusterrole/x"}})) }, StepRelease},
	} {
		for _, restart := range []bool{false, true} {
			t.Run(name, func(t *testing.T) {
				l := newEvLab(t, false)
				tc.end(l)
				for i := 0; i < 5; i++ {
					l.pass()
					if restart {
						l.restart()
					}
				}
				// the fake release leaves its subject on the lane, so it is released again on every later pass
				if got := l.kinds(); !strings.HasPrefix(got, tc.want) || strings.Contains(got, evVerify) || strings.Contains(got, StepUnhold) {
					t.Fatalf("steps %q, want %q", got, tc.want)
				}
				if l.w.cleaned[0] != evRun || len(l.w.salvaged)+len(l.w.put) != 0 {
					t.Fatalf("cleaned %v, salvaged %v, sent %v", l.w.cleaned, l.w.salvaged, l.w.put)
				}
			})
		}
	}
	t.Run("held by its manifest, then completed", func(t *testing.T) {
		l := newEvLab(t, false)
		l.w.files["operator/"+EvidenceManifest] = `{"problems": ["x"]}`
		l.end(done(map[string]any{"evidence_incomplete": "1 problem(s): x"}))
		for i := 0; i < 4; i++ {
			l.pass()
			l.restart()
		}
		if lf := l.lane(); lf.State != LaneHeld || l.kinds() != "" {
			t.Fatalf("lane %+v, steps %q", lf, l.kinds())
		}
		evOK(t, writeFile(filepath.Join(l.m.Root.RunDir("e1", "r1"), "operator", EvidenceManifest), []byte(`{"problems": []}`)))
		for i := 0; i < 3; i++ {
			l.pass()
		}
		if got := l.kinds(); !strings.HasPrefix(got, StepUnhold+" "+StepRelease) {
			t.Fatalf("steps %q", got)
		}
	})
	t.Run("held by a manifest that is gone", func(t *testing.T) {
		l := newEvLab(t, false)
		l.end(done(map[string]any{"evidence_incomplete": "1 problem(s): x"}))
		l.pass()
		l.pass()
		if got := l.kinds(); !strings.HasPrefix(got, StepUnhold) {
			t.Fatalf("steps %q", got)
		}
	})
}

// What a dead step left is added to what is home already, never over it.
func TestSalvageKeepsWhatIsHome(t *testing.T) {
	staging, dest := t.TempDir(), filepath.Join(t.TempDir(), "runs", "r1")
	h := localHost{staging: staging}
	evOK(t, h.Salvage(context.Background(), "c1", "gone", dest))
	step := filepath.Join(staging, "c1", "s1")
	evOK(t, writeFile(filepath.Join(step, "record.json"), []byte("dead")))
	evOK(t, writeFile(filepath.Join(step, "operator", "a"), []byte("dead")))
	evOK(t, writeFile(filepath.Join(dest, "operator", "a"), []byte("home")))
	evOK(t, h.Salvage(context.Background(), "c1", "s1", dest))
	evOK(t, h.Salvage(context.Background(), "c1", "s1", dest))
	for name, want := range map[string]string{"record.json": "dead", "operator/a": "home"} {
		if b, _ := os.ReadFile(filepath.Join(dest, name)); string(b) != want {
			t.Errorf("%s = %q, want %q", name, b, want)
		}
	}
}

func evOK(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
