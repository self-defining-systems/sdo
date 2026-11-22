//go:build integration

package bench

import (
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
)

// The host's workload_env is the workload's own configuration (docs/contract.md): every process the platform starts for
// a workload gets it, whichever step starts it and whether or not the experiment that prepared the lane still exists.
// A recover step runs after a plan was removed, so there is no experiment to name the workload; it takes the workload
// from what the lane was prepared with, and its env must follow that same workload.

const workloadEnvMark = "WL_ENV_MARK=from-workload-env"

// A step of every kind carries the workload's env, with its experiment present and gone.
func TestEveryStepKindCarriesTheWorkloadEnvOfTheLanesWorkload(t *testing.T) {
	for _, experimentPresent := range []bool{true, false} {
		l := newFlab(t, "h1=c1")
		l.specExtra = "workload: ./adapter-x\n"
		l.extra["h1"] = `, env: {HOST_VAR: host}, workload_env: {"./adapter-x": {WL_ENV_MARK: from-workload-env}, other: {OTHER_ONLY: x}}`
		l.writeInventory()
		l.add("e1", []string{"h1"}, "a@x", 0)
		l.runningStep("c1", StepBuild)
		l.buildOK("c1")
		l.pass()
		lane := l.m.lanes["c1"]
		if lane.Prepared == nil {
			t.Fatalf("lane has no prepared operator: %+v", lane)
		}
		if !experimentPresent {
			delete(l.m.exps, "e1") // a plan removed while the lane still holds its app
			lane.Experiment = ""
		}
		for _, kind := range []string{StepBuild, StepRun, StepRelease, StepUnhold, StepRecover} {
			lane.Step = nil
			s := Step{Kind: kind, Prev: lane.Prepared}
			if kind == StepBuild || kind == StepRun {
				if !experimentPresent {
					continue // a build or run is made from an experiment: there is none to start one from
				}
				x := l.m.exps["e1"]
				st, err := l.m.expStep(context.Background(), lane, x, kind)
				if err != nil {
					t.Fatal(err)
				}
				s = st
				if kind == StepRun {
					s.Run = &RunSpec{ID: "r1"}
				}
			}
			if !l.m.start(context.Background(), lane, s) {
				t.Fatalf("%s was refused (experiment present: %v)", kind, experimentPresent)
			}
			got := l.w.last("c1", kind)
			if got == nil {
				t.Fatalf("%s was not started", kind)
			}
			env := strings.Join(got.HostEnv, " ")
			if !slices.Contains(got.HostEnv, workloadEnvMark) || !strings.Contains(env, "HOST_VAR=host") {
				t.Errorf("%s step (experiment present: %v) host env lacks the workload_env: %s", kind, experimentPresent, env)
			}
			if strings.Contains(env, "OTHER_ONLY") {
				t.Errorf("%s step carries another workload's env: %s", kind, env)
			}
		}
	}
}

// recordingWorkload answers every verb with exit 0 and keeps the environment of each call.
type recordingWorkload struct {
	mu     sync.Mutex
	calls  []WorkloadCall
	active bool // the fault is active until its first undo
}

func (w *recordingWorkload) Call(_ context.Context, c WorkloadCall) (int, string, error) {
	w.mu.Lock()
	w.calls = append(w.calls, c)
	was := w.active
	if c.Verb == VerbRun && c.Action == "undo" {
		w.active = false
	}
	w.mu.Unlock()
	if c.Stdout != nil && c.Verb == VerbRun && (c.Action == "active" || c.Action == "undo") {
		answer := `{"active": false}`
		if c.Action == "active" && was {
			answer = `{"active": true}`
		}
		io.WriteString(c.Stdout, answer+"\n")
	}
	if c.Stdout != nil && c.Verb == VerbIdentity {
		io.WriteString(c.Stdout, `{"revision": "r", "modified": false}`+"\n")
	}
	return 0, "", nil
}

func (w *recordingWorkload) Grade(context.Context, string, []string, io.Writer) error { return nil }

// Every workload verb the step runner calls gets the host's env (workload_env included) and the lane's facts, from
// the one place that builds a call's environment: the verbs are enumerated, so a verb added to a step that skips it
// fails here.
func TestEveryWorkloadCallOfAStepGetsTheHostEnvAndLaneFacts(t *testing.T) {
	r := newRunner(t, presentCluster)
	rec := &recordingWorkload{active: true}
	r.wl = rec
	r.s.HostEnv = []string{"HOST_VAR=host", workloadEnvMark}
	r.s.Kubeconfig = "/kube/lane-a"
	p := &Prepared{Experiment: "e", Namespace: "ns", AppPack: "pack", Pack: "pack"}
	q := &Prepared{Experiment: "old", Namespace: "old-ns", AppPack: "old-pack"}
	r.s.New, r.s.Prev = p, q
	ctx := context.Background()

	_, _ = r.version(ctx)
	r.packLoads(ctx, p)
	r.recoverLane(ctx, p)
	r.freshApp(ctx)
	if err := os.WriteFile(r.freshFingerprintFile(), []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	r.appMatchesFresh(ctx, p)
	r.makeCluster(ctx, p)
	r.appHealthy(ctx, p, true)
	r.checkOnce(ctx, p)
	r.safety(ctx, p, &RunSpec{Fault: "f1", Instances: "instances.json"})

	want := []string{"identity/", "app-up/check-pack", "app-reset/recover", "app-up/down", "app-up/up", "app-snapshot/write",
		"app-snapshot/verify", "app-up/images", "gate/check", "run/active", "run/undo"}
	var seen []string
	for _, c := range rec.calls {
		seen = append(seen, c.Verb+"/"+c.Action)
		env := strings.Join(c.Env, " ")
		for _, need := range []string{"HOST_VAR=host", workloadEnvMark, "BENCH_LANE=lane-a", "KUBECONFIG=/kube/lane-a"} {
			if !strings.Contains(env, need) {
				t.Errorf("%s/%s call lacks %s in its env: %s", c.Verb, c.Action, need, env)
			}
		}
	}
	for _, w := range want {
		if !slices.Contains(seen, w) {
			t.Errorf("the test never exercised %s (saw %v): drive the step code that calls it", w, seen)
		}
	}
}

// The workload's calls made outside a step (the footprint at add, the grade of results, the identity of a pin check
// on a host) get the same env: the host's, with the workload_env of that workload on top.
func TestHostLevelWorkloadCallsGetTheWorkloadEnv(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "seen")
	script := "#!/bin/sh\n" + `echo "$WL_ENV_MARK $HOST_VAR" >> ` + out + "\n" + `[ "$WORKLOAD_VERB" = footprint ] && echo '{"cpus": 2, "memory": "4Gi"}'` + "\nexit 0\n"
	if err := os.WriteFile(filepath.Join(dir, "wl.sh"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	h := Host{Env: map[string]string{"HOST_VAR": "host"}, WorkloadEnv: map[string]map[string]string{"./wl.sh": {"WL_ENV_MARK": "mark"}}}
	env := h.EnvFor("./wl.sh")
	if _, err := footprintOf(context.Background(), Spec{Workload: "./wl.sh", App: "app", Name: "e"}, dir, env); err != nil {
		t.Fatalf("footprint: %v", err)
	}
	if err := NewWorkload("./wl.sh", dir).Grade(context.Background(), dir, env, io.Discard); err != nil {
		t.Fatalf("grade: %v", err)
	}
	ident, err := exec.Command("sh", "-c", IdentityCommand("./wl.sh", dir, env)).CombinedOutput()
	if err != nil {
		t.Fatalf("identity: %v %s", err, ident)
	}
	b, _ := os.ReadFile(out)
	if got := strings.Fields(string(b)); !slices.Equal(got, []string{"mark", "host", "mark", "host", "mark", "host"}) {
		t.Errorf("footprint, grade and identity saw %q, want the workload_env and host env on each of the three calls", got)
	}
}
