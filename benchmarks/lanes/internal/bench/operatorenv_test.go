//go:build integration

package bench

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// A workload verb that needs the operator's settings (a real agent's images are chosen from them) must see them in
// every call of a step, not only from the one that happens to follow the operator file's write: the file is written
// before the step's first workload call and BENCH_OPERATOR comes from the one place that builds a call's env.

// operatorSeen is what a call's env says about the operator file at the time of the call.
type operatorSeen struct {
	call    string
	path    string
	content string
}

func operatorOf(t *testing.T, c WorkloadCall) operatorSeen {
	t.Helper()
	seen := operatorSeen{call: c.Verb + "/" + c.Action}
	for _, kv := range c.Env {
		if v, ok := strings.CutPrefix(kv, "BENCH_OPERATOR="); ok {
			seen.path = v
		}
	}
	if seen.path != "" {
		if b, err := os.ReadFile(seen.path); err == nil {
			seen.content = string(b)
		}
	}
	return seen
}

func operatorSpec() *HookOperator {
	return &HookOperator{Hooks: "sregym", Revision: "r1", Settings: map[string]any{"model": "synthetic-model", "provider": "claude"}}
}

// callsOfBuildPaths drives the workload calls each way a build reaches an app: a fresh app (the last sequence's copy
// is another app and goes down first), a rewind that keeps the last sequence's app (it is checked against the fresh
// one), a swap or pool claim (the lane holds a prepared operator), and a lane's cluster made anew.
func callsOfBuildPaths(t *testing.T, scenario string) []WorkloadCall {
	t.Helper()
	r := newRunner(t, presentCluster)
	rec := &recordingWorkload{active: true}
	r.wl = rec
	r.s.Kubeconfig = "/kube/lane-a"
	r.s.OperatorFile = operatorSpec()
	p := &Prepared{Experiment: "e", Namespace: "ns", AppPack: "pack", Pack: "pack", Hooks: "sregym"}
	prev := &Prepared{Experiment: "old", Namespace: "old-ns", AppPack: "old-pack", Hooks: "sregym", Ready: true,
		Operator: filepath.Join(r.s.LaneDir, "operator.json")}
	must(t, writeJSON(prev.Operator, &HookOperator{Hooks: "test", Settings: map[string]any{"model": "old-model"}}))
	r.s.New, r.s.Prev = p, prev
	ctx := context.Background()
	if err := r.writeSpecFile(); err != nil {
		t.Fatal(err)
	}
	_, _ = r.version(ctx)
	r.packLoads(ctx, p)
	switch scenario {
	case "fresh app":
		r.recoverLane(ctx, prev)
		r.recoverLane(ctx, p)
		r.freshApp(ctx)
		r.appHealthy(ctx, p, true)
	case "rewind":
		r.s.Prev = &Prepared{Experiment: "old", Namespace: "ns", AppPack: "pack", Hooks: "sregym", Ready: true, Operator: prev.Operator}
		r.recoverLane(ctx, r.s.Prev)
		if err := os.WriteFile(r.freshFingerprintFile(), []byte("{}"), 0o644); err != nil {
			t.Fatal(err)
		}
		r.appMatchesFresh(ctx, p)
		r.appHealthy(ctx, p, false)
	case "swap":
		r.s.Swap = true
		r.checkOnce(ctx, p)
	case "cluster made anew":
		r.s.Create = true
		r.makeCluster(ctx, p)
		r.freshApp(ctx)
		r.recordFresh(ctx, p)
	default:
		t.Fatalf("unknown scenario %q", scenario)
	}
	return rec.calls
}

func TestEveryWorkloadCallOfABuildSeesTheOperatorFile(t *testing.T) {
	want := map[string][]string{
		"fresh app":         {"identity/", "app-up/check-pack", "app-reset/recover", "app-up/down", "app-up/up", "app-snapshot/write", "gate/check"},
		"rewind":            {"identity/", "app-up/check-pack", "app-reset/recover", "app-snapshot/verify", "gate/check"},
		"swap":              {"identity/", "app-up/check-pack", "gate/check"},
		"cluster made anew": {"identity/", "app-up/check-pack", "app-up/images", "app-up/down", "app-up/up", "app-snapshot/write"},
	}
	for scenario, verbs := range want {
		t.Run(scenario, func(t *testing.T) {
			calls := callsOfBuildPaths(t, scenario)
			var seen []string
			for _, c := range calls {
				got := operatorOf(t, c)
				seen = append(seen, got.call)
				if got.path == "" {
					t.Errorf("%s: BENCH_OPERATOR is not set in the call's env", got.call)
					continue
				}
				model := "synthetic-model"
				if slices.Contains(c.Env, "BENCH_EXPERIMENT=old") {
					model = "old-model"
				}
				if !strings.Contains(got.content, model) {
					t.Errorf("%s: BENCH_OPERATOR=%s should contain %q, got %q", got.call, got.path, model, got.content)
				}
			}
			for _, v := range verbs {
				if !slices.Contains(seen, v) {
					t.Errorf("the scenario never made %s (saw %v): drive the step code that calls it", v, seen)
				}
			}
		})
	}
}

// The run, safety, release and recover calls: the operator file is the one the build wrote, found through the lane.
func TestEveryWorkloadCallOfARunAndAReleaseSeesTheOperatorFile(t *testing.T) {
	r := newRunner(t, presentCluster)
	rec := &recordingWorkload{active: true}
	r.wl = rec
	r.s.OperatorFile = operatorSpec()
	p := &Prepared{Experiment: "e", Namespace: "ns", AppPack: "pack", Pack: "pack"}
	r.s.New, r.s.Prev = p, p
	if err := r.writeSpecFile(); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	r.safety(ctx, p, &RunSpec{Fault: "f1", Instances: "instances.json"})
	r.recoverLane(ctx, p)
	if len(rec.calls) == 0 {
		t.Fatal("no call made")
	}
	for _, c := range rec.calls {
		if got := operatorOf(t, c); got.path == "" || !strings.Contains(got.content, "synthetic-model") {
			t.Errorf("%s: BENCH_OPERATOR=%q content %q", got.call, got.path, got.content)
		}
	}
}

// A step reaches its first workload call (the identity) through RunStep: the file is there by then, and the flag that
// says the spec has an operator block agrees with the variable that names it.
func TestRunStepWritesTheOperatorFileBeforeItsFirstWorkloadCall(t *testing.T) {
	root := t.TempDir()
	fuzzops := filepath.Join(root, "wl")
	if err := os.MkdirAll(fuzzops, 0o755); err != nil {
		t.Fatal(err)
	}
	seenFile := filepath.Join(root, "seen")
	script := `#!/bin/sh
echo "$WORKLOAD_VERB/$WORKLOAD_ACTION op=$BENCH_OPERATOR block=$BENCH_OPERATOR_BLOCK content=$(cat "$BENCH_OPERATOR" 2>/dev/null | tr -d ' \n')" >> ` + seenFile + `
case "$WORKLOAD_VERB" in identity) echo '{"revision":"r","modified":false}';; esac
exit 0
`
	if err := os.WriteFile(filepath.Join(fuzzops, "adapter"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	stepDir := filepath.Join(root, "vc1.steps", "build-1")
	if err := os.MkdirAll(stepDir, 0o755); err != nil {
		t.Fatal(err)
	}
	s := Step{ID: "build-1", Kind: StepBuild, Lane: "vc1", Host: "h", Fuzzops: fuzzops, Workload: "./adapter", LaneDir: filepath.Join(root, "lane"),
		Kubeconfig: filepath.Join(root, "kube"), ClusterUp: "exit 1", Create: true,
		New: &Prepared{Experiment: "e", Namespace: "ns", AppPack: "pack"}, OperatorFile: operatorSpec()}
	b, _ := json.Marshal(s)
	if err := os.WriteFile(filepath.Join(stepDir, "step.json"), b, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := RunStep(context.Background(), filepath.Join(stepDir, "step.json")); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(seenFile)
	lines := strings.Split(strings.TrimSpace(string(got)), "\n")
	if len(lines) < 2 {
		t.Fatalf("the adapter was called %d times: %q", len(lines), got)
	}
	for _, l := range lines {
		if !strings.Contains(l, "block=1") || !strings.Contains(l, "synthetic-model") || !strings.Contains(l, filepath.Join(root, hookInputDirName(s.ID), "operator.json")) {
			t.Errorf("call %q does not see the operator file", l)
		}
	}
}

// A kept lane can be claimed by a different subject. Each hook validates the
// block it received, including the old subject's teardown before the new prepare.
func TestBuildSwitchesSubjectsWithTheirOwnOperatorFiles(t *testing.T) {
	root := t.TempDir()
	t.Setenv("PATH", testdata("fakebin")+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("FAKE_STATE", root)
	for _, args := range [][]string{{"init", "-q"}, {"-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "hooks", "--allow-empty"}} {
		cmd := exec.Command("git", args...)
		cmd.Dir = root
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	adapter := filepath.Join(root, "adapter")
	must(t, os.WriteFile(adapter, []byte(`#!/bin/sh
case "$WORKLOAD_VERB" in
  identity) echo '{"revision":"r","modified":false}' ;;
esac
`), 0o755))
	hook := filepath.Join(root, "hook")
	must(t, os.WriteFile(hook, []byte(`#!/bin/sh
grep -Fq '"hooks": "'"$1"'"' "$BENCH_OPERATOR" || {
  echo "$2: expected operator block for $1, got $(cat "$BENCH_OPERATOR")" >&2
  if [ "$2" != idle ]; then exit 1; fi
  echo "wrong operator for $1 $2" >> "$FAKE_STATE/hooks"
}
echo "$1 $2" >> "$FAKE_STATE/hooks"
if [ "$2" = prepare ]; then echo '{"pack":"pack"}'; fi
`), 0o755))

	staging := filepath.Join(root, "staging")
	var prev *Prepared
	for _, subject := range []string{"subject-a", "subject-b"} {
		s := Step{
			ID: "build-" + subject, Kind: StepBuild, Lane: "c1", Experiment: subject,
			Fuzzops: root, Workload: "./adapter", LaneDir: filepath.Join(staging, "c1", "lane"),
			Cap: 1, ReadyTimeout: 1, Poll: 0.001,
			New: &Prepared{Experiment: subject, Hooks: subject, HooksPath: root,
				Command: shellQuote(hook) + " " + subject, AppPack: "pack"},
			OperatorFile: &HookOperator{Hooks: subject}, Prev: prev,
		}
		dir := filepath.Join(staging, "c1", s.ID)
		path := filepath.Join(dir, "step.json")
		must(t, writeJSON(path, s))
		must(t, RunStep(context.Background(), path))
		var result Result
		must(t, readJSON(filepath.Join(dir, "result.json"), &result))
		if !result.OK || result.Prepared == nil || result.Prepared.Hooks != subject {
			t.Fatalf("build %s failed: %s", subject, result.Reason)
		}
		prev = result.Prepared
		// The manager collects the completed step directory and can run staging
		// GC before the next experiment picks up the lane.
		host := Host{Staging: staging}
		manager := &Manager{
			Root:  Root(root),
			inv:   Inventory{Hosts: map[string]Host{"test": host}},
			lanes: map[string]*LaneFile{"c1": {Lane: "c1", Host: "test", Prepared: prev}},
			Ops:   func(string, Host) HostOps { return localHost{staging: host.Staging} },
		}
		must(t, os.RemoveAll(dir))
		manager.gcHost(context.Background(), "test")
	}
	b, err := os.ReadFile(filepath.Join(root, "hooks"))
	must(t, err)
	log := string(b)
	if strings.Contains(log, "wrong operator") || strings.Count(log, "subject-a teardown\n") != 2 || !strings.Contains(log, "subject-b prepare\n") {
		t.Fatalf("expected subject A's second teardown and subject B's prepare, got:\n%s", log)
	}
}
