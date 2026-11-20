//go:build integration

package bench

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// benchFixture is a root with an inventory of one local host and a fake
// fuzzops checkout whose bin/fuzzops answers `report` and `version`.
type benchFixture struct {
	root    Root
	fuzzops string
	specDir string
}

func newFixture(t *testing.T) benchFixture {
	t.Helper()
	dir := t.TempDir()
	f := benchFixture{root: Root(filepath.Join(dir, "root")), fuzzops: filepath.Join(dir, "fuzzops"), specDir: filepath.Join(dir, "in")}
	for _, d := range []string{string(f.root), filepath.Join(f.fuzzops, "bin"), filepath.Join(f.fuzzops, "examples", "shop"), filepath.Join(f.specDir, "base")} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	hook := testdata("hook.sh")
	inv := `hosts:
  h1: {local: true, staging: ` + dir + `/staging, max_builds: 1, max_load5: 1000, kubeconfigs: ` + dir + `/kube, evidence: ` + dir + `/evidence, clusters: [c1, c2]}
checkouts:
  fuzzops: {0b528ac: {h1: ` + f.fuzzops + `}}
  hooks:
    test: {4d2915b3f: {h1: ` + dir + `}}
hook_command: {test: "` + hook + `"}
`
	must(t, os.WriteFile(f.root.path("hosts.yaml"), []byte(inv), 0o644))
	must(t, os.WriteFile(filepath.Join(f.specDir, "instances.json"), []byte(`{"ids": ["a@x", "b@y"], "instances": []}`), 0o644))
	must(t, os.WriteFile(filepath.Join(f.specDir, "base", "baseline.json"), []byte(`{"namespace": "app1"}`), 0o644))
	// report: copy runs.jsonl's operators into grades.json; mark seed 9 interfered.
	fake := `#!/bin/sh
case $1 in
version) echo '{"revision":"0b528ac0000","measure_version":12}' ;;
report) d=$2
  python3 - "$d" <<'PY'
import json,sys
d=sys.argv[1]
recs=[json.loads(l) for l in open(d+"/runs.jsonl") if l.strip()]
json.dump([{"Fault":r["fault"],"Operator":r["operator"],"Grade":"better"} for r in recs], open(d+"/grades.json","w"))
json.dump([{"fault":r["fault"],"operator":r["operator"],"seed":r["seed"],"started":r["started"],"lane":r["lane"],"why":"api p50 ms 90 above 40"} for r in recs if r["seed"]==9], open(d+"/interference.json","w"))
open(d+"/report.md","w").write("# fuzzops report\n%d runs\n" % len(recs))
PY
;;
esac
`
	must(t, os.WriteFile(filepath.Join(f.fuzzops, "bin", "fuzzops"), []byte(fake), 0o755))
	return f
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func (f benchFixture) add(t *testing.T, body string) error {
	return Add(context.Background(), f.root, writeSpec(t, f.specDir, body), io.Discard)
}

func TestAddFreezesInputsAndRefusesReuse(t *testing.T) {
	f := newFixture(t)
	if err := f.add(t, exampleSpec); err != nil {
		t.Fatal(err)
	}
	x, err := f.root.LoadExperiment("detect-1")
	if err != nil {
		t.Fatal(err)
	}
	if x.Lock.Instances == nil || x.Lock.Baseline == nil || !strings.HasPrefix(x.Lock.Instances.Path, "inputs/") {
		t.Fatalf("lock %+v", x.Lock)
	}
	if _, err := os.Stat(f.root.path(x.Lock.Baseline.Path, "baseline.json")); err != nil {
		t.Fatalf("baseline not stored: %v", err)
	}
	if err := f.add(t, exampleSpec); err == nil || !strings.Contains(err.Error(), "never reused") {
		t.Fatalf("reuse: %v", err)
	}
	// Editing the spec: a live field applies, an identity field is refused (and the lock wins).
	edited := strings.Replace(strings.Replace(exampleSpec, "lanes: 2", "lanes: 5", 1), "cap: 25m", "cap: 40m", 1)
	must(t, os.WriteFile(f.root.SpecPath("detect-1"), []byte(edited), 0o644))
	x, _ = f.root.LoadExperiment("detect-1")
	if x.Measured().Lanes != 5 || x.Measured().Cap != "25m" || !strings.Contains(x.SpecErr, "cap") {
		t.Fatalf("lanes %d cap %s err %q", x.Measured().Lanes, x.Measured().Cap, x.SpecErr)
	}
}

// A seed is stored by content next to the baseline; one without a baseline, or missing, is refused.
func TestAddStoresTheSeedAndRefusesOneWithoutABaseline(t *testing.T) {
	f := newFixture(t)
	must(t, os.MkdirAll(filepath.Join(f.specDir, "seed"), 0o755))
	must(t, os.WriteFile(filepath.Join(f.specDir, "seed", "baseline.json"), []byte(`{"namespace": "app1", "note": "learned"}`), 0o644))
	if err := f.add(t, strings.Replace(exampleSpec, "  baseline: base", "  baseline: base\n  seed: seed", 1)); err != nil {
		t.Fatal(err)
	}
	x, err := f.root.LoadExperiment("detect-1")
	if err != nil || x.Lock.Seed == nil || x.Lock.Seed.SHA256 == x.Lock.Baseline.SHA256 {
		t.Fatalf("seed not stored by content: %+v %v", x.Lock, err)
	}
	if err := f.add(t, strings.Replace(strings.Replace(exampleSpec, "name: detect-1", "name: detect-2", 1), "  baseline: base", "  seed: seed", 1)); err == nil {
		t.Fatal("a seed without a baseline was accepted")
	}
}

func TestAddChecks(t *testing.T) {
	for _, c := range []struct{ edit, want string }{
		{"    - b@y", "not in the instances file"},
		{"hosts: [h2]", "no host"},
		{"  baseline: nowhere", "operator.baseline"},
		{"app: examples/none", "not a pack directory"},
	} {
		f := newFixture(t)
		body := exampleSpec
		switch {
		case strings.HasPrefix(c.edit, "    - "):
			body = strings.Replace(body, c.edit, "    - c@z", 1)
		case strings.HasPrefix(c.edit, "  baseline"):
			body = strings.Replace(body, "  baseline: base", c.edit, 1)
		default:
			k, _, _ := strings.Cut(c.edit, ":")
			for _, l := range strings.Split(body, "\n") {
				if strings.HasPrefix(l, k+":") {
					body = strings.Replace(body, l, c.edit, 1)
				}
			}
		}
		if err := f.add(t, body); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v", c.edit, err)
		}
	}
	// The hooks' check runs: a baseline it rejects is refused.
	f := newFixture(t)
	st := t.TempDir()
	t.Setenv("HOOK_STATE", st)
	must(t, os.WriteFile(filepath.Join(st, "fail-check"), []byte("namespace fg1 is not app1\n"), 0o644))
	if err := f.add(t, exampleSpec); err == nil || !strings.Contains(err.Error(), "namespace fg1 is not app1") {
		t.Fatalf("hook check: %v", err)
	}
}

// results grades done runs only, marks the runs above the host's fence
// invalid "host interference", and refuses a baseline measured another way.
func TestResultsGradesDoneRunsOnly(t *testing.T) {
	f := newFixture(t)
	must(t, f.add(t, exampleSpec))
	now := time.Now().UTC().Truncate(time.Second)
	put := func(exp string, rf RunFile, op string) {
		rf.Experiment, rf.OperatorLabel, rf.Host, rf.Lane = exp, op, "h1", "c1"
		rf.Fuzzops = Version{Revision: "0b528ac0000", MeasureVersion: 12}
		d := f.root.RunDir(exp, rf.ID)
		must(t, writeJSON(filepath.Join(d, "run.json"), rf))
		rec := map[string]any{"fault": rf.Fault, "operator": op, "seed": rf.Seed, "started": now.Add(time.Duration(rf.Seed) * time.Minute)}
		must(t, writeJSON(filepath.Join(d, "record.json"), rec))
	}
	put("detect-1", RunFile{ID: "a1", Fault: "a@x", Sequence: 1, Position: 1, Seed: 1, Attempt: 1, Status: StatusDone}, "exec:test-detect-1-r1")
	put("detect-1", RunFile{ID: "a2", Fault: "a@x", Sequence: 1, Position: 2, Seed: 9, Attempt: 1, Status: StatusDone}, "exec:test-detect-1-r2")
	put("detect-1", RunFile{ID: "a3", Fault: "a@x", Sequence: 1, Position: 3, Seed: 2, Attempt: 1, Status: StatusInvalid, Reason: "capped: reflection queued"}, "exec:test-detect-1-r3")
	must(t, writeJSON(filepath.Join(f.root.RunDir("detect-1", "a1"), "operator", "transcripts.json"),
		map[string]any{"incidents": []map[string]any{{"id": "i1", "outcome": true, "agents_ran": true, "rollouts": 0}}, "rollouts": 0, "missing": []string{"i1"}}))
	if err := Results(context.Background(), f.root, "detect-1", nil, io.Discard); err != nil {
		t.Fatal(err)
	}
	var meta ResultsMeta
	must(t, readJSON(f.root.path("detect-1", "results", ResultsMetaFile), &meta))
	if meta.At.IsZero() || meta.Fuzzops != "0b528ac" || len(meta.With) != 0 || meta.DoneRuns != 1 {
		t.Fatalf("meta %+v", meta)
	}
	b, _ := os.ReadFile(f.root.path("detect-1", "results", "runs.jsonl"))
	if n := strings.Count(strings.TrimSpace(string(b)), "\n") + 1; n != 1 || strings.Contains(string(b), `"seed":2`) {
		t.Fatalf("graded runs:\n%s", b)
	}
	var rf RunFile
	must(t, readJSON(filepath.Join(f.root.RunDir("detect-1", "a2"), "run.json"), &rf))
	if rf.Status != StatusInvalid || !strings.HasPrefix(rf.Reason, "host interference") {
		t.Fatalf("interfered run: %+v", rf)
	}
	csv, _ := os.ReadFile(f.root.path("detect-1", "results", "runs.csv"))
	if !strings.Contains(string(csv), "done,,better") || !strings.Contains(string(csv), "capped: reflection queued") {
		t.Fatalf("runs.csv:\n%s", csv)
	}
	if !strings.Contains(string(csv), ",1,0,i1") {
		t.Fatalf("transcript coverage missing from runs.csv:\n%s", csv)
	}
	rep, _ := os.ReadFile(f.root.path("detect-1", "results", "report.md"))
	if !strings.Contains(string(rep), "gate-time false alarms") || !strings.Contains(string(rep), "1 runs") || !strings.Contains(string(rep), "1 done runs with an incident lacking its transcript") {
		t.Fatalf("report:\n%s", rep)
	}
	// A baseline with another run block is refused.
	other := strings.Replace(strings.Replace(exampleSpec, "name: detect-1", "name: noop-1", 1), "churn: false", "churn: true", 1)
	other = strings.Replace(other, "operator:\n  hooks: test\n  revision: 4d2915b3f\n  baseline: base\n  settings: {model: m1, effort: medium}\n", "operator: {builtin: noop}\n", 1)
	must(t, f.add(t, other))
	if err := Results(context.Background(), f.root, "detect-1", []string{"noop-1"}, io.Discard); err == nil || !strings.Contains(err.Error(), "run block") {
		t.Fatalf("incompatible baseline: %v", err)
	}
}

func TestStatusShowsExperimentsAndLanes(t *testing.T) {
	f := newFixture(t)
	must(t, f.add(t, exampleSpec))
	must(t, writeJSON(f.root.LanePath("c1"), LaneFile{Lane: "c1", Host: "h1", State: LaneDown, Reason: "fault still active"}))
	var b strings.Builder
	must(t, Status(f.root, "", &b))
	out := b.String()
	if !strings.Contains(out, "detect-1") || !strings.Contains(out, " 8 ") || !strings.Contains(out, "h1/c1") || !strings.Contains(out, "fault still active") {
		t.Fatalf("status:\n%s", out)
	}
}

func TestStatusOfIsTheSameStatusAsData(t *testing.T) {
	f := newFixture(t)
	must(t, f.add(t, exampleSpec))
	started := time.Now().UTC().Add(-time.Minute)
	must(t, writeJSON(f.root.LanePath("c1"), LaneFile{Lane: "c1", Host: "h1", State: LaneRunning, Experiment: "detect-1",
		Seq: &Seq{Fault: "a@x", Q: 1}, Position: 1, Attempt: 1,
		Step: &StepRef{ID: "s1", Kind: StepRun, RunID: "a-x-q1-r1-a1", Started: started, Phase: &Phase{Name: PhaseWindow, Since: started}}}))
	must(t, writeJSON(f.root.path("hosts.status.json"), map[string]HostStatus{"h1": {At: started, Load5: 12, Builds: 0, Free: 300 << 30}}))
	st, err := StatusOf(f.root, "")
	must(t, err)
	if len(st.Experiments) != 1 || st.Experiments[0].Name != "detect-1" || st.Experiments[0].State != ExpActive || st.Experiments[0].Counts.Running != 1 {
		t.Fatalf("experiments %+v", st.Experiments)
	}
	if len(st.Hosts) != 1 || st.Hosts[0].Name != "h1" || st.Hosts[0].Lanes != 2 || st.Hosts[0].Busy != 1 || st.Hosts[0].Free != 300<<30 {
		t.Fatalf("hosts %+v", st.Hosts)
	}
	if len(st.Lanes) != 1 || st.Lanes[0].Step.Phase.Name != PhaseWindow {
		t.Fatalf("lanes %+v", st.Lanes)
	}
	var b strings.Builder
	must(t, Status(f.root, "", &b))
	if !strings.Contains(b.String(), "window") {
		t.Fatalf("status does not print the phase:\n%s", b.String())
	}
	must(t, Pause(f.root, "detect-1"))
	st, err = StatusOf(f.root, "detect-1")
	must(t, err)
	if st.Experiments[0].State != ExpPaused {
		t.Fatalf("paused experiment: %+v", st.Experiments[0])
	}
}
