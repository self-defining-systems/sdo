//go:build integration

package bench

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const exampleSpec = `# one wave of two faults
name: detect-1
app: examples/shop
fuzzops: 0b528ac
operator:
  hooks: test
  revision: 4d2915b3f
  baseline: base
  settings: {model: m1, effort: medium}
namespace: app1
faults:
  instances: instances.json
  ids:
    - a@x
    - b@y
    # - healthy
sequences: [[1, 2, 2], [3]]
run: {window: 330s, early-stop: false, churn: false}
ready_for: 2m
ready_timeout: 20m
cap: 25m
lanes: 2
hosts: [h1]
retries: 1
pause_after_wave: true
`

func writeSpec(t *testing.T, dir, body string) string {
	t.Helper()
	p := filepath.Join(dir, "spec.yaml")
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestSpecExampleLoads(t *testing.T) {
	s, err := LoadSpec(writeSpec(t, t.TempDir(), exampleSpec))
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Sequences) != 2 || len(s.Sequences[0]) != 3 || s.Sequences[0][1] != 2 || s.Operator.Settings["model"] != "m1" || !s.PauseAfterWave {
		t.Fatalf("%+v", s)
	}
	if got := strings.Join(s.RunFlags(), " "); got != "--churn=false --early-stop=false --window=330s" {
		t.Fatalf("run flags %q", got)
	}
	if s.OperatorLabel(2) != "exec:test-detect-1-r2" {
		t.Fatal(s.OperatorLabel(2))
	}
	if len(s.Seqs()) != 4 {
		t.Fatalf("seqs %v", s.Seqs())
	}
}

func TestSpecRules(t *testing.T) {
	for _, c := range []struct{ edit, want string }{
		{"name: Detect_1", "name"},
		{"fuzzops: main", "fuzzops"},
		{"operator: {hooks: test, builtin: noop}", "exactly one of hooks and builtin"},
		{"operator: {builtin: noop, revision: abcdef1}", "takes no revision"},
		{"operator: {hooks: test}", "operator.revision"},
		{"sequences: []", "sequences is empty"},
		{"sequences: [[0]]", "seeds start at 1"},
		{"run: {seed: 3}", "run.seed is set by the bench"},
		{"ready_for: 30m", "ready_timeout must be at least ready_for"},
		{"cap: soon", "cap"},
		{"lanes: 0", "lanes"},
		{"hosts: []", "hosts is empty"},
	} {
		key, _, _ := strings.Cut(c.edit, ":")
		var lines []string
		skip := false
		for _, l := range strings.Split(exampleSpec, "\n") {
			if skip && strings.HasPrefix(l, "  ") {
				continue
			}
			skip = false
			if strings.HasPrefix(l, key+":") {
				l, skip = c.edit, true
			}
			lines = append(lines, l)
		}
		_, err := LoadSpec(writeSpec(t, t.TempDir(), strings.Join(lines, "\n")))
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: err %v, want %q", c.edit, err, c.want)
		}
	}
}

// A spec without ready_for, ready_timeout or cap is refused: no default is guessed.
func TestSpecNeedsItsTimes(t *testing.T) {
	for _, k := range []string{"ready_for", "ready_timeout", "cap"} {
		var lines []string
		for _, l := range strings.Split(exampleSpec, "\n") {
			if !strings.HasPrefix(l, k+":") {
				lines = append(lines, l)
			}
		}
		if _, err := LoadSpec(writeSpec(t, t.TempDir(), strings.Join(lines, "\n"))); err == nil || !strings.Contains(err.Error(), k+" is required") {
			t.Errorf("%s: %v", k, err)
		}
	}
}

func TestSlugIsReadableAndUnique(t *testing.T) {
	a, b := Slug("gitaly-auth@dependency:sts/x:8075"), Slug("gitaly-auth@dependency:sts/x:8076")
	if a == b || !strings.HasPrefix(a, "gitaly-auth-dependency-sts-x-8075-") || Slug(Healthy) != Healthy {
		t.Fatalf("%s %s", a, b)
	}
	if RunID("a@x", 2, 3, 1) != Slug("a@x")+"-q2-r3-a1" {
		t.Fatal(RunID("a@x", 2, 3, 1))
	}
}

func TestParseSizeAndClusterCommand(t *testing.T) {
	for in, want := range map[string]int64{"": 0, "150G": 150 << 30, "1.5T": 3 << 39, "500m": 500 << 20, "2048": 2048} {
		if got, err := ParseSize(in); err != nil || got != want {
			t.Errorf("%q: %d %v", in, got, err)
		}
	}
	if _, err := ParseSize("lots"); err == nil {
		t.Error("lots parsed")
	}
	h := Host{Kubeconfigs: "/k", ClusterUp: "deploy/kind-lane.sh up {cluster} {kubeconfig}"}
	if got := h.ClusterCommand(h.ClusterUp, "c1"); got != "deploy/kind-lane.sh up 'c1' '/k/c1'" {
		t.Fatal(got)
	}
}

// A failed hook is reported by the end of what it wrote (the last line last), with its exit code.
func TestHookWhyEndsWithTheLastLine(t *testing.T) {
	r := HookResult{Exit: 1, First: "render identical", Output: "render identical\nnamespace created\nerror: reset not acknowledged\n"}
	if got := r.Why("prepare"); got != "prepare exit 1: render identical | namespace created | error: reset not acknowledged" {
		t.Fatal(got)
	}
}
