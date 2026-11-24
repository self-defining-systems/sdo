//go:build integration

package bench

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The step that asks a held lane's subject again, with a real hook: only a readable "verified" with no reason is one;
// a hook that fails, hangs, is killed, answers something else or does not know the verb it declared never is.
func TestVerifyStepReadsTheSubjectsAnswer(t *testing.T) {
	if testing.Short() {
		t.Skip("real hook processes: nightly tier")
	}
	old := hookTimeouts["verify-evidence"]
	hookTimeouts["verify-evidence"] = 2 * time.Second
	t.Cleanup(func() { hookTimeouts["verify-evidence"] = old })
	for name, tc := range map[string]struct{ script, verified, reason string }{
		"verified":           {`echo progress; echo '{"verified": true}'`, `"verified": true`, ""},
		"missing":            {`echo '{"verified": false, "reasons": ["store x empty"]}'`, `"verified": false`, "store x empty"},
		"verified but":       {`echo '{"verified": true, "reasons": ["z"]}'`, `"verified": false`, "z"},
		"no verdict":         {`echo '{}'`, `"verified": false`, "unreadable answer"},
		"no reason":          {`echo '{"verified": false}'`, `"verified": false`, "no reason given"},
		"not json":           {`echo fine`, `"verified": false`, "unreadable answer"},
		"silent":             {`:`, `"verified": false`, "unreadable answer"},
		"fails":              {`echo '{"verified": true}'; echo broken >&2; exit 3`, `"verified": false`, "exit 3"},
		"killed":             {`kill -9 $$`, `"verified": false`, "verify-evidence"},
		"hangs":              {`sleep 60`, `"verified": false`, "timed out after 2s"},
		"unsupported":        {`echo '{"unsupported": true}'; exit 64`, `"verified": false`, "does not implement it"},
		"a verb it predates": {`echo usage >&2; exit 2`, `"verified": false`, "does not implement it"},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			hook, step := filepath.Join(dir, "hook.sh"), filepath.Join(dir, "c1", "verify-1")
			must(t, os.WriteFile(hook, []byte("[ \"$1\" = verify-evidence ] || exit 64\nenv | sort > "+dir+"/env\n"+tc.script+"\n"), 0o644))
			must(t, os.MkdirAll(filepath.Join(step, "run"), 0o755))
			s := Step{ID: "verify-1", Kind: "verify", Lane: "c1", Fuzzops: testdata("fakefuzzops"), LaneDir: filepath.Join(dir, "lane"),
				Run:  &RunSpec{ID: "r1", Fault: "a@x"},
				Prev: &Prepared{Experiment: "e1", Namespace: "ns", Hooks: "h", HooksPath: dir, Command: "sh " + hook}}
			must(t, writeJSON(filepath.Join(step, "step.json"), s))
			must(t, RunStep(context.Background(), filepath.Join(step, "step.json")))
			if got := readString(filepath.Join(step, "result.json")); !strings.Contains(got, tc.verified) || !strings.Contains(got, tc.reason) {
				t.Fatalf("result %s", got)
			}
			for _, want := range []string{"BENCH_RUN_DIR=" + filepath.Join(step, "run"), "BENCH_EVIDENCE_RECHECK=1", "BENCH_RUN=r1", "BENCH_VERB_LIMIT_SECONDS=2"} {
				if env := readString(filepath.Join(dir, "env")); !strings.Contains(env, want+"\n") {
					t.Errorf("the hook was not given %s", want)
				}
			}
			if files, _ := filepath.Glob(filepath.Join(step, "run", "*")); len(files) != 0 {
				t.Errorf("the platform wrote into the run directory: %v", files)
			}
		})
	}
}
