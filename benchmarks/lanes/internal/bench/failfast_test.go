//go:build integration

package bench

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/self-defining-systems/sdo/benchmarks/lanes/internal/lanelock"
)

// fakeFuzzops puts a bin/fuzzops in a fresh directory: sh runs for a `recover` (and any other verb but version,
// which says it is the pinned revision). Every call is appended to calls.
func fakeFuzzops(t *testing.T, recoverBody string) (dir, calls string) {
	t.Helper()
	dir = t.TempDir()
	calls = filepath.Join(dir, "calls")
	script := fmt.Sprintf(`#!/bin/sh
echo "$*" >> %q
case "$1" in
version) echo '{"revision":"abc","modified":false,"measure_version":1}' ;;
recover) %s ;;
esac
`, calls, recoverBody)
	if err := os.MkdirAll(filepath.Join(dir, "bin"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "bin", "fuzzops"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return dir, calls
}

func callCount(t *testing.T, calls, verb string) int {
	t.Helper()
	b, _ := os.ReadFile(calls)
	n := 0
	for _, l := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(l, verb) {
			n++
		}
	}
	return n
}

// A recover that exits with the permanent-error code (the pack does not load) cannot succeed on a retry: the step
// fails at once with fuzzops' reason, and the lane is not taken down for a fault that was never looked at.
func TestRecoverFailsAtOnceOnAPermanentError(t *testing.T) {
	r := newRunner(t, presentCluster)
	r.poll = time.Hour // a retry would sleep this long
	var calls string
	r.s.Fuzzops, calls = fakeFuzzops(t, fmt.Sprintf(`echo "pack /x/y: no app.yaml or app.json" >&2; exit %d`, lanelock.ExitInvalid))
	p := &Prepared{Namespace: "ns", AppPack: "/x/y"}
	start := time.Now()
	ok := r.recoverLane(context.Background(), p)
	if ok || time.Since(start) > 5*time.Second {
		t.Fatalf("ok=%v after %v, want a failure at once", ok, time.Since(start))
	}
	if n := callCount(t, calls, "recover"); n != 1 {
		t.Errorf("recover ran %d times, want 1", n)
	}
	if !strings.Contains(r.res.Reason, "no app.yaml or app.json") || !strings.Contains(r.res.Reason, "/x/y") {
		t.Errorf("reason does not carry fuzzops' error: %q", r.res.Reason)
	}
	if r.res.LaneDown != "" || r.res.Config == "" {
		t.Errorf("a pack that does not load is a spec problem, not a lane one: down=%q config=%q", r.res.LaneDown, r.res.Config)
	}
}

// A lane another fuzzops holds is transient: the step keeps waiting and retrying, and succeeds when it frees.
func TestRecoverKeepsRetryingABusyLane(t *testing.T) {
	r := newRunner(t, presentCluster)
	dir, calls := fakeFuzzops(t, fmt.Sprintf(`n=$(grep -c '^recover' %q); if [ "$n" -lt 3 ]; then echo "lane busy" >&2; exit %d; fi`, "CALLS", lanelock.ExitBusy))
	// the script reads its own call log
	b, _ := os.ReadFile(filepath.Join(dir, "bin", "fuzzops"))
	if err := os.WriteFile(filepath.Join(dir, "bin", "fuzzops"), []byte(strings.ReplaceAll(string(b), "CALLS", calls)), 0o755); err != nil {
		t.Fatal(err)
	}
	r.s.Fuzzops = dir
	if ok := r.recoverLane(context.Background(), &Prepared{Namespace: "ns", AppPack: "/x/y"}); !ok {
		t.Fatalf("busy lane that frees: not ok, %q", r.res.Reason)
	}
	if n := callCount(t, calls, "recover"); n != 3 {
		t.Errorf("recover ran %d times, want 3", n)
	}
}

// A recover that fails for another reason (exit 1: the undo failed, the fault may still be in place) takes the lane
// down at once, as before.
func TestRecoverThatCouldNotUndoTakesTheLaneDown(t *testing.T) {
	r := newRunner(t, presentCluster)
	r.s.Fuzzops, _ = fakeFuzzops(t, `echo "undo failed" >&2; exit 1`)
	if ok := r.recoverLane(context.Background(), &Prepared{Namespace: "ns", AppPack: "/x/y"}); ok || r.res.LaneDown == "" {
		t.Errorf("ok=%v down=%q", ok, r.res.LaneDown)
	}
}

// A build whose spec points at a pack that is not there fails before it makes a cluster, in seconds, naming the
// path; it neither recreates the lane nor counts against it (the reason is the spec's).
func TestBuildWithAMissingPackFailsBeforeMakingACluster(t *testing.T) {
	r := newRunner(t, missingCluster)
	var calls string
	r.s.Fuzzops, calls = fakeFuzzops(t, "exit 0")
	// `pack load` is answered like the real command does for a missing pack.
	script := fmt.Sprintf(`#!/bin/sh
echo "$*" >> %q
case "$1" in
version) echo '{"revision":"abc","modified":false,"measure_version":1}' ;;
pack) echo "pack $4: no app.yaml or app.json" >&2; exit %d ;;
esac
`, calls, lanelock.ExitInvalid)
	if err := os.WriteFile(filepath.Join(r.s.Fuzzops, "bin", "fuzzops"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(t.TempDir(), "cluster-made")
	r.s.ClusterUp = "touch " + marker
	r.s.Create = true
	r.s.New = &Prepared{Experiment: "y", Namespace: "ns", AppPack: "/pin/mnt/data/hardroot/examples/gitlab"}
	start := time.Now()
	r.build(context.Background())
	if time.Since(start) > 5*time.Second {
		t.Errorf("took %v", time.Since(start))
	}
	if _, err := os.Stat(marker); err == nil {
		t.Error("a cluster was made for a build that cannot succeed")
	}
	if r.res.OK || r.res.Config == "" || r.res.LaneDown != "" {
		t.Errorf("ok=%v config=%q down=%q", r.res.OK, r.res.Config, r.res.LaneDown)
	}
	if !strings.Contains(r.res.Reason, "/pin/mnt/data/hardroot/examples/gitlab") || !strings.Contains(r.res.Reason, "no app.yaml or app.json") {
		t.Errorf("reason %q does not name the pack and why", r.res.Reason)
	}
}

// A pinned fuzzops that predates `pack load` (it prints usage, exit 2) cannot tell: the build goes on as before.
func TestBuildPreflightSkipsAFuzzopsWithoutPackLoad(t *testing.T) {
	r := newRunner(t, missingCluster)
	var calls string
	r.s.Fuzzops, calls = fakeFuzzops(t, "exit 0")
	script := fmt.Sprintf("#!/bin/sh\necho \"$*\" >> %q\n[ \"$1\" = pack ] && { echo usage >&2; exit 2; }\nexit 0\n", calls)
	if err := os.WriteFile(filepath.Join(r.s.Fuzzops, "bin", "fuzzops"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	r.s.ClusterUp = "exit 1" // the cluster creation is reached and ends the test
	r.s.Create = true
	r.s.New = &Prepared{Experiment: "y", Namespace: "ns", AppPack: "/x/y"}
	r.build(context.Background())
	if !strings.Contains(r.res.Reason, "cluster-up") || r.res.Config != "" {
		t.Errorf("want the cluster-up failure, got config=%q reason=%q", r.res.Config, r.res.Reason)
	}
}

// A recover that fails because the lane's cluster does not exist (a build killed before its cluster was made) has no
// fault to undo: the lane is not taken down for a fault that cannot be in place.
func TestRecoverOfAMissingClusterDoesNotTakeTheLaneDown(t *testing.T) {
	r := newRunner(t, missingCluster)
	r.s.Fuzzops, _ = fakeFuzzops(t, `echo "namespace not found" >&2; exit 1`)
	if ok := r.recoverLane(context.Background(), &Prepared{Namespace: "ns", AppPack: "/x/y"}); !ok || r.res.LaneDown != "" {
		t.Errorf("ok=%v down=%q, want a lane that stays up (no cluster, no fault)", ok, r.res.LaneDown)
	}
}
