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

func missingCluster(context.Context) (bool, error) { return false, nil }
func presentCluster(context.Context) (bool, error) { return true, nil }

func newRunner(t *testing.T, exists func(context.Context) (bool, error)) *stepRunner {
	t.Helper()
	dir := t.TempDir()
	r := &stepRunner{dir: dir, log: io.Discard, hookLog: io.Discard, poll: time.Millisecond, exists: exists}
	r.s.Lane, r.s.ClusterUp = "lane-a", "true"
	r.s.LaneDir = filepath.Join(dir, "lane")
	return r
}

// A release of a lane whose cluster is gone is already done: it must not wait on an operator that is not there.
func TestReleaseOfAMissingClusterIsAlreadyReleased(t *testing.T) {
	for _, del := range []bool{false, true} {
		r := newRunner(t, missingCluster)
		r.s.ClusterUp, r.s.ClusterDown, r.s.DeleteCluster = "true", "false", del
		prev := &Prepared{Experiment: "x", Hooks: "h", HooksPath: "/nonexistent", Ready: true, Golden: true}
		r.s.Prev, r.res.Prepared = prev, prev
		r.release(context.Background())
		if !r.res.OK || r.res.Prepared != nil || !r.res.Gone || r.res.LaneDown != "" {
			t.Errorf("delete=%v: ok=%v prepared=%v gone=%v down=%q", del, r.res.OK, r.res.Prepared, r.res.Gone, r.res.LaneDown)
		}
	}
}

// A build whose lane file says an app is installed, on a cluster that is gone, creates the cluster instead of
// reusing, swapping into or tearing down what is not there.
func TestBuildOfAMissingClusterStartsFromNothing(t *testing.T) {
	r := newRunner(t, missingCluster)
	r.s.ClusterUp = "exit 1" // the creation is reached, and stops the build here
	prev := &Prepared{Experiment: "x", Hooks: "h", HooksPath: "/nonexistent", Ready: true}
	r.s.Prev, r.s.Swap, r.s.ReuseApp, r.s.Recreate = prev, true, true, true
	r.s.New = &Prepared{Experiment: "y"}
	r.res.Prepared = prev
	r.build(context.Background())
	if r.s.Create != true || r.s.Prev != nil || r.s.Swap || r.s.ReuseApp || r.s.Recreate {
		t.Errorf("step not reset: %+v", r.s)
	}
	if r.res.OK || !strings.Contains(r.res.Reason, "cluster-up") {
		t.Errorf("want the cluster-up failure, got ok=%v %q", r.res.OK, r.res.Reason)
	}
	if r.res.Prepared != nil {
		t.Errorf("a failed cluster-up must clear the prepared record, got %+v", r.res.Prepared)
	}
}

// A cluster-up that fails drops the record even when the cluster was believed to exist (a recreate).
func TestFailedClusterUpClearsPrepared(t *testing.T) {
	r := newRunner(t, presentCluster)
	r.s.ClusterUp, r.s.ClusterDown, r.s.Recreate = "exit 1", "true", true
	r.s.New = &Prepared{Experiment: "y"}
	r.res.Prepared = &Prepared{Experiment: "x"}
	if r.makeCluster(context.Background(), r.s.New) || r.res.Prepared != nil {
		t.Errorf("prepared must be nil after a failed cluster-up, got %+v", r.res.Prepared)
	}
}

// Waiting for an idle operator ends with a clear error, at once, when the cluster is gone.
func TestWaitIdleStopsWhenTheClusterVanishes(t *testing.T) {
	dir := t.TempDir()
	hook := filepath.Join(dir, "hook.sh")
	if err := os.WriteFile(hook, []byte("#!/bin/sh\nexit 1\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	r := newRunner(t, missingCluster)
	r.s.Cap = Seconds(600)
	p := &Prepared{Hooks: "t", HooksPath: dir, Command: hook, Ready: true}
	start := time.Now()
	if r.waitIdle(context.Background(), p) {
		t.Error("waitIdle must report failure")
	}
	if time.Since(start) > 30*time.Second || !strings.Contains(r.res.LaneDown, "no longer exists") {
		t.Errorf("took %s, down=%q", time.Since(start), r.res.LaneDown)
	}
}

// A host that does not make its clusters is not probed.
func TestHostWithoutClusterUpIsNotProbed(t *testing.T) {
	r := newRunner(t, func(context.Context) (bool, error) { t.Error("probed"); return false, nil })
	r.s.ClusterUp = ""
	if r.distrustPrepared(context.Background()) {
		t.Error("must trust the record")
	}
}

// A probe that cannot tell (busy API, slow docker) never drops a lane's record or takes it down.
func TestInconclusiveProbeCountsAsPresent(t *testing.T) {
	r := newRunner(t, func(context.Context) (bool, error) { return false, io.ErrUnexpectedEOF })
	if r.distrustPrepared(context.Background()) || r.clusterVanished(context.Background(), "x") {
		t.Error("an inconclusive probe must not count as a missing cluster")
	}
}

func TestLaneResetClearsPrepared(t *testing.T) {
	f := newFixture(t)
	lf := LaneFile{Lane: "l1", Host: "local", State: LaneDown, Reason: "x", Prepared: &Prepared{Experiment: "e", Namespace: "ns"}}
	if err := writeJSON(f.root.LanePath("l1"), lf); err != nil {
		t.Fatal(err)
	}
	if err := LaneReset(f.root, "l1"); err != nil {
		t.Fatal(err)
	}
	var got LaneFile
	if err := readJSON(f.root.LanePath("l1"), &got); err != nil {
		t.Fatal(err)
	}
	if got.State != LaneFree || got.Prepared != nil {
		t.Errorf("state=%s prepared=%+v", got.State, got.Prepared)
	}
}

// A host's cluster_up is a workload/operator command: it runs in the pinned workload checkout, so a checkout-relative
// script is found, and it learns where the lanes install and the checkout are.
func TestHostClusterUpRunsInTheWorkloadCheckout(t *testing.T) {
	r := newRunner(t, presentCluster)
	r.s.Fuzzops = t.TempDir()
	r.s.Platform = t.TempDir()
	if err := os.MkdirAll(filepath.Join(r.s.Fuzzops, "wl", "lanes"), 0o755); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "env")
	script := "#!/bin/sh\nprintf '%s|%s|%s' \"$1\" \"$LANES_INSTALL\" \"$BENCH_WORKLOAD_DIR\" > " + out + "\n"
	if err := os.WriteFile(filepath.Join(r.s.Fuzzops, "wl", "lanes", "cluster.sh"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	if !r.shell(context.Background(), "cluster-up", "wl/lanes/cluster.sh up", nil) {
		t.Fatal("a checkout-relative host command did not run")
	}
	b, _ := os.ReadFile(out)
	if want := "up|" + r.s.Platform + "|" + r.s.Fuzzops; string(b) != want {
		t.Fatalf("got %q, want %q", b, want)
	}
}
