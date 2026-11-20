//go:build integration

package bench

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func limitRunner(t *testing.T, cores int) *stepRunner {
	t.Helper()
	r := newRunner(t, presentCluster)
	r.topology = func() (Topology, error) { return topo(8, 1), nil }
	r.live = func() (map[string]bool, error) { return nil, errors.New("not known") }
	r.foreign = func() ([]ContainerPin, error) { return nil, nil }
	r.oom = func() (int, error) { return 0, nil }
	r.s.Limits = &StepLimits{Cores: cores, Memory: "2Gi", CPUs: "2-7", State: filepath.Join(t.TempDir(), "state")}
	r.s.New = &Prepared{Experiment: "x"}
	r.s.Fuzzops = t.TempDir()
	return r
}

const printEnv = `printf '%s|%s|%s' "${FUZZOPS_LANE_CPUSET-unset}" "${FUZZOPS_LANE_MEMORY-unset}" "${FUZZOPS_LANE_SLICE-unset}" > `

// The lane's cluster-up gets the cores the host's allocator picked, in bytes, and the shared parent slice.
func TestClusterUpGetsTheLaneLimits(t *testing.T) {
	r := limitRunner(t, 3)
	out := filepath.Join(t.TempDir(), "env")
	r.s.ClusterUp = printEnv + out
	r.makeClusterUp(context.Background())
	b, _ := os.ReadFile(out)
	if got, want := string(b), "2-4|2147483648|eval.slice"; got != want {
		t.Fatalf("env %q, want %q", got, want)
	}
}

// A spec without lane limits (a host without lanes.cpus) builds exactly as before: no variables.
func TestClusterUpWithoutLimitsIsUnchanged(t *testing.T) {
	r := newRunner(t, presentCluster)
	out := filepath.Join(t.TempDir(), "env")
	r.s.ClusterUp = printEnv + out
	r.makeClusterUp(context.Background())
	if b, _ := os.ReadFile(out); string(b) != "unset|unset|unset" {
		t.Fatalf("env %q", b)
	}
}

// With every core of the range held by another lane the build waits, and goes on when they are released.
func TestClusterUpWaitsForCores(t *testing.T) {
	r := limitRunner(t, 3)
	r.s.ClusterUp = "true"
	other := CPUAllocator{Dir: r.s.Limits.State, Allowed: []int{2, 3, 4, 5, 6, 7}, Topo: topo(8, 1)}
	if _, err := other.Take("other", 5, 0); err != nil {
		t.Fatal(err)
	}
	go func() { time.Sleep(60 * time.Millisecond); _ = other.Release("other") }()
	if !r.makeClusterUp(context.Background()) {
		t.Fatalf("did not go on: %q", r.res.Reason)
	}
	held, _ := other.Held()
	if len(held["lane-a"]) != 3 {
		t.Fatalf("held %v", held)
	}
}

// A build that waits too long fails with the reason; it never starts on cores another lane holds.
func TestClusterUpFailsWhenCoresNeverFree(t *testing.T) {
	old := coreWait
	coreWait = 50 * time.Millisecond
	defer func() { coreWait = old }()
	r := limitRunner(t, 3)
	r.s.ClusterUp = "touch ran"
	other := CPUAllocator{Dir: r.s.Limits.State, Allowed: []int{2, 3, 4, 5, 6, 7}, Topo: topo(8, 1)}
	if _, err := other.Take("other", 6, 0); err != nil {
		t.Fatal(err)
	}
	if r.makeClusterUp(context.Background()) || !strings.Contains(r.res.Reason, "no free cores") {
		t.Fatalf("reason %q", r.res.Reason)
	}
	if _, err := os.Stat(filepath.Join(r.s.Fuzzops, "ran")); err == nil {
		t.Fatal("cluster-up ran")
	}
}

func TestClusterUpFailsWhenTheRequestCanNeverFit(t *testing.T) {
	r := limitRunner(t, 9)
	r.s.ClusterUp = "true"
	if r.makeClusterUp(context.Background()) || !strings.Contains(r.res.Reason, "lane.cpus 9") {
		t.Fatalf("reason %q", r.res.Reason)
	}
}

// Deleting the lane's cluster gives its cores back.
func TestReleaseFreesTheLanesCores(t *testing.T) {
	r := limitRunner(t, 3)
	r.s.ClusterUp, r.s.ClusterDown, r.s.DeleteCluster = "true", "true", true
	if !r.makeClusterUp(context.Background()) {
		t.Fatal(r.res.Reason)
	}
	r.release(context.Background())
	if !r.res.Gone {
		t.Fatalf("not released: %q", r.res.Reason)
	}
	a := CPUAllocator{Dir: r.s.Limits.State, Allowed: []int{2, 3, 4, 5, 6, 7}, Topo: topo(8, 1)}
	if held, _ := a.Held(); len(held) != 0 {
		t.Fatalf("still held %v", held)
	}
}

// fakeReserve puts a sudo that logs and runs its arguments in PATH, and (withHelper) a helper at reserveBin that logs its.
func fakeReserve(t *testing.T, withHelper bool) string {
	t.Helper()
	bin, log := t.TempDir(), filepath.Join(t.TempDir(), "calls")
	must := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}
	must(os.WriteFile(filepath.Join(bin, "sudo"), []byte("#!/bin/sh\n[ \"$1\" = -n ] || exit 9\nshift\necho \"$@\" >> "+log+"\n"), 0o755))
	old := reserveBin
	reserveBin = filepath.Join(t.TempDir(), "sdo-eval-reserve")
	if withHelper {
		must(os.WriteFile(reserveBin, []byte("#!/bin/sh\n"), 0o755))
	}
	t.Cleanup(func() { reserveBin = old })
	t.Setenv("PATH", bin+":"+os.Getenv("PATH")) // the helper is found by its absolute path, not through PATH
	return log
}

func TestHelperIsCalledByAbsolutePath(t *testing.T) {
	log := fakeReserve(t, true)
	r := limitRunner(t, 2)
	r.s.Limits.Reserve, r.s.Limits.MemoryMin = true, "8G"
	if !r.reserveCall(context.Background(), true) {
		t.Fatal("not reserved")
	}
	r.reserve(context.Background(), false)
	b, _ := os.ReadFile(log)
	want := reserveBin + " on lane-a --cpus 2-7 --lease 10m --memory-min 8G\n" + reserveBin + " off lane-a\n"
	if string(b) != want || !filepath.IsAbs(reserveBin) {
		t.Fatalf("calls %q, want %q", b, want)
	}
}

// The production path is the one the sudoers rule names: no PATH lookup (a non-login ssh command's PATH has no sbin).
func TestHelperPathIsAbsoluteSbin(t *testing.T) {
	if reserveBin != "/usr/local/sbin/sdo-eval-reserve" {
		t.Fatal(reserveBin)
	}
}

func TestReserveWithoutHelperSaysSo(t *testing.T) {
	fakeReserve(t, false)
	var buf strings.Builder
	r := limitRunner(t, 2)
	r.s.Limits.Reserve = true
	r.log = &buf
	if r.reserveCall(context.Background(), true) {
		t.Fatal("reserved without a helper")
	}
	if !strings.Contains(buf.String(), "host not reserved") {
		t.Fatalf("log %q", buf.String())
	}
}

func TestNoReserveWhenNotConfigured(t *testing.T) {
	log := fakeReserve(t, true)
	r := limitRunner(t, 2)
	if r.reserveCall(context.Background(), true) {
		t.Fatal("reserved although lanes.reserve is off")
	}
	if _, err := os.Stat(log); err == nil {
		t.Fatal("helper called although lanes.reserve is off")
	}
}

// A run keeps its lease while it lasts (a renewal every beat), gives the host back at its end, and records what it was
// measured under.
func TestRunStepRenewsItsLeaseAndRecordsIsolation(t *testing.T) {
	log := fakeReserve(t, true)
	old := reserveBeat
	reserveBeat = 20 * time.Millisecond
	defer func() { reserveBeat = old }()
	r := limitRunner(t, 3)
	r.s.Limits.Reserve = true
	a, _ := r.allocator()
	if _, err := a.Take("lane-a", 3, 2<<30); err != nil {
		t.Fatal(err)
	}
	finish := r.isolate(context.Background())
	time.Sleep(150 * time.Millisecond)
	finish()
	b, _ := os.ReadFile(log)
	calls := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(calls) < 4 || !strings.Contains(calls[0], " on lane-a ") || !strings.Contains(calls[len(calls)-1], " off lane-a") {
		t.Fatalf("calls: %q", calls)
	}
	for _, c := range calls[1 : len(calls)-1] {
		if !strings.Contains(c, " on lane-a ") {
			t.Fatalf("a renewal is an `on`: %q", c)
		}
	}
	iso := r.res.Isolation
	if iso == nil || !iso.Reserved || iso.CPUs != "2-4" || iso.Memory != "2Gi" || iso.OOMKills != 0 {
		t.Fatalf("isolation %+v", iso)
	}
}

func TestRunWithoutHelperRecordsNotReserved(t *testing.T) {
	fakeReserve(t, false)
	r := limitRunner(t, 3)
	r.s.Limits.Reserve = true
	r.isolate(context.Background())()
	if iso := r.res.Isolation; iso == nil || iso.Reserved {
		t.Fatalf("isolation %+v", iso)
	}
}

// Memory-limit kills in the lane's nodes during the run are counted and flagged.
func TestRunRecordsOOMKills(t *testing.T) {
	r := limitRunner(t, 2)
	kills := []int{3, 5}
	r.oom = func() (int, error) { k := kills[0]; kills = kills[1:]; return k, nil }
	r.isolate(context.Background())()
	if iso := r.res.Isolation; iso == nil || iso.OOMKills != 2 {
		t.Fatalf("isolation %+v", iso)
	}
	// and the manager writes it into run.json
	rf := RunFile{Isolation: r.res.Isolation}
	if rf.Isolation.OOMKills != 2 {
		t.Fatal(rf)
	}
}

// A host without lane limits records nothing.
func TestNoIsolationRecordWithoutLimits(t *testing.T) {
	r := newRunner(t, presentCluster)
	r.isolate(context.Background())()
	if r.res.Isolation != nil {
		t.Fatalf("%+v", r.res.Isolation)
	}
}

// A lane whose cluster is gone does not keep the build waiting: the ledger is reconciled with the clusters that exist.
func TestBuildReclaimsCoresOfADeadLane(t *testing.T) {
	r := limitRunner(t, 3)
	r.s.ClusterUp = "true"
	other := CPUAllocator{Dir: r.s.Limits.State, Allowed: []int{2, 3, 4, 5, 6, 7}, Topo: topo(8, 1)}
	if _, err := other.Take("dead", 6, 0); err != nil {
		t.Fatal(err)
	}
	if err := other.MarkCreated("dead"); err != nil {
		t.Fatal(err)
	}
	r.live = func() (map[string]bool, error) { return map[string]bool{"lane-a": true}, nil }
	if !r.makeClusterUp(context.Background()) {
		t.Fatalf("the dead lane's cores were not reclaimed: %q", r.res.Reason)
	}
	if held, _ := other.Held(); held["dead"] != nil || len(held["lane-a"]) != 3 {
		t.Fatalf("held %v", held)
	}
}

// A failed cluster-up that left no cluster gives its cores back at once; a successful one marks the lane created.
func TestFailedClusterUpFreesItsCores(t *testing.T) {
	r := limitRunner(t, 3)
	r.s.ClusterUp = "exit 1"
	r.exists = missingCluster
	if r.makeClusterUp(context.Background()) {
		t.Fatal("cluster-up should fail")
	}
	a, _ := r.allocator()
	if held, _ := a.Held(); len(held) != 0 {
		t.Fatalf("held after a failed cluster-up: %v", held)
	}
	r.s.ClusterUp = "true"
	if !r.makeClusterUp(context.Background()) {
		t.Fatal(r.res.Reason)
	}
	es, _ := a.Entries()
	if !es["lane-a"].Created {
		t.Fatalf("the ledger does not say the cluster exists: %+v", es)
	}
}

// A claim or reuse re-applies the lane's limits from the ledger (the systemd cpuset property may have been undone).
func TestReapplyLimitsUsesTheLedger(t *testing.T) {
	r := limitRunner(t, 3)
	a, _ := r.allocator()
	if _, err := a.Take("lane-a", 3, 3<<30); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(r.s.Fuzzops, "deploy"), 0o755); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "applied")
	script := "#!/bin/sh\necho \"$@|$FUZZOPS_LANE_CPUSET|$FUZZOPS_LANE_MEMORY|$FUZZOPS_LANE_SLICE\" > " + out + "\n"
	if err := os.WriteFile(filepath.Join(r.s.Fuzzops, "deploy", "lane-limits.sh"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	r.reapplyLimits(context.Background())
	if b, _ := os.ReadFile(out); string(b) != "apply lane-a|2-4|3221225472|eval.slice\n" {
		t.Fatalf("applied %q", b)
	}
	r2 := newRunner(t, presentCluster) // no limits: nothing to apply
	r2.reapplyLimits(context.Background())
}

// A lane waiting for the memory budget goes on when it is there.
func TestClusterUpWaitsForMemoryBudget(t *testing.T) {
	r := limitRunner(t, 1)
	r.s.Limits.MemBudget = "5Gi"
	r.s.ClusterUp = "true"
	other := CPUAllocator{Dir: r.s.Limits.State, Allowed: []int{2, 3, 4, 5, 6, 7}, Topo: topo(8, 1), MemBudget: 5 << 30}
	if _, err := other.Take("other", 1, 4<<30); err != nil {
		t.Fatal(err)
	}
	// While it waits, its phase says so and why (status shows it).
	waitPhase := make(chan Phase, 1)
	go func() {
		var p Phase
		for end := time.Now().Add(time.Minute); time.Now().Before(end); time.Sleep(5 * time.Millisecond) {
			if readJSON(filepath.Join(r.dir, phaseFile), &p) == nil && p.Name == PhaseLaneWait {
				break
			}
		}
		waitPhase <- p
		_ = other.Release("other")
	}()
	if !r.makeClusterUp(context.Background()) {
		t.Fatalf("did not go on: %q", r.res.Reason)
	}
	if p := <-waitPhase; !strings.Contains(p.Detail, "memory") {
		t.Fatalf("wait phase %+v", p)
	}
}
