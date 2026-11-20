//go:build integration

package deploy_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The set of slices that must exclude the block is every top-level *.slice cgroup except lanes.slice, computed at each
// check: another team's sdo.slice or eval.slice that appears later is covered, named when it is not, and fixed by gc.
func TestEverySiblingSliceMustExcludeTheBlock(t *testing.T) {
	b := blockSetup(t)
	b.must("block", "reserve", "--cpus", "40-55", "--memory", "8G", "--host-reserve-cpus", "2")
	b.cgroup("sdo.slice", "cpuset.cpus.effective", "0-55")
	b.cgroup("team.slice", "cpuset.cpus.effective", "0-55")
	st := b.must("block", "status")
	for _, want := range []string{"two-way no", "unexcluded sdo.slice", "unexcluded team.slice"} {
		if !strings.Contains(st, want) {
			t.Fatalf("status must name exactly the slices that are not excluded; lacks %q:\n%s", want, st)
		}
	}
	if strings.Contains(st, "unexcluded user.slice") || strings.Contains(st, "unexcluded lanes.slice") {
		t.Fatalf("slices that are excluded, and lanes.slice, are not named:\n%s", st)
	}
	b.must("gc")
	for _, s := range []string{"sdo.slice", "team.slice"} {
		if got := cgfile(t, filepath.Join(b.cg, s), "cpuset.cpus"); got != "0-39" {
			t.Errorf("%s: gc must re-apply the reservation to a new sibling: cpuset.cpus %q", s, got)
		}
	}
	// the files stand in for the kernel's effective cpus
	b.cgroup("sdo.slice", "cpuset.cpus.effective", "0-39")
	b.cgroup("team.slice", "cpuset.cpus.effective", "0-39")
	if st := b.must("block", "status"); !strings.Contains(st, "two-way yes") {
		t.Fatalf("once every sibling excludes the block:\n%s", st)
	}
}

// A slice that holds another team's container pinned inside the block must never be shrunk (that changes the container's
// effective cpuset). It is reported by name, the host stays one-way, and nothing else is touched.
func TestASliceWithAContainerPinnedInsideTheBlockIsNeverShrunk(t *testing.T) {
	b := blockSetup(t)
	b.must("block", "reserve", "--cpus", "40-55", "--memory", "8G", "--host-reserve-cpus", "2")
	b.cgroup("team.slice", "cpuset.cpus.effective", "0-55")
	b.cgroup("team.slice", "cpuset.cpus", "0-55")
	pin := filepath.Join(b.cg, "team.slice", "docker-abc.scope")
	os.MkdirAll(pin, 0o755)
	os.WriteFile(filepath.Join(pin, "cpuset.cpus"), []byte("50-52\n"), 0o644)
	b.cgroup("sdo.slice", "cpuset.cpus.effective", "0-55")
	b.must("gc")
	if got := cgfile(t, filepath.Join(b.cg, "team.slice"), "cpuset.cpus"); got != "0-55" {
		t.Fatalf("a slice holding a container pinned inside the block was shrunk: %q", got)
	}
	if got := cgfile(t, filepath.Join(b.cg, "sdo.slice"), "cpuset.cpus"); got != "0-39" {
		t.Errorf("the other new sibling is still fixed: %q", got)
	}
	st := b.must("block", "status")
	if !strings.Contains(st, "foreign-pinned team.slice docker-abc.scope") || !strings.Contains(st, "two-way no") {
		t.Fatalf("status must say foreign pinned inside block in team.slice:\n%s", st)
	}
	if strings.Contains(st, "unexcluded team.slice") {
		t.Fatalf("a foreign-pinned slice is reported as that, not as merely unexcluded:\n%s", st)
	}
}
