//go:build integration

package deploy_test

// The helper's persistent lane block (`lane-reserve block ...`, `lane-exec`) against a fake systemctl and a fake cgroupfs
// under a temporary directory. The real helper is never run here: it needs root, and the tests only use its test hooks.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fakeSystemctl keeps each slice's AllowedCPUs in $SC_STATE/<slice>; a slice with no file does not exist.
const fakeSystemctl = `#!/bin/sh
echo "systemctl $*" >> "$SC_STATE/log"
case "$1" in
show)
  case "$4" in
  LoadState) [ -e "$SC_STATE/$2" ] && echo loaded || echo not-found ;;
  AllowedCPUs) cat "$SC_STATE/$2" 2>/dev/null || true ;;
  esac ;;
set-property)
  s=$2; shift 2
  for kv in "$@"; do case "$kv" in AllowedCPUs=*) printf '%s\n' "${kv#AllowedCPUs=}" > "$SC_STATE/$s" ;; esac; done ;;
start) : > "$SC_STATE/$2" ;;
esac
exit 0
`

type blockEnv struct {
	t     *testing.T
	dir   string
	vars  []string
	state string // the helper's state directory
	sc    string // the fake systemctl's state
	cg    string // the fake cgroupfs
}

func blockSetup(t *testing.T) *blockEnv {
	t.Helper()
	if os.Geteuid() == 0 {
		t.Skip("the helper ignores its test hooks as root")
	}
	dir := t.TempDir()
	b := &blockEnv{t: t, dir: dir, state: filepath.Join(dir, "state"), sc: filepath.Join(dir, "sc"), cg: filepath.Join(dir, "cg")}
	for _, d := range []string{b.sc, b.cg, filepath.Join(dir, "bin"), filepath.Join(dir, "sys", "node", "node1"), filepath.Join(b.cg, "lanes.slice")} {
		os.MkdirAll(d, 0o755)
	}
	write := func(p, s string) { os.WriteFile(p, []byte(s), 0o644) }
	write(filepath.Join(dir, "online"), "0-55\n")
	write(filepath.Join(dir, "meminfo"), "MemTotal:       188743680 kB\n")
	write(filepath.Join(b.sc, "user.slice"), "\n")
	write(filepath.Join(b.sc, "system.slice"), "\n")
	write(filepath.Join(b.sc, "machine.slice"), "0-3\n") // somebody had set this one by hand
	write(filepath.Join(dir, "sys", "node", "node1", "cpulist"), "4-7\n")
	for c, sib := range map[string]string{"4": "4,6", "5": "5,7", "6": "4,6", "7": "5,7"} {
		os.MkdirAll(filepath.Join(dir, "sys", "cpu", "cpu"+c, "topology"), 0o755)
		write(filepath.Join(dir, "sys", "cpu", "cpu"+c, "topology", "thread_siblings_list"), sib+"\n")
	}
	sc := filepath.Join(dir, "bin", "systemctl")
	os.WriteFile(sc, []byte(fakeSystemctl), 0o755)
	b.vars = append(os.Environ(), "SC_STATE="+b.sc, "SDO_EVAL_CGROUP_ROOT="+b.cg, "SDO_EVAL_STATE="+b.state, "SDO_EVAL_ONLINE="+filepath.Join(dir, "online"),
		"SDO_EVAL_MEMINFO="+filepath.Join(dir, "meminfo"), "SDO_EVAL_SYSTEMCTL="+sc, "SDO_EVAL_SYSFS="+filepath.Join(dir, "sys"))
	return b
}

func (b *blockEnv) do(args ...string) (string, error) {
	e := &env{t: b.t}
	return e.run(b.vars, append([]string{"host/lane-reserve"}, args...)...)
}

func (b *blockEnv) must(args ...string) string {
	b.t.Helper()
	out, err := b.do(args...)
	if err != nil {
		b.t.Fatalf("%v: %v\n%s", args, err, out)
	}
	return out
}

func (b *blockEnv) slice(name string) string {
	d, _ := os.ReadFile(filepath.Join(b.sc, name))
	return strings.TrimSpace(string(d))
}

func (b *blockEnv) sysLog() string { d, _ := os.ReadFile(filepath.Join(b.sc, "log")); return string(d) }

func TestBlockReserveShrinksTheOtherSlicesAndUnreserveRestoresThem(t *testing.T) {
	b := blockSetup(t)
	out := b.must("block", "reserve", "--cpus", "40-55", "--memory", "8G", "--host-reserve-cpus", "2")
	for _, s := range []string{"user.slice", "system.slice", "machine.slice"} {
		if got := b.slice(s); got != "0-39" {
			t.Errorf("%s AllowedCPUs %q, want the complement 0-39", s, got)
		}
	}
	if got := b.slice("lanes.slice"); got != "40-55" {
		t.Errorf("lanes.slice %q", got)
	}
	if _, err := os.Stat(filepath.Join(b.sc, "kubepods.slice")); err == nil {
		t.Error("a slice that does not exist was created")
	}
	for _, want := range []string{"before:", "after:", "two-way yes", "block 40-55"} {
		if !strings.Contains(out, want) {
			t.Errorf("output lacks %q:\n%s", want, out)
		}
	}
	// idempotent: the same block again sets nothing and keeps the first saved values
	b.must("block", "reserve", "--cpus", "40-55", "--memory", "8G", "--host-reserve-cpus", "2")
	if n := strings.Count(b.sysLog(), "set-property user.slice"); n != 1 {
		t.Errorf("user.slice was set %d times, want 1:\n%s", n, b.sysLog())
	}
	if st := b.must("block", "status"); !strings.Contains(st, "block 40-55") || !strings.Contains(st, "two-way yes") {
		t.Errorf("status:\n%s", st)
	}
	b.must("block", "unreserve")
	if got := b.slice("machine.slice"); got != "0-3" {
		t.Errorf("machine.slice not restored: %q", got)
	}
	if got := b.slice("user.slice"); got != "" {
		t.Errorf("user.slice not restored: %q", got)
	}
	if st := b.must("block", "status"); !strings.Contains(st, "block -") || !strings.Contains(st, "two-way no") {
		t.Errorf("status after unreserve:\n%s", st)
	}
	if _, err := b.do("block", "unreserve"); err == nil {
		t.Error("unreserving what was never reserved must fail")
	}
}

func TestBlockReserveRefusesWhatWouldStarveTheHost(t *testing.T) {
	b := blockSetup(t)
	if out, err := b.do("block", "reserve", "--cpus", "2-55", "--host-reserve-cpus", "4"); err == nil || !strings.Contains(out, "host_reserve_cpus 4") {
		t.Fatalf("host reserve too small: %v\n%s", err, out)
	}
	if out, err := b.do("block", "reserve", "--cpus", "0-55"); err == nil {
		t.Fatalf("a block with nothing left for the host: %s", out)
	}
	if got := b.slice("user.slice"); got != "" {
		t.Errorf("a refused reserve changed user.slice: %q", got)
	}
}

func TestBlockReserveByCoresAndNuma(t *testing.T) {
	b := blockSetup(t)
	b.must("block", "reserve", "--cores", "1", "--numa", "1")
	if got := b.slice("lanes.slice"); got != "5,7" {
		t.Errorf("the last whole core of node 1 is cpus 5,7, got %q", got)
	}
	if _, err := b.do("block", "reserve", "--cores", "9", "--numa", "1"); err == nil {
		t.Error("more cores than the node has must fail")
	}
}

func TestBlockAdoptRecordsAndChangesNothing(t *testing.T) {
	b := blockSetup(t)
	b.must("block", "adopt", "--cpus", "40-55")
	if strings.Contains(b.sysLog(), "set-property") {
		t.Errorf("adopt changed a slice:\n%s", b.sysLog())
	}
	if st := b.must("block", "status"); !strings.Contains(st, "block 40-55") {
		t.Errorf("status:\n%s", st)
	}
}

// Invariant 11: lane-exec puts the command's own process into the lane's cgroup under lanes.slice before it runs, so
// everything the command starts is charged to the lane; a block is required and the lane's cpus must be inside it.
func TestLaneExecRunsTheCommandInTheLaneCgroup(t *testing.T) {
	b := blockSetup(t)
	if _, err := b.do("lane-exec", "c1", "--", "true"); err == nil {
		t.Fatal("no block recorded: lane-exec must refuse")
	}
	b.must("block", "reserve", "--cpus", "40-55", "--memory", "8G")
	pidFile := filepath.Join(b.dir, "pid")
	b.must("lane-exec", "tp-c1", "--cpus", "41-42", "--memory", "2G", "--", "sh", "-c", "echo $$ > "+pidFile)
	lane := filepath.Join(b.cg, "lanes.slice", "lanes-tp_c1.slice")
	procs, _ := os.ReadFile(filepath.Join(lane, "cgroup.procs"))
	pid, _ := os.ReadFile(pidFile)
	if strings.TrimSpace(string(procs)) == "" || strings.TrimSpace(string(procs)) != strings.TrimSpace(string(pid)) {
		t.Errorf("the command is not in the lane cgroup: cgroup.procs %q, command pid %q", procs, pid)
	}
	if cpus, _ := os.ReadFile(filepath.Join(lane, "cpuset.cpus")); strings.TrimSpace(string(cpus)) != "41-42" {
		t.Errorf("lane cpuset %q", cpus)
	}
	if out, err := b.do("lane-exec", "c1", "--cpus", "1-2", "--", "true"); err == nil || !strings.Contains(out, "not inside the block") {
		t.Errorf("cpus outside the block: %v\n%s", err, out)
	}
}
