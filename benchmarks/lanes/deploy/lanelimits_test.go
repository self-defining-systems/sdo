//go:build integration

package deploy_test

// Tests of the per-lane CPU and memory limits (docs/bench.md, "Lane limits"): lane-docker-shim/docker puts every
// node container of a kind lane under the parent slice with the lane's cpuset and memory, lane-limits.sh applies
// the same limits to nodes that already exist, and host/sdo-eval-reserve (the optional root helper) turns the
// parent slice into an exclusive cpuset partition while measured runs are active. All against stubs and a
// directory standing in for the cgroup tree.

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

type limitEnv struct {
	t        *testing.T
	dir, bin string
	vars     []string
	shimDir  string
}

const gi = 1 << 30

func limitSetup(t *testing.T) *limitEnv {
	t.Helper()
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	os.MkdirAll(bin, 0o755)
	stubs := map[string]string{
		"docker":    "#!/bin/sh\necho \"docker $*\" >> \"$STUB_LOG\"\n[ \"$1\" = inspect ] && echo \"$STUB_ID\"\nexit 0\n",
		"systemctl": "#!/bin/sh\necho \"$STUB_PROP\"\n",
		"sudo":      "#!/bin/sh\necho \"sudo $*\" >> \"$STUB_LOG\"\n",
		"kind":      "#!/bin/sh\nif [ \"$1 $2\" = \"get nodes\" ]; then printf '%s\\n' ${STUB_NODES:-lane1-control-plane lane1-worker lane1-worker2}; fi\nexit 0\n",
	}
	for n, b := range stubs {
		if err := os.WriteFile(filepath.Join(bin, n), []byte(b), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	abs, _ := filepath.Abs("lane-docker-shim")
	return &limitEnv{t: t, dir: dir, bin: bin, shimDir: abs,
		vars: append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"), "STUB_LOG="+filepath.Join(dir, "log"))}
}

func (e *limitEnv) log() string { b, _ := os.ReadFile(filepath.Join(e.dir, "log")); return string(b) }

func (e *limitEnv) run(extra []string, name string, args ...string) (string, error) {
	e.t.Helper()
	cmd := exec.Command(name, args...)
	cmd.Env = append(append([]string{}, e.vars...), extra...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

var laneVars = []string{"FUZZOPS_LANE_CPUSET=8-11", "FUZZOPS_LANE_MEMORY=" + "21474836480", "FUZZOPS_LANE_SLICE=eval.slice"}

// kind creates every node with `docker run`: the shim adds the lane's limits to those and to nothing else.
func TestShimAddsLimitsToKindNodes(t *testing.T) {
	e := limitSetup(t)
	shim := []string{"PATH=" + e.shimDir + ":" + e.bin + ":" + os.Getenv("PATH"), "FUZZOPS_REAL_DOCKER=" + filepath.Join(e.bin, "docker")}
	vars := append(append([]string{}, laneVars...), shim...)
	for _, node := range []string{"lane1-control-plane", "lane1-worker"} {
		if out, err := e.run(vars, filepath.Join(e.shimDir, "docker"), "run", "--detach", "--name", node, "--label", "io.x-k8s.kind.cluster=lane1", "kindest/node:v1"); err != nil {
			t.Fatalf("%v: %s", err, out)
		}
	}
	if out, err := e.run(vars, filepath.Join(e.shimDir, "docker"), "run", "--rm", "alpine"); err != nil { // not a kind node
		t.Fatalf("%v: %s", err, out)
	}
	if out, err := e.run(vars, filepath.Join(e.shimDir, "docker"), "ps"); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	lines := strings.Split(strings.TrimSpace(e.log()), "\n")
	if len(lines) != 4 {
		t.Fatalf("log: %v", lines)
	}
	for _, f := range []string{"--cgroup-parent=eval.slice", "--cpuset-cpus=8-11"} {
		if !strings.Contains(lines[0], f) || !strings.Contains(lines[1], f) {
			t.Errorf("missing %s: %v", f, lines[:2])
		}
	}
	// the lane's memory is split: the control-plane 15 %, the two workers 85 % between them; swap equal to memory (no swap)
	if !strings.Contains(lines[0], "--memory=3221225472 --memory-swap=3221225472") || !strings.Contains(lines[1], "--memory=9126805504 --memory-swap=9126805504") {
		t.Errorf("memory split: %v", lines[:2])
	}
	if strings.Contains(lines[2], "cpuset") || strings.Contains(lines[3], "cgroup") {
		t.Errorf("a command that is not a kind node was changed: %v", lines[2:])
	}
}

// Without lane variables the shim is the docker it wraps.
func TestShimIsTransparentWithoutLimits(t *testing.T) {
	e := limitSetup(t)
	shim := []string{"PATH=" + e.shimDir + ":" + e.bin + ":" + os.Getenv("PATH"), "FUZZOPS_REAL_DOCKER=" + filepath.Join(e.bin, "docker")}
	if out, err := e.run(shim, filepath.Join(e.shimDir, "docker"), "run", "--name", "lane1-worker", "--label", "io.x-k8s.kind.cluster=lane1", "img"); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if got := strings.TrimSpace(e.log()); got != "docker run --name lane1-worker --label io.x-k8s.kind.cluster=lane1 img" {
		t.Fatalf("%q", got)
	}
}

// An existing lane (adopted, or built before limits existed) gets the same limits with docker update, without restart.
func TestApplyUpdatesExistingNodes(t *testing.T) {
	e := limitSetup(t)
	if out, err := e.run(laneVars, "bash", "lane-limits.sh", "apply", "lane1"); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	log := e.log()
	for _, want := range []string{
		"docker update --cpuset-cpus=8-11 --memory=3221225472 --memory-swap=3221225472 lane1-control-plane",
		"docker update --cpuset-cpus=8-11 --memory=9126805504 --memory-swap=9126805504 lane1-worker\n",
		"docker update --cpuset-cpus=8-11 --memory=9126805504 --memory-swap=9126805504 lane1-worker2",
	} {
		if !strings.Contains(log, want) {
			t.Errorf("missing %q in\n%s", want, log)
		}
	}
	if strings.Contains(log, "restart") {
		t.Error("apply restarted a node")
	}
	e2 := limitSetup(t)
	if out, err := e2.run(nil, "bash", "lane-limits.sh", "apply", "lane1"); err != nil || e2.log() != "" {
		t.Fatalf("no limits asked, nothing to do: %v %s %q", err, out, e2.log())
	}
}

// runc's systemd driver may leave a wrong AllowedCPUs on the node's scope: apply sets it with the helper, and leaves a
// right one alone.
func TestApplyPinsAWronglyEncodedScope(t *testing.T) {
	id := strings.Repeat("cd", 32)
	e := limitSetup(t)
	helper := filepath.Join(e.dir, "sdo-eval-reserve")
	if err := os.WriteFile(helper, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	vars := append([]string{"STUB_ID=" + id, "STUB_PROP=0-2 14-15", "FUZZOPS_EVAL_HELPER=" + helper}, laneVars...)
	if out, err := e.run(vars, "bash", "lane-limits.sh", "apply", "lane1"); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if got := strings.Count(e.log(), "sudo -n "+helper+" pin "+id+" 8-11"); got != 3 {
		t.Errorf("pinned %d nodes, want 3:\n%s", got, e.log())
	}
	e = limitSetup(t)
	vars = append([]string{"STUB_ID=" + id, "STUB_PROP=8-11", "FUZZOPS_EVAL_HELPER=" + helper}, laneVars...)
	if out, err := e.run(vars, "bash", "lane-limits.sh", "apply", "lane1"); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if strings.Contains(e.log(), "sudo") {
		t.Errorf("a property that is right was set again:\n%s", e.log())
	}
	// no helper installed: the apply still succeeds, and says what is left undone
	e = limitSetup(t)
	vars = append([]string{"STUB_ID=" + id, "STUB_PROP=0-2 14-15", "FUZZOPS_EVAL_HELPER=" + filepath.Join(e.dir, "absent")}, laneVars...)
	out, err := e.run(vars, "bash", "lane-limits.sh", "apply", "lane1")
	if err != nil || !strings.Contains(out, "AllowedCPUs") || !strings.Contains(out, "daemon-reload") {
		t.Errorf("no helper: %v %s", err, out)
	}
}

// The lane's memory is a total: the shares of its nodes add up to it, whatever the number of workers; a node that is
// neither control-plane nor worker (a load balancer) gets no memory limit.
func TestMemorySharesAddUpToTheLaneTotal(t *testing.T) {
	const total = 20 << 30
	for name, tc := range map[string]struct {
		nodes   string
		wantSum int64
	}{
		"two workers":   {"lane1-control-plane lane1-worker lane1-worker2", total},
		"one worker":    {"lane1-control-plane lane1-worker", total},
		"four workers":  {"lane1-control-plane lane1-worker lane1-worker2 lane1-worker3 lane1-worker4", total},
		"two control":   {"lane1-control-plane lane1-control-plane2 lane1-worker lane1-worker2", total},
		"no worker":     {"lane1-control-plane", total},
		"load balancer": {"lane1-external-load-balancer lane1-control-plane lane1-worker", total},
	} {
		e := limitSetup(t)
		vars := []string{"STUB_NODES=" + tc.nodes, "FUZZOPS_LANE_CPUSET=8-11", "FUZZOPS_LANE_MEMORY=" + strconv.Itoa(total)}
		if out, err := e.run(vars, "bash", "lane-limits.sh", "apply", "lane1"); err != nil {
			t.Fatalf("%s: %v: %s", name, err, out)
		}
		var sum int64
		for _, l := range strings.Split(strings.TrimSpace(e.log()), "\n") {
			if !strings.HasPrefix(l, "docker update") {
				continue
			}
			if i := strings.Index(l, "--memory="); i >= 0 {
				v, _ := strconv.ParseInt(strings.Fields(l[i+len("--memory="):])[0], 10, 64)
				sum += v
			} else if !strings.Contains(l, "load-balancer") {
				t.Errorf("%s: a node without a memory limit: %s", name, l)
			}
		}
		// pages are rounded down, so the shares may fall a few pages short of the total, never above it
		if sum > tc.wantSum || sum < tc.wantSum-16*4096 {
			t.Errorf("%s: shares add up to %d, the lane's total is %d", name, sum, tc.wantSum)
		}
	}
	e := limitSetup(t)
	e.run([]string{"STUB_NODES=lane1-external-load-balancer", "FUZZOPS_LANE_CPUSET=8-11", "FUZZOPS_LANE_MEMORY=1073741824"}, "bash", "lane-limits.sh", "apply", "lane1")
	if !strings.Contains(e.log(), "docker update --cpuset-cpus=8-11 lane1-external-load-balancer") || strings.Contains(e.log(), "memory") {
		t.Errorf("load balancer: %s", e.log())
	}
}

// Without FUZZOPS_LANE_* kind-lane.sh up is what it was: no shim, no docker update, no parent check.
func TestKindLaneUpWithoutLimitsIsUnchanged(t *testing.T) {
	e := setup(t)
	out, err := e.run(e.with("KIND_NODE_PRELOAD="), "kind-lane.sh", "up", "lane", filepath.Join(e.dir, "kc"), filepath.Join(e.dir, "ev"))
	if err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	for _, bad := range []string{"docker update", "cgroup-parent", "CgroupParent"} {
		if strings.Contains(e.log(), bad) {
			t.Errorf("%q in the log without lane limits:\n%s", bad, e.log())
		}
	}
}

// With limits the lane is checked after kind create: a node outside the slice (a kind that did not pass its `docker run`
// through the shim) fails the lane up loudly and deletes the lane; one inside it passes.
func TestKindLaneUpChecksTheCgroupParent(t *testing.T) {
	for _, tc := range []struct {
		parent string
		ok     bool
	}{{"eval.slice", true}, {"", false}, {"system.slice", false}} {
		e := setup(t)
		vars := e.with("KIND_NODE_PRELOAD=", "FUZZOPS_LANE_CPUSET=8-11", "FUZZOPS_LANE_MEMORY=4294967296", "FUZZOPS_LANE_SLICE=eval.slice", "STUB_CGROUP_PARENT="+tc.parent)
		out, err := e.run(vars, "kind-lane.sh", "up", "lane", filepath.Join(e.dir, "kc"), filepath.Join(e.dir, "ev"))
		if tc.ok && err != nil {
			t.Fatalf("parent %q: %v: %s", tc.parent, err, out)
		}
		if !tc.ok {
			if err == nil || !strings.Contains(out, "not '"+"eval.slice"+"'") {
				t.Fatalf("parent %q must fail loudly: %v: %s", tc.parent, err, out)
			}
			if !strings.Contains(e.log(), "kind delete cluster --name lane") {
				t.Errorf("parent %q: the wrongly placed lane was left:\n%s", tc.parent, e.log())
			}
		}
	}
}

// A memory limit that is only a number handed to docker is not a limit: after the lane is up, every node must read back
// a non-zero HostConfig.Memory, or the lane is deleted and the up fails (an unlimited node can take the whole host).
func TestKindLaneUpChecksTheMemoryLimitIsApplied(t *testing.T) {
	for _, tc := range []struct {
		mem string
		ok  bool
	}{{"4294967296", true}, {"0", false}, {"", false}} {
		e := setup(t)
		vars := e.with("KIND_NODE_PRELOAD=", "FUZZOPS_LANE_CPUSET=8-11", "FUZZOPS_LANE_MEMORY=4294967296", "FUZZOPS_LANE_SLICE=eval.slice", "STUB_CGROUP_PARENT=eval.slice", "STUB_NODE_MEMORY="+tc.mem)
		out, err := e.run(vars, "kind-lane.sh", "up", "lane", filepath.Join(e.dir, "kc"), filepath.Join(e.dir, "ev"))
		if tc.ok && err != nil {
			t.Fatalf("memory %q: %v: %s", tc.mem, err, out)
		}
		if !tc.ok {
			if err == nil || !strings.Contains(out, "no memory limit") {
				t.Fatalf("memory %q must fail loudly: %v: %s", tc.mem, err, out)
			}
			if !strings.Contains(e.log(), "kind delete cluster --name lane") {
				t.Errorf("memory %q: the unlimited lane was left:\n%s", tc.mem, e.log())
			}
		}
	}
}

// The helper is run by an unprivileged user against a directory standing in for the cgroup tree: the SDO_EVAL_* hooks
// exist for that and are not read as root.
type reserveT struct {
	*limitEnv
	vars  []string
	slice string
	state string
}

func reserveEnv(t *testing.T) *reserveT {
	e := limitSetup(t)
	if os.Geteuid() == 0 {
		t.Skip("the helper ignores its test hooks as root")
	}
	cg, state := filepath.Join(e.dir, "cg"), filepath.Join(e.dir, "state")
	slice := filepath.Join(cg, "eval.slice")
	os.MkdirAll(slice, 0o755)
	os.WriteFile(filepath.Join(slice, "cpuset.cpus.partition"), []byte("member\n"), 0o644)
	os.WriteFile(filepath.Join(e.dir, "online"), []byte("0-55\n"), 0o644)
	os.WriteFile(filepath.Join(e.dir, "meminfo"), []byte("MemTotal:       188743680 kB\n"), 0o644) // 180 GiB
	sc := filepath.Join(e.bin, "systemctl")
	os.WriteFile(sc, []byte("#!/bin/sh\necho \"systemctl $*\" >> "+filepath.Join(e.dir, "systemctl.log")+"\n[ \"$1\" = show ] && echo \"$STUB_PROP\"\nexit 0\n"), 0o755)
	return &reserveT{limitEnv: e, slice: slice, state: state, vars: []string{
		"SDO_EVAL_CGROUP_ROOT=" + cg, "SDO_EVAL_STATE=" + state, "SDO_EVAL_ONLINE=" + filepath.Join(e.dir, "online"),
		"SDO_EVAL_MEMINFO=" + filepath.Join(e.dir, "meminfo"), "SDO_EVAL_SYSTEMCTL=" + sc, "SDO_EVAL_DOCKER=" + filepath.Join(e.bin, "docker")}}
}

func (r *reserveT) do(extra []string, args ...string) (string, error) {
	return r.limitEnv.run(append(append([]string{}, r.vars...), extra...), "bash", append([]string{"host/sdo-eval-reserve"}, args...)...)
}

func (r *reserveT) must(extra []string, args ...string) string {
	r.t.Helper()
	out, err := r.do(extra, args...)
	if err != nil {
		r.t.Fatalf("%v: %v: %s", args, err, out)
	}
	return out
}

func (r *reserveT) file(name string) string { return cgfile(r.t, r.slice, name) }

func cgfile(t *testing.T, dir, name string) string {
	b, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		return "<absent>"
	}
	return strings.TrimSpace(string(b))
}

func TestReserveIsReferenceCounted(t *testing.T) {
	r := reserveEnv(t)
	if out := r.must(nil, "status"); !strings.Contains(out, "not reserved") {
		t.Fatal(out)
	}
	r.must(nil, "on", "a", "--cpus", "4-55", "--lease", "2h", "--memory-min", "8G")
	if r.file("cpuset.cpus") != "4-55" || r.file("cpuset.cpus.partition") != "root" || r.file("memory.min") != "8589934592" {
		t.Fatalf("on: cpus=%s partition=%s memory.min=%s", r.file("cpuset.cpus"), r.file("cpuset.cpus.partition"), r.file("memory.min"))
	}
	r.must(nil, "on", "b", "--cpus", "4-55")
	r.must(nil, "off", "a")
	if r.file("cpuset.cpus.partition") != "root" {
		t.Fatal("turned off while lane b still holds a lease")
	}
	if out := r.must(nil, "status"); !strings.Contains(out, "reserved") || !strings.Contains(out, "1 lease") {
		t.Fatal(out)
	}
	r.must(nil, "off", "b")
	if r.file("cpuset.cpus.partition") != "member" || r.file("memory.min") != "0" {
		t.Fatalf("last off: partition=%s memory.min=%s", r.file("cpuset.cpus.partition"), r.file("memory.min"))
	}
	r.must(nil, "off", "b") // an off without a lease is fine
}

// The reserved range is the union of the live leases, so benches with different ranges on one host are all protected.
func TestReserveIsTheUnionOfLiveLeases(t *testing.T) {
	r := reserveEnv(t)
	r.must(nil, "on", "a", "--cpus", "4-11")
	r.must(nil, "on", "b", "--cpus", "10-19,40-41")
	if got := r.file("cpuset.cpus"); got != "4-19,40-41" {
		t.Fatalf("union = %s", got)
	}
	r.must(nil, "off", "a")
	if got := r.file("cpuset.cpus"); got != "10-19,40-41" || r.file("cpuset.cpus.partition") != "root" {
		t.Fatalf("after a's end: %s %s", got, r.file("cpuset.cpus.partition"))
	}
}

// A lease nobody renews expires; one that is renewed does not; the host is released when none is left.
func TestReserveLeaseExpires(t *testing.T) {
	r := reserveEnv(t)
	r.must([]string{"SDO_EVAL_NOW=1000"}, "on", "a", "--cpus", "4-55", "--lease", "2h")
	r.must([]string{"SDO_EVAL_NOW=1000"}, "on", "b", "--cpus", "4-55", "--lease", "10m")
	r.must([]string{"SDO_EVAL_NOW=2000"}, "gc") // b expired, a (until 8200) has not
	if r.file("cpuset.cpus.partition") != "root" {
		t.Fatal("reservation dropped while a lease is live")
	}
	r.must([]string{"SDO_EVAL_NOW=9000"}, "gc")
	if r.file("cpuset.cpus.partition") != "member" {
		t.Fatal("expired leases still hold the host")
	}
	// renewing: the same lane asking again extends its lease, it does not count twice
	r.must([]string{"SDO_EVAL_NOW=10000"}, "on", "a", "--cpus", "4-55", "--lease", "10m")
	r.must([]string{"SDO_EVAL_NOW=10500"}, "on", "a", "--cpus", "4-55", "--lease", "10m") // the heartbeat
	r.must([]string{"SDO_EVAL_NOW=11050"}, "gc")                                          // 550 s after the last renewal, 1050 after the first
	if r.file("cpuset.cpus.partition") != "root" {
		t.Fatal("a lane that keeps renewing lost its lease")
	}
	r.must([]string{"SDO_EVAL_NOW=10600"}, "off", "a")
	if r.file("cpuset.cpus.partition") != "member" {
		t.Fatal("a lane renewing twice needs one off")
	}
}

func TestReserveRestoresAnAdministratorsMemoryMin(t *testing.T) {
	r := reserveEnv(t)
	os.WriteFile(filepath.Join(r.slice, "memory.min"), []byte("1073741824\n"), 0o644)
	r.must(nil, "on", "a", "--cpus", "4-55", "--memory-min", "8G")
	r.must(nil, "off", "a")
	if got := r.file("memory.min"); got != "1073741824" {
		t.Fatalf("memory.min = %s, the administrator's value was lost", got)
	}
}

// The helper is a root policy point: a caller cannot make it take the host's CPUs or memory, or overflow its arithmetic.
func TestReserveRefusesNonsense(t *testing.T) {
	r := reserveEnv(t)
	for name, args := range map[string][]string{
		"no lane":               {"on"},
		"no cpus":               {"on", "a"},
		"cpus not a list":       {"on", "a", "--cpus", "x"},
		"bad lease":             {"on", "a", "--cpus", "4-5", "--lease", "soon"},
		"unknown verb":          {"frobnicate"},
		"huge cpu range":        {"on", "a", "--cpus", "0-9999999999999"},
		"cpus not online":       {"on", "a", "--cpus", "50-70"},
		"every online cpu":      {"on", "a", "--cpus", "0-55"},
		"backwards range":       {"on", "a", "--cpus", "9-4"},
		"memory overflow":       {"on", "a", "--cpus", "4-5", "--memory-min", "99999999999999T"},
		"memory over the RAM":   {"on", "a", "--cpus", "4-5", "--memory-min", "400G"},
		"memory too many digit": {"on", "a", "--cpus", "4-5", "--memory-min", "9999999999999"},
		"lane with a slash":     {"on", "a/b", "--cpus", "4-5"},
		"lane with a semicolon": {"on", "a;touch /tmp/pwn", "--cpus", "4-5"},
	} {
		if out, err := r.do(nil, args...); err == nil {
			t.Errorf("%s accepted: %s", name, out)
		}
	}
	if r.file("cpuset.cpus.partition") != "member" || r.file("memory.min") != "<absent>" {
		t.Errorf("a refused request changed the cgroup: %s %s", r.file("cpuset.cpus.partition"), r.file("memory.min"))
	}
	if got, _ := filepath.Glob(filepath.Join(r.state, "lease.*")); len(got) != 0 {
		t.Errorf("a refused request left a lease: %v", got)
	}
	r.must(nil, "on", "kind.cluster-1", "--cpus", "4-55", "--memory-min", "170G") // a lane name kind allows; just under the RAM
}

// As a plain user the helper does nothing without its test hooks: they are the only way to point it anywhere.
func TestReserveNeedsRootOrTheTestHooks(t *testing.T) {
	e := limitSetup(t)
	if os.Geteuid() == 0 {
		t.Skip("root")
	}
	if out, err := e.run(nil, "bash", "host/sdo-eval-reserve", "status"); err == nil {
		t.Fatalf("ran unprivileged against the real cgroup tree: %s", out)
	}
}

// What docker says a lane node's cpuset is wins over systemd's property and the cgroup file, which runc 1.1.x and a
// daemon-reload can both get wrong: every `on`, `gc` and `repin` re-pins what differs.
func TestRepinFixesScrambledScopes(t *testing.T) {
	r := reserveEnv(t)
	id := strings.Repeat("ab", 32)
	scope := filepath.Join(r.slice, "docker-"+id+".scope")
	os.MkdirAll(scope, 0o755)
	os.WriteFile(filepath.Join(scope, "cpuset.cpus"), []byte("0-2,14-15\n"), 0o644) // what a daemon-reload wrote
	// stub docker: inspect prints the cpuset docker believes in
	os.WriteFile(filepath.Join(r.bin, "docker"), []byte("#!/bin/sh\n[ \"$1\" = inspect ] && echo \"$STUB_DOCKER_CPUSET\"\nexit 0\n"), 0o755)
	extra := []string{"STUB_DOCKER_CPUSET=38-42", "STUB_PROP=0-2 14-15"}
	for _, verb := range [][]string{{"repin"}, {"gc"}, {"on", "a", "--cpus", "4-55"}} {
		os.Remove(filepath.Join(r.dir, "systemctl.log"))
		r.must(extra, verb...)
		b, _ := os.ReadFile(filepath.Join(r.dir, "systemctl.log"))
		if !strings.Contains(string(b), "set-property --runtime docker-"+id+".scope AllowedCPUs=38-42 MemorySwapMax=0") {
			t.Errorf("%v did not re-pin:\n%s", verb, b)
		}
		r.must(nil, "off", "a")
	}
	// a scope that is right is left alone
	os.WriteFile(filepath.Join(scope, "cpuset.cpus"), []byte("38-42\n"), 0o644)
	os.Remove(filepath.Join(r.dir, "systemctl.log"))
	r.must([]string{"STUB_DOCKER_CPUSET=38-42", "STUB_PROP=38-42"}, "repin")
	if b, _ := os.ReadFile(filepath.Join(r.dir, "systemctl.log")); strings.Contains(string(b), "set-property") {
		t.Errorf("a right scope was set again:\n%s", b)
	}
}

// pin sets the systemd property of a lane node's scope, and only of one under eval.slice.
func TestPinSetsTheScopeProperty(t *testing.T) {
	r := reserveEnv(t)
	id := strings.Repeat("ab", 32)
	os.MkdirAll(filepath.Join(r.slice, "docker-"+id+".scope"), 0o755)
	r.must(nil, "pin", id, "38-42")
	b, _ := os.ReadFile(filepath.Join(r.dir, "systemctl.log"))
	if want := "systemctl set-property --runtime docker-" + id + ".scope AllowedCPUs=38-42 MemorySwapMax=0\n"; string(b) != want {
		t.Fatalf("systemctl called with %q, want %q", b, want)
	}
	other := strings.Repeat("cd", 32)
	for _, args := range [][]string{{"pin", "../../x", "1-2"}, {"pin", id, "x"}, {"pin", id}, {"pin", other, "1-2"}} {
		if out, err := r.do(nil, args...); err == nil {
			t.Errorf("%v accepted: %s", args, out)
		}
	}
}

// While reserved, status says what the partition displaced.
func TestStatusListsWhatTheReservationDisplaces(t *testing.T) {
	r := reserveEnv(t)
	os.WriteFile(filepath.Join(r.bin, "docker"), []byte("#!/bin/sh\ncase \"$1\" in ps) printf 'lane1-worker\\nregistry\\n' ;; inspect) case \"$4\" in lane1-worker) echo eval.slice ;; *) echo \"\" ;; esac ;; esac\nexit 0\n"), 0o755)
	r.must(nil, "on", "a", "--cpus", "4-55")
	out := r.must(nil, "status")
	if !strings.Contains(out, "registry") || strings.Contains(strings.SplitN(out, "containers:", 2)[1], "lane1-worker") {
		t.Fatalf("status:\n%s", out)
	}
}
