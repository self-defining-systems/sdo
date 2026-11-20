package bench

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// EvalSlice is the parent cgroup of every lane's node containers on a host with lane limits.
const EvalSlice = "eval.slice"

// reserveBin is the optional root helper (deploy/host/sdo-eval-reserve), called through `sudo -n` by its absolute path:
// the path the sudoers rule names, and one that does not depend on the PATH of a non-login ssh command (which has no sbin).
var reserveBin = "/usr/local/sbin/sdo-eval-reserve"

// reserveLease is how long one `on` holds the host reserved without a renewal; a running step renews it every
// reserveBeat, so a long run keeps it and a crashed one loses it within minutes.
const reserveLease = "10m"

// reserveBeat is how often a run step renews its lease (a var for tests).
var reserveBeat = 3 * time.Minute

// coreWait is how long a build waits for free cores before it fails (a var for tests).
var coreWait = 3 * time.Hour

// Isolation is what a run was measured under: the lane's cores and memory, whether the host was reserved for it, and the
// memory-limit kills (cgroup memory.events oom_kill) that hit its nodes while it ran.
type Isolation struct {
	CPUs     string `json:"cpus,omitempty"`
	Memory   string `json:"memory,omitempty"`
	Reserved bool   `json:"reserved"`
	// Mode is how the host was isolated: two-way, one-way (a block, but other work may use it) or one-way-ok (no reservation).
	Mode     string `json:"mode,omitempty"`
	OOMKills int    `json:"oom_kills,omitempty"`
	// PSIInject and PSIEnd are the lane's cgroup pressure (avg60) at the inject and at the end of the window, so a
	// starved run can be flagged afterwards (nil: not read, for example no lane slice).
	PSIInject *LanePSI `json:"psi_inject,omitempty"`
	PSIEnd    *LanePSI `json:"psi_end,omitempty"`
}

// StepLimits is the lane's share of its host, as the manager decided it (step.json).
type StepLimits struct {
	Cores     int    `json:"cores"`                // whole physical cores for the lane
	Memory    string `json:"memory"`               // the lane's memory in total ("20Gi"), split over its nodes
	MemBudget string `json:"mem_budget,omitempty"` // most the lanes' totals may add up to on the host ("" : 85 % of RAM)
	CPUs      string `json:"cpus"`                 // the host's lanes.cpus: the range cores come from
	State     string `json:"state"`                // the host directory of the allocator's ledger ("": the user's default on the host)
	// StateShared: State is open to every account on purpose (hosts.yaml lanes.state_shared).
	StateShared bool   `json:"state_shared,omitempty"`
	Reserve     bool   `json:"reserve,omitempty"`    // reserve the range while a measured run is active
	MemoryMin   string `json:"memory_min,omitempty"` // memory.min of the parent slice while reserved
	// Block is the host's active lane block: every lane cpuset is a subset of it and every step runs in the lane's cgroup.
	Block string `json:"block,omitempty"`
}

// hostMemTotal is the host's RAM in bytes (0: not known).
func hostMemTotal() int64 {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if k, ok := strings.CutPrefix(sc.Text(), "MemTotal:"); ok {
			if kb, err := strconv.ParseInt(strings.Fields(k)[0], 10, 64); err == nil {
				return kb << 10
			}
		}
	}
	return 0
}

func (r *stepRunner) allocator() (CPUAllocator, error) {
	l := r.s.Limits
	allowed, err := ParseCPUList(l.CPUs)
	if err != nil {
		return CPUAllocator{}, err
	}
	topo := r.topology
	if topo == nil {
		topo = func() (Topology, error) { return ReadTopology("/sys/devices/system/cpu") }
	}
	t, err := topo()
	if err != nil {
		return CPUAllocator{}, err
	}
	budget, _ := ParseSize(l.MemBudget)
	if budget == 0 {
		budget = hostMemTotal() / 100 * 85
	}
	return CPUAllocator{Dir: l.State, Shared: l.StateShared, Allowed: allowed, Topo: t, MemBudget: budget, Live: r.liveLanes, Foreign: r.foreignPins, Log: r.logf}, nil
}

// liveLanes lists the kind clusters that exist on the host (what the ledger is reconciled against).
func (r *stepRunner) liveLanes() (map[string]bool, error) {
	if r.live != nil {
		return r.live()
	}
	ctx, cancel := context.WithTimeout(context.Background(), clusterCheckTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kind", "get", "clusters")
	cmd.Env = append(os.Environ(), r.s.HostEnv...)
	out, err := cmd.Output()
	if err != nil {
		return nil, err
	}
	live := map[string]bool{}
	for _, n := range strings.Fields(string(out)) {
		live[n] = true
	}
	return live, nil
}

// foreignPins lists the containers on the host that are not ours and are pinned to cpus.
func (r *stepRunner) foreignPins() ([]ContainerPin, error) {
	if r.foreign != nil {
		return r.foreign()
	}
	ctx, cancel := context.WithTimeout(context.Background(), clusterCheckTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "sh", "-c", containerProbe)
	cmd.Env = append(os.Environ(), r.s.HostEnv...)
	out, err := cmd.Output()
	if err != nil {
		return nil, err
	}
	return ForeignPins(string(out)), nil
}

// laneEnvLimits takes the lane's cores and memory (waiting its turn while other lanes hold the range or the memory
// budget) and returns the cluster-up variables that put its nodes on them: nil when the host has no lane limits.
// ok is false when the step failed.
func (r *stepRunner) laneEnvLimits(ctx context.Context) (env []string, ok bool) {
	l := r.s.Limits
	if l == nil {
		return nil, true
	}
	mem, err := ParseSize(l.Memory)
	if err != nil || mem <= 0 {
		r.config("lane.memory %q: %v", l.Memory, err)
		return nil, false
	}
	a, err := r.allocator()
	if err != nil {
		r.fail("lane cpus: %v", err)
		return nil, false
	}
	// A TERM from the manager (the experiment was removed) ends the wait and leaves the queue; without it the step
	// would sit in lane-wait until coreWait.
	ctx, stopTerm := signal.NotifyContext(ctx, syscall.SIGTERM)
	defer stopTerm()
	deadline := time.Now().Add(coreWait)
	waited := false
	giveUp := func(f string, args ...any) ([]string, bool) {
		_ = a.Dequeue(r.s.Lane)
		r.fail(f, args...)
		return nil, false
	}
	for {
		cpus, err := a.Take(r.s.Lane, l.Cores, mem)
		switch {
		case err == nil:
			if waited {
				r.logf("lane cores: got %s", FormatCPUList(cpus))
			}
			if l.Block != "" && !cpusWithin(cpus, l.Block) {
				_ = a.Release(r.s.Lane)
				r.config("lane cpus %s are not inside the host's lane block %s: a lane never runs outside the reservation", FormatCPUList(cpus), l.Block)
				return nil, false
			}
			parent := laneParentSlice(r.s)
			r.logf("lane limits: cpus %s, memory %s, parent %s", FormatCPUList(cpus), l.Memory, parent)
			r.res.Isolation = &Isolation{CPUs: FormatCPUList(cpus), Memory: l.Memory, Mode: r.s.IsolationMode}
			return []string{"FUZZOPS_LANE_CPUSET=" + FormatCPUList(cpus), "FUZZOPS_LANE_MEMORY=" + strconv.FormatInt(mem, 10),
				"FUZZOPS_LANE_SLICE=" + parent}, true
		case errors.Is(err, ErrNeverFits):
			_ = a.Dequeue(r.s.Lane)
			r.config("lane.cpus %d / lane.memory %s: %v (host lanes.cpus %s)", l.Cores, l.Memory, err, l.CPUs)
			return nil, false
		case !errors.Is(err, ErrNoFreeCores):
			r.fail("lane cores: %v", err)
			return nil, false
		case time.Now().After(deadline):
			held, _ := a.Held()
			return giveUp("lane cores: no free cores for %d cores after %s (%v; held: %v)", l.Cores, coreWait, err, held)
		}
		if !waited {
			held, _ := a.Held()
			r.logf("lane cores: waiting, %v; held %v", err, held)
			waited = true
		}
		r.phase(PhaseLaneWait, err.Error())
		select {
		case <-ctx.Done():
			return giveUp("lane cores: wait cancelled: %v", context.Cause(ctx))
		case <-time.After(max(r.poll, time.Millisecond)):
		}
	}
}

// laneCreated records in the ledger that the lane's cluster exists (up) or, after a cluster-up that failed, drops
// what the lane holds when no cluster was left behind.
func (r *stepRunner) laneCreated(ctx context.Context, up bool) {
	if r.s.Limits == nil {
		return
	}
	a, err := r.allocator()
	if err == nil && up {
		err = a.MarkCreated(r.s.Lane)
	} else if err == nil && r.clusterMissing(ctx) {
		err = a.Release(r.s.Lane)
	}
	if err != nil {
		r.logf("lane ledger: %v", err)
	}
}

// freeLaneCores gives the lane's cores back when its cluster is gone.
func (r *stepRunner) freeLaneCores() {
	if r.s.Limits == nil {
		return
	}
	a, err := r.allocator()
	if err == nil {
		err = a.Release(r.s.Lane)
	}
	if err != nil {
		r.logf("lane cores not released: %v", err)
	}
}

// reapplyLimits re-sets an existing lane's cpuset, memory and systemd cpuset property to what the ledger says it holds
// (a claim or a reuse: a `systemctl daemon-reload` or a container restart may have undone the property since the lane was
// built). A failure is logged, not fatal: the run's isolation record tells what it was measured under.
func (r *stepRunner) reapplyLimits(ctx context.Context) {
	l := r.s.Limits
	if l == nil {
		return
	}
	a, err := r.allocator()
	var e ledgerEntry
	var ok bool
	if err == nil {
		var es map[string]ledgerEntry
		es, err = a.Entries()
		e, ok = es[r.s.Lane]
	}
	if err != nil || !ok {
		r.logf("lane limits not re-applied: no ledger entry for the lane (%v)", err)
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "sh", "-c", "deploy/lane-limits.sh apply "+shellQuote(r.s.Lane))
	cmd.Dir = r.s.Fuzzops
	cmd.Env = append(append(os.Environ(), r.s.HostEnv...), "FUZZOPS_LANE_CPUSET="+FormatCPUList(e.CPUs),
		"FUZZOPS_LANE_MEMORY="+strconv.FormatInt(e.Memory, 10), "FUZZOPS_LANE_SLICE="+laneParentSlice(r.s))
	cmd.Stdout, cmd.Stderr = r.log, r.log
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	if err := cmd.Run(); err != nil {
		r.logf("lane limits not re-applied: %v", err)
	}
}

// reserveCall asks the host's root helper to give the lanes' CPUs to the lanes alone (on) or to take that back (off).
// It reports whether the host is reserved after the call. Without the helper, or when sudo refuses it, the host is not
// reserved: the lanes are still apart from each other, and the run goes on.
func (r *stepRunner) reserveCall(ctx context.Context, on bool) bool {
	l := r.s.Limits
	if l == nil || !l.Reserve {
		return false
	}
	args := []string{"-n", reserveBin, "off", r.s.Lane}
	if on {
		args = []string{"-n", reserveBin, "on", r.s.Lane, "--cpus", l.CPUs, "--lease", reserveLease}
		if l.MemoryMin != "" {
			args = append(args, "--memory-min", l.MemoryMin)
		}
	}
	if _, err := os.Stat(reserveBin); err != nil {
		if on {
			r.logf("host not reserved: %s is not installed (deploy/host/README.md)", reserveBin)
		}
		return false
	}
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	out, err := exec.CommandContext(ctx, "sudo", args...).CombinedOutput()
	if err != nil {
		r.logf("host not reserved: sudo %s: %v: %s", strings.Join(args[1:], " "), err, strings.TrimSpace(string(out)))
		return false
	}
	return on
}

// reserve is reserveCall for callers that do not need the answer.
func (r *stepRunner) reserve(ctx context.Context, on bool) { r.reserveCall(ctx, on) }

// laneOOMKills sums the oom_kill counters (cgroup memory.events, which count the kills below the cgroup as well) of the
// lane's node containers.
func (r *stepRunner) laneOOMKills(ctx context.Context) (int, error) {
	if r.oom != nil {
		return r.oom()
	}
	run := func(name string, args ...string) (string, error) {
		cmd := exec.CommandContext(ctx, name, args...)
		cmd.Env = append(os.Environ(), r.s.HostEnv...)
		out, err := cmd.Output()
		return strings.TrimSpace(string(out)), err
	}
	ids, err := run("docker", "ps", "-aq", "--filter", "label=io.x-k8s.kind.cluster="+r.s.Lane)
	if err != nil {
		return 0, err
	}
	total := 0
	for _, id := range strings.Fields(ids) {
		pid, err := run("docker", "inspect", "-f", "{{.State.Pid}}", id)
		if err != nil || pid == "" || pid == "0" {
			continue
		}
		b, err := os.ReadFile("/proc/" + pid + "/cgroup")
		if err != nil {
			continue
		}
		path := strings.TrimSuffix(strings.TrimSpace(string(b[strings.LastIndexByte(strings.TrimSpace(string(b)), ':')+1:])), "/init.scope")
		ev, err := os.ReadFile("/sys/fs/cgroup" + path + "/memory.events")
		if err != nil {
			continue
		}
		for _, ln := range strings.Split(string(ev), "\n") {
			if v, ok := strings.CutPrefix(ln, "oom_kill "); ok {
				n, _ := strconv.Atoi(strings.TrimSpace(v))
				total += n
			}
		}
	}
	return total, nil
}

// isolate starts a run's part of the lane limits: the host reservation (renewed while the run lasts) and the baseline
// of the lane's OOM kills. The returned function ends it and records what the run was measured under.
func (r *stepRunner) isolate(ctx context.Context) (finish func()) {
	l := r.s.Limits
	if l == nil {
		return func() {}
	}
	iso := &Isolation{Memory: l.Memory}
	if a, err := r.allocator(); err == nil {
		if es, err := a.Entries(); err == nil {
			iso.CPUs = FormatCPUList(es[r.s.Lane].CPUs)
		}
	}
	iso.Reserved = r.reserveCall(ctx, true)
	if iso.Reserved {
		r.logf("host reserved for the lanes (cpus %s)", l.CPUs)
	}
	base, oerr := r.laneOOMKills(ctx)
	r.iso = iso
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(reserveBeat)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				if iso.Reserved {
					r.reserveCall(ctx, true) // renews the lease
				}
			}
		}
	}()
	return func() {
		close(stop)
		<-done
		bg := context.WithoutCancel(ctx)
		if iso.Reserved {
			r.reserveCall(bg, false)
		}
		if n, err := r.laneOOMKills(bg); oerr == nil && err == nil && n > base {
			iso.OOMKills = n - base
			r.logf("lane memory: %d process(es) in the lane's nodes were killed at their memory limit during the run", iso.OOMKills)
		}
		if p, ok := r.lanePSI(); ok {
			iso.PSIEnd = &p
		}
		r.res.Isolation = iso
	}
}

// lanePSI reads the lane slice's pressure (avg60) on this host.
func (r *stepRunner) lanePSI() (LanePSI, bool) {
	if r.psi != nil {
		return r.psi()
	}
	dir := "/sys/fs/cgroup/lanes.slice/" + LaneSlice(r.s.Lane)
	cpu, e1 := readPSI(dir+"/cpu.pressure", "some")
	mem, e2 := readPSI(dir+"/memory.pressure", "full")
	return LanePSI{CPUSome: cpu, MemFull: mem}, e1 == nil && e2 == nil
}

// recordPSIAtInject notes the lane's pressure when the fault is injected (the end is noted when the run ends).
func (r *stepRunner) recordPSIAtInject() {
	if r.iso == nil {
		return
	}
	if p, ok := r.lanePSI(); ok {
		r.iso.PSIInject = &p
	}
}

// readPSI returns the avg60 of the line starting with kind ("some" or "full") of a cgroup pressure file.
func readPSI(path, kind string) (float64, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	for _, ln := range strings.Split(string(b), "\n") {
		f := strings.Fields(ln)
		if len(f) > 1 && f[0] == kind {
			for _, kv := range f[1:] {
				if v, ok := strings.CutPrefix(kv, "avg60="); ok {
					return strconv.ParseFloat(v, 64)
				}
			}
		}
	}
	return 0, fmt.Errorf("%s: no %s avg60", path, kind)
}

// laneLimits builds the manager's StepLimits for a lane on host h, or nil when the host has no lane limits.
func laneLimits(h Host, s Spec, block string) *StepLimits {
	if h.IsVcluster() || (!h.Lanes.Enabled() && block == "") {
		return nil
	}
	lim := s.LaneLimit()
	if block != "" {
		return &StepLimits{Cores: lim.CPUs, Memory: lim.Memory, MemBudget: firstNonEmpty(h.Reserve.memory(), h.Lanes.Memory), CPUs: block, State: h.Lanes.State, StateShared: h.Lanes.StateShared, Block: block}
	}
	// An empty State is resolved by the step on the host, for the user that runs it (the manager may be another account).
	return &StepLimits{Cores: lim.CPUs, Memory: lim.Memory, MemBudget: h.Lanes.Memory, CPUs: h.Lanes.CPUs, State: h.Lanes.State, StateShared: h.Lanes.StateShared, Reserve: h.Lanes.Reserve, MemoryMin: h.Lanes.MemoryMin}
}

func firstNonEmpty(v ...string) string {
	for _, s := range v {
		if s != "" {
			return s
		}
	}
	return ""
}

func (r *HostReserve) memory() string {
	if r == nil {
		return ""
	}
	return r.Memory
}

// laneParentSlice is the cgroup parent of the lane's node containers (docker --cgroup-parent through the shim): the
// lane's own slice under lanes.slice on a reserved host, else the shared eval.slice.
func laneParentSlice(s Step) string {
	if s.Limits != nil && s.Limits.Block != "" {
		return LaneSlice(s.Lane)
	}
	return EvalSlice
}

// cpusWithin reports whether every cpu is in the list.
func cpusWithin(cpus []int, list string) bool {
	set, err := ParseCPUList(list)
	if err != nil {
		return false
	}
	in := map[int]bool{}
	for _, c := range set {
		in[c] = true
	}
	for _, c := range cpus {
		if !in[c] {
			return false
		}
	}
	return true
}
