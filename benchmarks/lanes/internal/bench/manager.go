package bench

import (
	"context"
	"errors"
	"fmt"
	"github.com/self-defining-systems/sdo/benchmarks/lanes/internal/plan"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Manager is the one scheduler: every Tick it reads the specs, the inventory
// and each busy lane's step, adopts finished steps (copied home and checked
// before a run is terminal), binds free lanes and starts the next steps. All
// state is files under Root; a restarted manager picks up where the last one
// stopped (steps run on without it).
type Manager struct {
	Root       Root
	Tick       time.Duration // default 10 s
	Poll       time.Duration // idle polls inside steps (default 10 s)
	Log        io.Writer
	claimSince map[string]time.Time // when each claimant began waiting for a pool lane
	// Ops returns how to reach host (default: local steps with Bin, or ssh).
	Ops func(name string, h Host) HostOps
	Bin string // fuzzops-bench for local steps (default: this executable)
	// RootPointer, when set, is the file Serve records the root in (PointerPath), so
	// `fuzzops view` finds it with no flag.
	RootPointer string
	// Now is the manager's clock (default the wall clock); tests set a fake one.
	Now func() time.Time
	// HostPreflight, when set, must pass for a host that was unreachable before it takes work again.
	HostPreflight func(ctx context.Context, host string) error

	inv Inventory
	// invGood: inv is an inventory that loaded; invErr: why hosts.yaml was refused since (used instead of it).
	invGood bool
	invErr  string
	exps    map[string]*Experiment
	lanes   map[string]*LaneFile
	hosts   map[string]HostStatus
	// gcWanted: hosts whose unneeded lane clusters are deleted this pass (gc, or below the disk floor).
	gcWanted map[string]bool
	// Helper runs the root helper's verbs on a host (nil: sudo -n through the host's shell or ssh); DriftEvery is how
	// often a host's slices are compared with their record (default a minute). Tests set both.
	Helper     Helper
	DriftEvery time.Duration

	// planGrading counts the background grading jobs the plans' backends started.
	planGrading sync.WaitGroup

	// down: hosts that stopped answering and since when; alarmed: the alarms standing (raised once each).
	down    map[string]time.Time
	downMu  sync.Mutex
	seeded  bool
	alarmed map[string]bool

	// crash is why a lane gets no new steps (its pods crash-loop, crashloop.go); crashTrack are its restart counters.
	crash      map[string]string
	crashTrack map[string]*crashTracker
	// health is each host's health gate (hosthealth.go), kept across passes.
	health map[string]*healthState
	// kernelWhy is why a host is not admitted for a kernel setting below its minimum ("" : none); warns are its warnings.
	kernelWhy  map[string]string
	psiWhy     map[string]string
	warns      map[string][]string
	rdmaWarned map[string]bool

	reservations map[string]Reservation
	drift        map[string]string
	foreign      map[string]string // host -> slices that hold containers pinned inside the block (live one-way)
	driftAt      map[string]time.Time
}

// Serve runs the manager until ctx ends; it holds <root>/bench.lock.
func (m *Manager) Serve(ctx context.Context) error {
	f, err := os.OpenFile(m.Root.path("bench.lock"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return errors.New("another manager holds " + f.Name())
	}
	if m.Tick == 0 {
		m.Tick = 10 * time.Second
	}
	if m.RootPointer != "" {
		if err := RecordRoot(m.RootPointer, string(m.Root)); err != nil {
			m.logf("cannot record the root for `fuzzops view` at %s: %v", m.RootPointer, err)
		}
	}
	m.logf("manager started")
	for {
		if err := m.Reconcile(ctx); err != nil {
			m.logf("reconcile: %v", err)
		}
		select {
		case <-ctx.Done():
			m.logf("manager stopped (steps run on)")
			return nil
		case <-time.After(m.Tick):
		}
	}
}

func (m *Manager) now() time.Time {
	if m.Now != nil {
		return m.Now()
	}
	return time.Now()
}

func (m *Manager) logf(f string, a ...any) {
	line := fmt.Sprintf("%s %s\n", time.Now().UTC().Format(time.RFC3339), fmt.Sprintf(f, a...))
	if m.Log != nil {
		_, _ = io.WriteString(m.Log, line)
	}
	if lf, err := os.OpenFile(m.Root.path("bench.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644); err == nil {
		_, _ = lf.WriteString(line)
		lf.Close()
	}
}

// ops reaches host through the manager's bounds (guardedOps): no call waits longer than its bound, and none waits at
// all on a host marked unreachable.
func (m *Manager) ops(host string) HostOps {
	return guardedOps{in: m.rawOps(host), m: m, host: host}
}

func (m *Manager) rawOps(host string) HostOps {
	h := m.inv.Hosts[host]
	if m.Ops != nil {
		return m.Ops(host, h)
	}
	if h.Local {
		bin := m.Bin
		if bin == "" {
			bin, _ = os.Executable()
		}
		return localHost{staging: h.Staging, bin: bin, health: h.Health.Thresholds(), mins: h.KernelMins()}
	}
	return sshHost{name: host, h: h, fuzzops: m.inv.Checkouts.Fuzzops, health: h.Health.Thresholds()}
}

// stepsOn is the step of each lane of host that has one in flight, or given up as an orphan (lane -> step id).
func (m *Manager) stepsOn(host string) map[string]string {
	steps := map[string]string{}
	for _, l := range m.lanes {
		switch {
		case l.Host != host:
		case l.Step != nil:
			steps[l.Lane] = l.Step.ID
		case l.Orphan != nil:
			steps[l.Lane] = l.Orphan.ID
		}
	}
	return steps
}

type hostProbe struct {
	pr  Probe
	err error
}

// probeAll probes every host at once. Each probe ends within its bound (guardedOps), so a wedged host delays the
// pass by at most that and the others are never queued behind it.
func (m *Manager) probeAll(ctx context.Context) map[string]hostProbe {
	out := map[string]hostProbe{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for name := range m.inv.Hosts {
		steps := m.stepsOn(name)
		var fz []string
		for _, byHost := range m.inv.Checkouts.Fuzzops {
			if p := byHost[name]; p != "" {
				fz = append(fz, p)
			}
		}
		sort.Strings(fz)
		ops := m.ops(name)
		wg.Add(1)
		go func() {
			defer wg.Done()
			pr, err := ops.Probe(ctx, steps, fz)
			mu.Lock()
			out[name] = hostProbe{pr, err}
			mu.Unlock()
		}()
	}
	wg.Wait()
	return out
}

// seedDown restores, after a restart, which hosts were unreachable and since when.
func (m *Manager) seedDown() {
	if m.seeded {
		return
	}
	m.seeded = true
	var prev map[string]HostStatus
	_ = readJSON(m.Root.path("hosts.status.json"), &prev)
	m.downMu.Lock()
	defer m.downMu.Unlock()
	for name, st := range prev {
		if st.Error != "" && !st.DownSince.IsZero() {
			if m.down == nil {
				m.down = map[string]time.Time{}
			}
			m.down[name] = st.DownSince
		}
	}
}

// markDown records that host did not answer and returns since when it has not.
func (m *Manager) markDown(host string) time.Time {
	m.downMu.Lock()
	defer m.downMu.Unlock()
	if m.down == nil {
		m.down = map[string]time.Time{}
	}
	if _, ok := m.down[host]; !ok {
		m.down[host] = m.now().UTC()
	}
	return m.down[host]
}

// Reconcile is one pass: adopt, then continue sequences, then bind lanes, then release idle operators.
func (m *Manager) Reconcile(ctx context.Context) error {
	if err := m.loadInventory(); err != nil {
		return err
	}
	if err := m.load(); err != nil {
		return err
	}
	m.seedDown()
	m.checkReservations(ctx)
	m.hosts = map[string]HostStatus{}
	reachable := map[string]bool{}
	probes := m.probeAll(ctx)
	var names []string
	for name := range m.inv.Hosts {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		pr, err := probes[name].pr, probes[name].err
		steps := m.stepsOn(name)
		if err == nil && m.isDown(name) && m.HostPreflight != nil {
			// A host that was unreachable takes work again only once its preflight passes.
			_, perr := bounded(ctx, hostCallBound, "preflight of "+name, func(c context.Context) (struct{}, error) { return none(m.HostPreflight(c, name)) })
			if perr != nil {
				err = fmt.Errorf("preflight failed, not admitted: %v", perr)
			}
		}
		st := HostStatus{At: m.now().UTC(), Load5: pr.Load5, Free: pr.Free, Inflight: pr.Inflight}
		// Only a reading of the host's kernel feeds the health gate. A host that does not answer at all is the
		// unreachable state (bound.go), which has its own recovery.
		if pr.Canary != nil {
			m.observeHealth(name, pr.Canary)
		}
		if why := m.hostBlock(name); why != "" {
			st.Unhealthy = strings.TrimPrefix(why, "host "+name+" is ")
		}
		st.Warnings = m.warns[name]
		if err != nil {
			since := m.markDown(name)
			st.Error, st.DownSince = err.Error(), since
			m.hosts[name] = st
			m.hostLost(name, since, err)
			continue
		}
		if m.isDown(name) {
			m.downMu.Lock()
			delete(m.down, name)
			m.downMu.Unlock()
			m.alarmClear(AlarmHostUnreachable, name)
			m.logf("host %s answers again and is admitted", name)
		}
		reachable[name] = true
		m.hosts[name] = st
		if min := m.inv.Hosts[name].MinFreeBytes(); min > 0 && pr.Free > 0 && pr.Free < min {
			m.alarm(AlarmDiskFloor, name, fmt.Sprintf("%s free, below the floor %s: no new build starts until it is above", size(pr.Free), m.inv.Hosts[name].MinFree))
		} else {
			m.alarmClear(AlarmDiskFloor, name)
		}
		for lane, state := range pr.Steps {
			// Only this root's lanes with a step in flight count: a host shared with another bench root, or an
			// older host binary, may report others, and a settled step may still be on disk.
			l := m.lanes[lane]
			if l == nil {
				continue
			}
			if l.Step == nil && l.Orphan != nil && l.Orphan.ID == steps[lane] {
				m.settleOrphan(ctx, l, state)
				continue
			}
			if l.Step == nil || l.Step.ID != steps[lane] {
				continue
			}
			switch state {
			case StepFinished:
				id := l.Step.ID
				if err := m.adopt(ctx, l); err != nil {
					m.logf("lane %s: adopt %s: %v (retried next pass: the step is still on the lane)", lane, id, err)
				}
			case StepDead:
				m.died(ctx, l)
			case StepRunning:
				l.Step.Phase = pr.Phases[lane]
				m.cancelRemoved(ctx, l)
				m.enforceBound(ctx, l)
			}
		}
	}
	m.observeCrashLoops(ctx, reachable)
	for _, l := range m.sortedLanes() {
		m.unhold(ctx, l)
	}
	// admit: the hosts that may take new work (reachable and healthy). An unhealthy host keeps what it runs; its
	// finished steps are still adopted and its dead ones recovered, but nothing new starts on it.
	admit := map[string]bool{}
	for name := range reachable {
		admit[name] = m.admitted(name)
	}
	for _, l := range m.sortedLanes() {
		if admit[l.Host] && l.Step == nil && l.State == LaneReady {
			m.continueSeq(ctx, l)
		}
	}
	m.resetDown()
	m.gcWanted = map[string]bool{}
	gcAll := fileExists(m.Root.path("gc.request"))
	m.assign(ctx, admit)
	for _, l := range m.sortedLanes() {
		if !admit[l.Host] || l.Step != nil || l.State != LaneFree {
			continue
		}
		if m.churnFull(l.Host) {
			continue // a release or a reset waits for a free churn slot; the next pass tries again
		}
		del := !l.Gone && m.inv.Hosts[l.Host].ClusterDown != "" && !m.wanted(l.Host) &&
			(gcAll || m.gcWanted[l.Host] || m.ownerDone(l))
		switch {
		case del:
			m.start(ctx, l, Step{Kind: StepRelease, Prev: l.Prepared, DeleteCluster: true})
		case l.Prepared != nil && l.Prepared.Hooks != "" && !l.Prepared.Reset:
			// A lane already returned to its golden copy has nothing left to release; releasing it again only
			// repeats the reset (minutes of host load and a lane that is never idle for a build).
			m.start(ctx, l, Step{Kind: StepRelease, Prev: l.Prepared})
		}
	}
	if gcAll {
		for name := range m.inv.Hosts {
			if reachable[name] {
				m.gcHost(ctx, name)
			}
		}
		_ = os.Remove(m.Root.path("gc.request"))
		m.logf("gc: done")
	}
	for _, l := range m.lanes {
		if err := writeJSON(m.Root.LanePath(l.Lane), l); err != nil {
			return err
		}
	}
	for name, st := range m.hosts {
		st.Builds = m.builds(name)
		m.hosts[name] = st
	}
	err := writeJSON(m.Root.path("hosts.status.json"), m.hosts)
	m.tickPlans(ctx)
	return err
}

// tickPlans runs one pass of every plan under <root>/plans (internal/plan): it releases ready nodes as experiments,
// which the next pass of this manager picks up. A plan that does not load is logged and left alone.
func (m *Manager) tickPlans(ctx context.Context) {
	for _, name := range plan.Names(m.Root.PlansDir()) {
		if ctl, err := plan.LoadControl(plan.Dir(m.Root.PlansDir(), name)); err == nil && ctl.Purge {
			if done, err := purgePlan(m.Root, name); err != nil {
				m.logf("plan %s: removal: %v", name, err)
			} else if done {
				m.logf("plan %s removed; its name is free", name)
			}
			continue
		}
		e, err := plan.Open(plan.Dir(m.Root.PlansDir(), name), &PlanBackend{Root: m.Root, Inv: m.inv, Log: m.Log, Grading: &m.planGrading}, SystemClock{})
		if err != nil {
			m.logf("plan %s: %v", name, err)
			continue
		}
		e.Logf = m.logf
		hook := ""
		if e.Plan.Alarms != nil {
			hook = e.Plan.Alarms.Hook
		}
		e.Alarms = &AlarmLog{Root: m.Root, Hook: hook, Logf: m.logf}
		if err := e.Tick(ctx); err != nil {
			m.logf("plan %s: %v", name, err)
		}
	}
}

// InventoryErrorFile is <root>/inventory.error: why hosts.yaml was refused at the manager's last pass (absent when it
// loaded). `status` shows it.
const InventoryErrorFile = "inventory.error"

// loadInventory reads hosts.yaml. One that does not load (a bad edit) does not stop the manager: it keeps the last
// inventory that loaded, so steps are still adopted and lanes still scheduled, and says why in bench.log (once per
// distinct error) and in inventory.error until hosts.yaml loads again. With no good inventory yet the pass fails.
func (m *Manager) loadInventory() error {
	inv, err := LoadInventory(string(m.Root))
	errPath := m.Root.path(InventoryErrorFile)
	if err == nil {
		if m.invErr != "" {
			m.logf("hosts.yaml loads again")
		}
		m.inv, m.invGood, m.invErr = inv, true, ""
		_ = os.Remove(errPath)
		return nil
	}
	if !m.invGood {
		return err
	}
	if err.Error() != m.invErr {
		m.logf("hosts.yaml refused, the last inventory that loaded is used until it is fixed: %v", err)
	}
	m.invErr = err.Error()
	return writeFile(errPath, []byte(m.now().UTC().Format(time.RFC3339)+" "+m.invErr+"\n"))
}

func (m *Manager) load() error {
	names, err := m.Root.Experiments()
	if err != nil {
		return err
	}
	m.exps = map[string]*Experiment{}
	for _, n := range names {
		x, err := m.Root.LoadExperiment(n)
		if err != nil {
			m.logf("experiment %s: %v", n, err)
			continue
		}
		m.exps[n] = x
	}
	m.lanes = map[string]*LaneFile{}
	for _, ln := range m.inv.Lanes() {
		// A lane the bench has not seen yet, on a host that makes its clusters: its first build makes it
		// (kind-lane.sh up is idempotent, and refuses a cluster made without the evidence mount).
		l := &LaneFile{Lane: ln.Cluster, Host: ln.Host, State: LaneFree, Since: m.now().UTC()}
		switch err := readJSON(m.Root.LanePath(ln.Cluster), l); {
		case errors.Is(err, os.ErrNotExist):
			l.Gone = m.inv.Hosts[ln.Host].ClusterUp != ""
		case err != nil:
			return err
		}
		l.Host = ln.Host
		m.lanes[ln.Cluster] = l
	}
	for _, l := range m.lanes {
		// A lane prepared before the workload and checkout were recorded: they are the spec's that prepared it
		// (the experiment the lane's prepared state names), not a guess among the host's checkouts.
		if p := l.Prepared; p != nil && p.FuzzopsRev == "" {
			if x := m.exps[p.Experiment]; x != nil {
				p.Workload, p.FuzzopsRev = x.Measured().Workload, x.Measured().Fuzzops
			}
		}
	}
	m.retireLeft()
	return nil
}

// retireLeft closes the state of a lane that left hosts.yaml: bench.log gets its last transition (to retired), so
// lane-time totals stop counting it, and its files move to lanes/retired. A lane with a step in flight waits.
func (m *Manager) retireLeft() {
	ents, err := os.ReadDir(m.Root.path("lanes"))
	if err != nil {
		return
	}
	for _, e := range ents {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".json") {
			continue
		}
		lane := strings.TrimSuffix(name, ".json")
		if m.lanes[lane] != nil {
			continue
		}
		var lf LaneFile
		if readJSON(m.Root.LanePath(lane), &lf) != nil || lf.Step != nil {
			continue
		}
		m.logf("lane %s: %s -> %s left hosts.yaml", lane, lf.State, LaneRetired)
		dir := m.Root.path("lanes", "retired")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			continue
		}
		for _, f := range []string{name, lane + ".timeline.jsonl"} {
			if err := os.Rename(m.Root.path("lanes", f), filepath.Join(dir, f)); err != nil && !os.IsNotExist(err) {
				m.logf("lane %s: move %s: %v", lane, f, err)
			}
		}
	}
}

// downRetryAfter is how long a lane stays down before the manager resets it (a var for tests); maxAutoResets is how
// many times it does before it retires the lane.
var downRetryAfter = 10 * time.Minute

const maxAutoResets = 2

// resetDown returns lanes that went down to use without a human: recreated (a lane-specific cause lives in its
// cluster or its record), at most maxAutoResets times in a row; then the lane is retired and the reason logged.
func (m *Manager) resetDown() {
	now := m.now().UTC()
	for _, l := range m.sortedLanes() {
		if l.State != LaneDown || l.Step != nil || now.Sub(l.Since) < downRetryAfter {
			continue
		}
		if l.AutoResets >= maxAutoResets {
			m.setState(l, LaneRetired, fmt.Sprintf("down after %d automatic resets: %s", l.AutoResets, l.Reason))
			continue
		}
		l.AutoResets++
		reason := fmt.Sprintf("auto-reset %d/%d after: %s", l.AutoResets, maxAutoResets, l.Reason)
		l.Experiment, l.Seq, l.BuildFailures, l.GateFailures = "", nil, 0, 0
		l.Prepared = nil
		l.Recreate = m.inv.Hosts[l.Host].ClusterUp != ""
		m.setState(l, LaneFree, reason)
	}
}

func (m *Manager) sortedLanes() []*LaneFile {
	var out []*LaneFile
	for _, l := range m.lanes {
		out = append(out, l)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Host+"/"+out[i].Lane < out[j].Host+"/"+out[j].Lane })
	return out
}

func (m *Manager) setState(l *LaneFile, state, reason string) {
	if l.State != state || l.Reason != reason {
		m.logf("lane %s: %s -> %s %s", l.Lane, l.State, state, reason)
	}
	l.State, l.Reason, l.Since = state, reason, m.now().UTC()
}

// unbind frees the lane from its sequence (its operator stays until the next build or release).
func (m *Manager) unbind(l *LaneFile, reason string) {
	l.Experiment, l.Seq, l.Attempt, l.Position = "", nil, 0, 0
	m.setState(l, LaneFree, reason)
}

// builds is the host's churn: the steps that create, reset or delete a lane's cluster or app (a build, a release, an
// incident reset, a recover). Each takes a network namespace under the kernel's global lock, so max_builds bounds them
// together. A recover (the cleanup of a step that died) counts but is never held back: what a dead step left must be undone.
func (m *Manager) builds(host string) int {
	n := 0
	for _, l := range m.lanes {
		if l.Host != host || l.Step == nil {
			continue
		}
		switch l.Step.Kind {
		case StepBuild, StepRelease, StepUnhold, StepRecover:
			n++
		}
	}
	return n
}

// churnFull says whether host h already runs max_builds churn steps (a new release or reset waits).
func (m *Manager) churnFull(h string) bool { return m.builds(h) >= m.inv.Hosts[h].MaxBuilds }

// start fills in the lane's part of a step and starts it on the lane's host.
func (m *Manager) start(ctx context.Context, l *LaneFile, s Step) bool {
	if why := m.crashBlock(l.Lane); why != "" && (s.Kind == StepBuild || s.Kind == StepRun) {
		return false // no new work on a lane whose pods crash-loop; recover, release and unhold still run
	}
	h := m.inv.Hosts[l.Host]
	s.ID = NewStepID(s.Kind)
	s.Lane, s.Host = l.Lane, l.Host
	s.Kubeconfig = filepath.Join(h.Kubeconfigs, l.Lane)
	s.LaneDir = filepath.Join(h.Staging, l.Lane, "lane")
	s.Poll = Seconds(m.Poll.Seconds())
	s.ClusterUp, s.ClusterDown = h.ClusterCommand(h.ClusterUp, l.Lane), h.ClusterCommand(h.ClusterDown, l.Lane)
	s.Evidence = h.LaneEvidence(l.Lane)
	s.HostEnv = h.EnvList()
	s.Provider = h.Provider
	// The lane's share of the host: from the experiment the step is for (a release has none, and only gives cores back).
	var limitSpec Spec
	if x := m.exps[s.Experiment]; x != nil {
		limitSpec = x.Measured()
	} else if x := m.exps[l.Experiment]; x != nil {
		limitSpec = x.Measured()
	}
	mode, block, _ := isolationOf(h, limitSpec, m.reservations, l.Host, nil)
	s.Limits = laneLimits(h, limitSpec, block)
	s.IsolationMode = mode
	if s.Cap == 0 && s.Prev != nil {
		// A release or build tearing down another experiment's operator waits for it at most that experiment's cap.
		if x := m.exps[s.Prev.Experiment]; x != nil {
			_, _, c := x.Measured().Durations()
			s.Cap = Seconds(c.Seconds())
		}
	}
	if s.Fuzzops == "" {
		// A step without an experiment (release, recover, unhold) works with what the lane was prepared with.
		prep := s.Prev
		if prep == nil {
			prep = l.Prepared
		}
		if prep == nil || prep.FuzzopsRev == "" {
			m.logf("lane %s: %s refused: no recorded workload and fuzzops checkout for the lane (it was prepared before they were recorded)", l.Lane, s.Kind)
			return false
		}
		s.Fuzzops = m.inv.FuzzopsPath(prep.FuzzopsRev, l.Host)
		if s.Fuzzops == "" {
			m.logf("lane %s: %s refused: the lane's fuzzops %s has no checkout on %s", l.Lane, s.Kind, prep.FuzzopsRev, l.Host)
			return false
		}
		s.Workload, s.FuzzopsRev = prep.Workload, prep.FuzzopsRev
	}
	if err := m.ops(l.Host).Start(ctx, s); err != nil {
		// It may have started all the same (an ssh that timed out after the step began): the
		// probe decides. A step that never ran shows as dead and is followed by a recover step.
		m.logf("lane %s: start %s: %v (the probe decides whether it runs)", l.Lane, s.ID, err)
	}
	l.Step = &StepRef{ID: s.ID, Kind: s.Kind, Started: m.now().UTC(), Run: s.Run, Bound: Seconds(stepBound(s.Kind, limitSpec).Seconds())}
	if s.Run != nil {
		l.Step.RunID = s.Run.ID
	}
	m.logf("lane %s: started %s", l.Lane, s.ID)
	return true
}

// preparedFor is the operator and app a build of x puts on l (before it has run).
func (m *Manager) preparedFor(l *LaneFile, x *Experiment) *Prepared {
	s := x.Measured()
	p := &Prepared{Experiment: x.Name, Namespace: s.Namespace, AppPack: filepath.Join(m.inv.FuzzopsPath(s.Fuzzops, l.Host), s.App),
		Workload: s.Workload, FuzzopsRev: s.Fuzzops}
	if o := s.Operator; o.Hooks != "" {
		p.Hooks, p.HooksPath, p.Command = o.Hooks, m.inv.HooksPath(o.Hooks, o.Revision, l.Host), m.inv.HookCommand[o.Hooks]
	}
	return p
}

// expStep is the experiment part of a build or run step.
func (m *Manager) expStep(ctx context.Context, l *LaneFile, x *Experiment, kind string) (Step, error) {
	s := x.Measured()
	fz := m.inv.FuzzopsPath(s.Fuzzops, l.Host)
	rf, rt, cap := s.Durations()
	var gate time.Duration
	if s.GateAfterCheck != "" {
		gate, _ = time.ParseDuration(s.GateAfterCheck)
	}
	st := Step{Kind: kind, Experiment: x.Name, Fuzzops: fz, Workload: s.Workload, FuzzopsRev: s.Fuzzops, ReadyFor: Seconds(rf.Seconds()), GateAfterCheck: Seconds(gate.Seconds()),
		ReadyTimeout: Seconds(rt.Seconds()), Cap: Seconds(cap.Seconds())}
	if kind == StepBuild {
		b := s.BuildBad()
		st.BuildBad = &b
	}
	if kind == StepBuild {
		p := m.preparedFor(l, x)
		if o := s.Operator; o.Hooks != "" {
			op := &HookOperator{Hooks: o.Hooks, Revision: o.Revision, Settings: o.Settings}
			if x.Lock.Baseline != nil {
				path, err := m.ops(l.Host).Stage(ctx, m.Root, *x.Lock.Baseline)
				if err != nil {
					return st, fmt.Errorf("stage baseline: %w", err)
				}
				op.Baseline = path
			}
			if x.Lock.Seed != nil {
				path, err := m.ops(l.Host).Stage(ctx, m.Root, *x.Lock.Seed)
				if err != nil {
					return st, fmt.Errorf("stage seed: %w", err)
				}
				op.Seed = path
			}
			st.OperatorFile = op
		}
		st.New, st.Prev = p, l.Prepared
	} else {
		st.New = l.Prepared
	}
	return st, nil
}

// unhold returns a held lane to use once its run's evidence manifest lists no problems (the evidence was completed,
// or waived, and the manifest rewritten). First the operator reset collect skipped runs as a step (the lane keeps its
// held state meanwhile); then the lane continues its sequence when it still has one, else it is free. A failed reset
// leaves it held and is tried again after a minute.
func (m *Manager) unhold(ctx context.Context, l *LaneFile) {
	if l.State != LaneHeld || l.Step != nil || evidenceProblems(l.HeldRun) != "" {
		return
	}
	if m.now().Sub(l.Since) < time.Minute && strings.HasPrefix(l.Reason, "unhold failed") {
		return
	}
	if m.churnFull(l.Host) {
		return
	}
	if l.Prepared != nil && l.Prepared.Hooks != "" {
		m.logf("lane %s: evidence complete, resetting incidents", l.Lane)
		m.start(ctx, l, Step{Kind: StepUnhold, Prev: l.Prepared})
		return
	}
	m.release(l)
}

// release ends a hold: the lane continues its sequence, else it is free.
func (m *Manager) release(l *LaneFile) {
	m.logf("lane %s: evidence complete, hold released", l.Lane)
	l.HeldRun = ""
	if l.Seq != nil {
		m.setState(l, LaneReady, "")
		return
	}
	m.setState(l, LaneFree, "")
}

// continueSeq starts the next run of a ready lane's sequence, or frees the lane.
func (m *Manager) continueSeq(ctx context.Context, l *LaneFile) {
	x := m.exps[l.Experiment]
	if x == nil || x.Spec == nil || l.Seq == nil {
		m.unbind(l, "")
		return
	}
	p := x.Progress(*l.Seq)
	switch {
	case p.Ended() || p.Attempt != l.Attempt:
		m.unbind(l, "")
		return
	case x.Paused:
		return // the lane keeps its memory until resume
	}
	s := x.Measured()
	if s.Hold {
		return // a pool lane waits, prepared, for a claim
	}
	st, err := m.expStep(ctx, l, x, StepRun)
	if err != nil {
		m.logf("lane %s: %v", l.Lane, err)
		return
	}
	rs := &RunSpec{ID: RunID(p.Seq.Fault, p.Seq.Q, p.Next, p.Attempt), Fault: p.Seq.Fault, Seed: s.Seeds(p.Seq.Q)[p.Next-1],
		Position: p.Next, Label: s.OperatorLabel(p.Next), Flags: s.RunFlags()}
	if p.Seq.Fault != Healthy && x.Lock.Instances != nil {
		path, err := m.ops(l.Host).Stage(ctx, m.Root, *x.Lock.Instances)
		if err != nil {
			m.logf("lane %s: stage instances: %v", l.Lane, err)
			return
		}
		rs.Instances = path
	}
	st.Run = rs
	l.Position = p.Next
	if m.start(ctx, l, st) {
		m.setState(l, LaneRunning, "")
	}
}

// assign binds free lanes to the sequences of each experiment's current wave. Afterwards every free lane says why it
// was not bound while some experiment has queued sequences (explainIdle).
func (m *Manager) assign(ctx context.Context, reachable map[string]bool) {
	var names []string
	for n := range m.exps {
		names = append(names, n)
	}
	sort.Strings(names)
	// waiting: the experiments with queued sequences that took no (more) lanes this pass, and why ("": no lane
	// fit, which explainIdle says per lane).
	waiting := map[string]string{}
	for _, n := range names {
		x := m.exps[n]
		if x.Spec == nil {
			continue
		}
		bound := map[Seq]bool{}
		held := 0
		for _, l := range m.lanes {
			if l.Experiment == n && l.Seq != nil {
				bound[*l.Seq] = true
				held++
			}
		}
		if x.Paused {
			if queued(x, bound) {
				waiting[n] = "paused"
			}
			continue
		}
		s := x.Measured()
		wave := m.wave(x)
		why := "its current wave is still running"
		for _, q := range wave {
			p := x.Progress(q)
			if p.Ended() || bound[q] {
				continue
			}
			if held >= s.Lanes {
				why = fmt.Sprintf("it holds its lanes: %d", s.Lanes)
				break
			}
			l, swap := m.claimable(x, s), true
			if l == nil {
				l = m.resettable(x, s, reachable)
			}
			if l == nil && !s.ClaimWaits() {
				// A claimant that waits for its pool never builds a lane of its own: the pool builds the lanes.
				l, swap = m.freeLane(n, s, reachable), false
			}
			if l == nil {
				why = ""
				if s.ClaimWaits() {
					why = fmt.Sprintf("it waits for a lane of its pool %s (claim_only)", s.Claim)
					if m.claimWaitExceeded(x, s) {
						why = "paused: claim wait exceeded"
					}
				}
				break
			}
			delete(m.claimSince, n)
			st, err := m.expStep(ctx, l, x, StepBuild)
			if err != nil {
				m.logf("lane %s: %v", l.Lane, err)
				why = err.Error()
				break
			}
			l.Experiment, l.Seq, l.Attempt, l.Position = n, &q, p.Attempt, 1
			st.Create = l.Gone
			st.Recreate = !l.Gone && (l.Recreate || s.NeedsFreshCluster(q.Fault))
			st.New.Fault = q.Fault
			st.ReuseApp = reusableApp(s, st, q.Fault)
			st.Swap = swap
			st.Hold = s.Hold
			if !m.start(ctx, l, st) {
				m.unbind(l, "")
				why = "its build could not start"
				break
			}
			m.setState(l, LaneBuilding, "")
			bound[q] = true
			held++
		}
		if queued(x, bound) {
			waiting[n] = why
		}
	}
	m.explainIdle(waiting, reachable)
}

// claimWaitExceeded starts or checks the clock of a claimant that has a sequence and no pool lane. Past the spec's
// bound it pauses the experiment with a named error (the pause mark, bench.log) instead of waiting for ever.
func (m *Manager) claimWaitExceeded(x *Experiment, s Spec) bool {
	if m.claimSince == nil {
		m.claimSince = map[string]time.Time{}
	}
	since, ok := m.claimSince[x.Name]
	if !ok {
		m.claimSince[x.Name] = m.now()
		return false
	}
	waited := m.now().Sub(since)
	if waited < s.ClaimWaitBound() {
		return false
	}
	delete(m.claimSince, x.Name)
	msg := fmt.Sprintf("claim wait exceeded: %s waited %s for a lane of pool %s (bound %s); resume it once the pool has lanes", x.Name, waited.Round(time.Second), s.Claim, s.ClaimWaitBound())
	m.logf("%s", msg)
	if err := writeFile(m.Root.PausedPath(x.Name), []byte("paused "+m.now().UTC().Format(time.RFC3339)+": "+msg+"\n")); err != nil {
		m.logf("%s: pause: %v", x.Name, err)
	}
	x.Paused = true
	return true
}

// queued reports whether x has a sequence that has not ended and holds no lane.
func queued(x *Experiment, bound map[Seq]bool) bool {
	for _, q := range x.AllSeqs() {
		if !bound[q] && !x.Progress(q).Ended() {
			return true
		}
	}
	return false
}

// idleLogEvery is how long a free lane may stay unbound while sequences are queued before the log says why (and
// how often it says it again).
const idleLogEvery = 10 * time.Minute

// explainIdle records on every free lane why none of the waiting experiments took it (LaneFile.Idle, shown by
// `status`), from the checks assign itself makes (laneBlock), and logs it when the lane stays idle that way longer
// than idleLogEvery. A lane is explained only while some experiment has queued sequences.
func (m *Manager) explainIdle(waiting map[string]string, reachable map[string]bool) {
	var names []string
	for n := range waiting {
		names = append(names, n)
	}
	sort.Strings(names)
	now := m.now().UTC()
	for _, l := range m.sortedLanes() {
		if l.State != LaneFree || l.Step != nil || len(names) == 0 {
			l.Idle, l.IdleLogged = "", time.Time{}
			continue
		}
		byWhy := map[string][]string{}
		var order []string
		for _, n := range names {
			why := waiting[n]
			if why == "" {
				x := m.exps[n]
				why = m.laneBlock(l, x, x.Measured(), reachable)
			}
			if why == "" {
				why = "nothing (bound at the next pass)"
			}
			if byWhy[why] == nil {
				order = append(order, why)
			}
			byWhy[why] = append(byWhy[why], n)
		}
		var parts []string
		for _, why := range order {
			ns := byWhy[why]
			who := strings.Join(ns, ", ")
			if len(ns) > 3 {
				who = fmt.Sprintf("%s +%d", strings.Join(ns[:3], ", "), len(ns)-3)
			}
			parts = append(parts, fmt.Sprintf("%s [%s]", why, who))
		}
		l.Idle = strings.Join(parts, "; ")
		if now.Sub(l.Since) >= idleLogEvery && now.Sub(l.IdleLogged) >= idleLogEvery {
			m.logf("lane %s: free for %s while sequences are queued: %s", l.Lane, now.Sub(l.Since).Round(time.Minute), l.Idle)
			l.IdleLogged = now
		}
	}
}

// laneBlock says why the free lane l was not bound to a sequence of x this pass ("" when nothing stood in the way):
// the checks of resettable and freeLane (a ready pool lane that claimable takes is not free).
func (m *Manager) laneBlock(l *LaneFile, x *Experiment, s Spec, reachable map[string]bool) string {
	if why := m.crashBlock(l.Lane); why != "" {
		return why
	}
	if why := m.buildBlock(l.Host, s, reachable); why != "" {
		return why
	}
	if m.resetFits(l, x, s) {
		return ""
	}
	if awaitsRelease(l) {
		return "its pool operator is returned to its golden copy first"
	}
	why, _ := m.freshBlock(l.Host)
	return why
}

// buildBlock says why no build of s may start on host h now ("" when one may): the host is not one of the spec's
// usable hosts, is unreachable, or already runs its max_builds builds. Every way of binding a free lane checks it.
func (m *Manager) buildBlock(h string, s Spec, reachable map[string]bool) string {
	hs := m.inv.Hosts[h]
	switch {
	case !slices.Contains(s.Hosts, h):
		return fmt.Sprintf("host %s is not in the spec's hosts", h)
	case m.hostBlock(h) != "":
		return m.hostBlock(h)
	case !slices.Contains(m.inv.UsableHosts(s), h):
		return fmt.Sprintf("host %s has no checkout of the spec's pinned fuzzops or hooks", h)
	case !reachable[h]:
		return fmt.Sprintf("host %s is unreachable: %s", h, m.hosts[h].Error)
	case m.builds(h) >= hs.MaxBuilds:
		return fmt.Sprintf("host %s runs max_builds (%d) builds", h, hs.MaxBuilds)
	}
	if _, _, why := isolationOf(hs, s, m.reservations, h, m.drift); why != "" {
		return why
	}
	return ""
}

// freshBlock says why no lane may be built anew on host h now ("" when one may): its 5-minute load is at max_load5
// or above, or its staging disk is below min_free (floor: garbage is collected first).
func (m *Manager) freshBlock(h string) (why string, floor bool) {
	hs := m.inv.Hosts[h]
	if hs.MaxLoad5 > 0 && m.hosts[h].Load5 >= hs.MaxLoad5 {
		return fmt.Sprintf("host %s load5 %.0f is at or above max_load5 %.0f", h, m.hosts[h].Load5, hs.MaxLoad5), false
	}
	if min := hs.MinFreeBytes(); min > 0 && m.hosts[h].Free > 0 && m.hosts[h].Free < min {
		return fmt.Sprintf("host %s has %s free, below its floor %s", h, size(m.hosts[h].Free), hs.MinFree), true
	}
	return "", false
}

// freeLane picks a free lane for experiment name on the spec's usable hosts (buildOrder), within the host's limits.
func (m *Manager) freeLane(name string, s Spec, reachable map[string]bool) *LaneFile {
	for _, h := range m.buildOrder(name, s) {
		if m.buildBlock(h, s, reachable) != "" {
			continue
		}
		if why, floor := m.freshBlock(h); why != "" {
			if floor && !m.gcWanted[h] {
				m.logf("%s: collecting garbage, then waiting", why)
			}
			m.gcWanted[h] = m.gcWanted[h] || floor
			continue
		}
		for _, l := range m.sortedLanes() {
			// A pool lane whose operator still runs is released first (by reset), so that the next build can take it by swap.
			if l.Host == h && l.State == LaneFree && l.Step == nil && !awaitsRelease(l) {
				return l
			}
		}
	}
	return nil
}

// buildOrder is the order in which a build of experiment name tries the spec's usable hosts: the spec's order, except
// that a pool (hold) goes first to the hosts where it has the fewest lanes, so that every host it may use gets one
// golden lane before any gets a second (claimants wait for a pool lane on the hosts they may use).
func (m *Manager) buildOrder(name string, s Spec) []string {
	hosts := m.inv.UsableHosts(s)
	if !s.Hold {
		return hosts
	}
	n := map[string]int{}
	for _, l := range m.lanes {
		if m.poolOf(l) == name {
			n[l.Host]++
		}
	}
	sort.SliceStable(hosts, func(i, j int) bool { return n[hosts[i]] < n[hosts[j]] })
	return hosts
}

// poolOf is the pool a lane belongs to ("" when none): the hold experiment bound to it, or the pool of the claimant
// that holds it or of the golden copy prepared on it (a claimed lane stays its pool's lane).
func (m *Manager) poolOf(l *LaneFile) string {
	pool := func(name string) string {
		x := m.exps[name]
		if x == nil || x.Spec == nil {
			return ""
		}
		if s := x.Measured(); s.Hold {
			return name
		} else if s.Claim != "" {
			return s.Claim
		}
		return ""
	}
	if p := pool(l.Experiment); p != "" {
		return p
	}
	if l.Prepared != nil && l.Prepared.Golden {
		return pool(l.Prepared.Experiment)
	}
	return ""
}

// wave returns the experiment's current wave: the next `lanes` sequences,
// formed only after every sequence of the last wave ended. With
// pause_after_wave the experiment pauses between waves.
func (m *Manager) wave(x *Experiment) []Seq {
	waves, err := m.Root.Waves(x.Name)
	if err != nil {
		m.logf("%s: waves: %v", x.Name, err)
		return nil
	}
	if n := len(waves); n > 0 {
		last := &waves[n-1]
		for _, q := range last.Seqs {
			if !x.Progress(q).Ended() {
				return last.Seqs
			}
		}
		if x.Measured().PauseAfterWave && !last.Paused {
			last.Paused = true
			if err := writeJSON(m.Root.WavesPath(x.Name), waves); err == nil {
				if err := Pause(m.Root, x.Name); err == nil {
					m.logf("%s: wave %d ended, paused (resume to go on)", x.Name, n)
				}
			}
			return nil
		}
	}
	in := map[Seq]bool{}
	for _, w := range waves {
		for _, q := range w.Seqs {
			in[q] = true
		}
	}
	var next []Seq
	s := x.Measured()
	for _, q := range s.Seqs() {
		if len(next) < s.Lanes && !in[q] && !x.Progress(q).Ended() {
			next = append(next, q)
		}
	}
	if len(next) == 0 {
		return nil
	}
	waves = append(waves, Wave{Seqs: next, Started: m.now().UTC()})
	if err := writeJSON(m.Root.WavesPath(x.Name), waves); err != nil {
		m.logf("%s: waves: %v", x.Name, err)
		return nil
	}
	m.logf("%s: wave %d: %d sequences", x.Name, len(waves), len(next))
	return next
}

// died handles a step that was killed: a recover step undoes what it may have left.
func (m *Manager) died(ctx context.Context, l *LaneFile) {
	dead := l.Step
	m.logf("lane %s: step %s died %s", l.Lane, dead.ID, dead.Killed)
	l.Step = nil
	if dead.Kind == StepRecover {
		m.setState(l, LaneDown, "recover step died")
		return
	}
	if dead.Kind == StepRun && l.Experiment != "" && l.Seq != nil {
		if x := m.exps[l.Experiment]; x != nil {
			reason := "lane lost: step died"
			if dead.Killed != "" {
				reason = "lane lost: step " + dead.Killed
			}
			m.endRun(x, l, dead.Run, Result{Status: StatusInvalid, Reason: reason})
		}
	}
	if dead.Kind == StepBuild {
		l.BuildFailures++
	}
	prev := l.Prepared
	if dead.Kind == StepBuild && prev == nil {
		// The build may have prepared its operator before it died: recover tears it down at the next build.
		if x := m.exps[l.Experiment]; x != nil {
			if st, err := m.expStep(ctx, l, x, StepBuild); err == nil {
				prev = st.New
				l.Prepared = st.New
			}
		}
	}
	m.unbind(l, "step died: "+dead.ID)
	if err := m.ops(l.Host).Clean(ctx, l.Lane, dead.ID); err != nil {
		m.logf("lane %s: clean %s: %v", l.Lane, dead.ID, err)
	}
	m.start(ctx, l, Step{Kind: StepRecover, Prev: prev, Run: dead.Run})
}

// adopt brings a finished step home and moves the lane on.
func (m *Manager) adopt(ctx context.Context, l *LaneFile) error {
	ref := l.Step
	ops := m.ops(l.Host)
	var res Result
	tmp := m.Root.path("lanes", l.Lane+".steps", ref.ID)
	if err := ops.Fetch(ctx, l.Lane, ref.ID, tmp); err != nil {
		return err
	}
	if err := readJSON(filepath.Join(tmp, "result.json"), &res); err != nil {
		return err
	}
	ivs, _ := readJSONL[Interval](filepath.Join(tmp, "timeline.jsonl"))
	x := m.exps[l.Experiment]
	oom := false
	if ref.Kind == StepRun && x != nil {
		res, oom = judgeOOM(x.Measured(), res)
	}
	if ref.Kind == StepRun && x != nil {
		// A run's files go to its run directory (run.json last, below); a not-ready try's apart.
		dest := m.Root.path(x.Name, "not-ready", ref.ID)
		if ref.Run != nil && res.Status != StatusNotReady {
			dest = m.Root.RunDir(x.Name, ref.Run.ID)
		}
		_ = os.RemoveAll(dest)
		if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
			return err
		}
		if err := os.Rename(tmp, dest); err != nil {
			return err
		}
	}
	for _, iv := range ivs {
		_ = appendJSONL(m.Root.TimelinePath(l.Lane), iv)
	}
	l.Step = nil
	m.logf("lane %s: %s finished ok=%v %s", l.Lane, ref.ID, res.OK, res.Reason)
	if res.Gate != nil && x != nil {
		_ = appendJSONL(m.Root.GatesPath(x.Name), res.Gate)
	}
	before := l.Prepared
	l.Prepared = res.Prepared
	if res.Transcripts != "" {
		l.Transcripts = res.Transcripts
		m.logf("lane %s: transcripts missing after teardown: %s", l.Lane, res.Transcripts)
		_ = appendJSONL(m.Root.path("transcripts.jsonl"), map[string]any{"lane": l.Lane, "host": l.Host, "step": ref.ID, "at": m.now().UTC(), "check": res.Transcripts})
	}
	switch ref.Kind {
	case StepBuild:
		switch {
		case res.LaneDown != "":
		case res.OK:
			l.BuildFailures, l.AutoResets = 0, 0
			l.Gone, l.Recreate = false, false
			m.setState(l, LaneReady, "")
			if x != nil && x.Measured().Claim != "" && freshCopy(ivs) {
				// A claimant that paid a whole build (no pool lane, or one whose cluster was gone): counted in status.
				m.logf("lane %s: claimant %s runs on a fresh build (an app copy, not a pool lane)", l.Lane, x.Name)
				_ = appendJSONL(m.Root.path(freshBuildsFile), map[string]any{"experiment": x.Name, "pool": x.Measured().Claim,
					"lane": l.Lane, "host": l.Host, "step": ref.ID, "at": m.now().UTC()})
			}
		case res.Recycle != "":
			// Not claimable (nothing was swapped): counted and logged, not a build failure of the lane; the release
			// returns it by reset, else rebuilds it.
			m.logf("lane %s: pool lane recycled, not claimed: %s", l.Lane, res.Recycle)
			_ = appendJSONL(m.Root.path("recycled.jsonl"), map[string]any{"lane": l.Lane, "host": l.Host, "pool": res.Prepared.Experiment, "at": m.now().UTC(), "why": res.Recycle})
			m.unbind(l, "pool lane recycled: "+res.Recycle)
		default:
			l.BuildFailures++
			if res.Fresh {
				l.Gone, l.Recreate = false, false
			}
			if l.BuildFailures >= m.inv.Hosts[l.Host].BuildFailureLimit() {
				res.LaneDown = fmt.Sprintf("%d builds failed in a row: %s", l.BuildFailures, res.Reason)
			} else {
				// An in-place reset that failed is tried next on a new cluster (when the host can make one); a
				// build refused for its spec (res.Config) did nothing to the cluster, which stays as it is.
				if res.Config == "" {
					l.Recreate = !res.Fresh && m.inv.Hosts[l.Host].ClusterUp != ""
				}
				m.unbind(l, "build failed: "+res.Reason)
			}
		}
	case StepRun:
		if x != nil {
			m.endRun(x, l, ref.Run, res)
		}
		switch {
		case res.LaneDown != "":
		case res.Status == StatusNotReady:
			l.GateFailures++
			switch {
			case l.GateFailures < 2:
				m.unbind(l, "not ready: rebuilt")
			case !l.GateRecreated && m.inv.Hosts[l.Host].ClusterUp != "":
				// Twice on a reused cluster: once more on a new one (once, until a run is done).
				l.GateFailures, l.Recreate, l.GateRecreated = 0, true, true
				m.unbind(l, "not ready twice: the cluster is made anew")
			default:
				res.LaneDown = fmt.Sprintf("%d not-ready gates in a row: %s", l.GateFailures, res.Reason)
			}
		case res.Status == StatusDone && len(res.Residue) > 0:
			// The cluster is not clean: the sequence cannot go on on it.
			l.GateFailures, l.Recreate = 0, true
			if x != nil {
				m.breakRest(x, l, ref.Run, "cluster-scoped residue: "+strings.Join(res.Residue, ", "))
			}
			m.unbind(l, "cluster-scoped residue: the cluster is made anew")
		case res.Status == StatusDone:
			l.GateFailures, l.GateRecreated = 0, false
			if x != nil && ref.Run != nil && l.Seq != nil && ref.Run.Position < len(x.Measured().Seeds(l.Seq.Q)) {
				l.Position = ref.Run.Position + 1
				m.setState(l, LaneReady, "")
			} else {
				m.unbind(l, "")
			}
			if res.EvidenceIncomplete != "" && x != nil && ref.Run != nil {
				l.HeldRun = filepath.Join(m.Root.RunDir(x.Name, ref.Run.ID), "operator", EvidenceManifest)
				m.logf("lane %s: evidence incomplete, lane held: %s", l.Lane, res.EvidenceIncomplete)
				m.setState(l, LaneHeld, "evidence incomplete: "+res.EvidenceIncomplete)
			}
		default:
			if len(res.Residue) > 0 || oom {
				// Residue, or processes in the nodes killed at their memory limit (the kubelet, etcd or containerd may
				// be among them): the next sequence starts on a new cluster.
				l.Recreate = true
			}
			m.unbind(l, "")
		}
	case StepRelease:
		if res.OK {
			if res.Prepared != nil && res.Prepared.Reset {
				// A pool lane went back to its golden copy: its operator stays, prepared and idle.
				l.Prepared = res.Prepared
			} else {
				// The operator is gone; the app copy stays for the next build to reuse (reuse_app), unless the cluster went.
				l.Prepared = appCopy(before, res.Gone)
			}
			l.Gone = res.Gone
		}
	case StepUnhold:
		if res.OK {
			m.release(l)
		} else {
			m.setState(l, LaneHeld, "unhold failed: "+res.Reason)
		}
	case StepRecover:
		if res.OK {
			if res.Gone {
				l.Gone, l.Recreate = true, false
			}
			m.setState(l, LaneFree, "recovered after "+l.Reason)
		}
	}
	if res.LaneDown != "" {
		l.Experiment, l.Seq = "", nil
		m.setState(l, LaneDown, res.LaneDown)
	}
	// The step is home and the lane has moved on, so a staging directory that cannot be removed now (an ssh that
	// drops, a step still writing) is not an adopt failure: adopting again would find no step. GC takes it later.
	if err := ops.Clean(ctx, l.Lane, ref.ID); err != nil {
		m.logf("lane %s: clean %s: %v (adopted; staging left for gc)", l.Lane, ref.ID, err)
	}
	return nil
}

// judgeOOM makes a done run whose lane had processes killed at its memory limit during the run invalid, unless the
// spec accepts OOM kills: the kernel kills a process of a node that reaches its limit (it may be the kubelet, etcd,
// containerd or the app), which changes what the run measured. An invalid run breaks its sequence, which is retried
// on a fresh lane like any invalid sequence. It reports whether the run was made invalid.
func judgeOOM(s Spec, res Result) (Result, bool) {
	if res.Status != StatusDone || res.Isolation == nil || res.Isolation.OOMKills <= 0 || s.AcceptOOMKills {
		return res, false
	}
	res.Status = StatusInvalid
	res.Reason = fmt.Sprintf("oom: %d process(es) in the lane's nodes killed at the lane's memory limit (lane.memory %s) during the run; set accept_oom_kills to keep such runs",
		res.Isolation.OOMKills, res.Isolation.Memory)
	return res, true
}

// endRun writes the run's run.json (terminal) and, when it broke its
// sequence, "sequence broken" for the positions after it. A not-ready
// first run stays queued; a not-ready repeat breaks its sequence (its memory is gone).
func (m *Manager) endRun(x *Experiment, l *LaneFile, rs *RunSpec, res Result) {
	if rs == nil || l.Seq == nil {
		return
	}
	status, reason := res.Status, res.Reason
	if status == StatusNotReady {
		if rs.Position == 1 {
			return
		}
		status, reason = StatusInvalid, "sequence broken: not ready: "+res.Reason
	}
	now := m.now().UTC()
	rf := RunFile{ID: rs.ID, Experiment: x.Name, Fault: rs.Fault, Sequence: l.Seq.Q, Position: rs.Position, Seed: rs.Seed,
		Attempt: l.Attempt, Status: status, Reason: reason, Lane: l.Lane, Host: l.Host, OperatorLabel: rs.Label, Fuzzops: res.Fuzzops,
		Started: res.Started, Ended: res.Ended, InjectAt: res.InjectAt, SettleEnd: res.SettleEnd, Capped: res.Capped, Isolation: res.Isolation}
	if res.Isolation != nil && res.Isolation.OOMKills > 0 {
		m.logf("run %s: %d OOM kill(s) in lane %s's nodes during the run: flagged in run.json (status %s)", rs.ID, res.Isolation.OOMKills, l.Lane, status)
	}
	if rf.Started.IsZero() {
		rf.Started, rf.Ended = now, now
	}
	m.writeRun(x, rf)
	if status != StatusDone {
		m.breakRest(x, l, rs, reason)
	}
}

// breakRest marks the positions after rs invalid "sequence broken: why".
func (m *Manager) breakRest(x *Experiment, l *LaneFile, rs *RunSpec, why string) {
	if rs == nil || l.Seq == nil {
		return
	}
	s, now := x.Measured(), m.now().UTC()
	seeds := s.Seeds(l.Seq.Q)
	for pos := rs.Position + 1; pos <= len(seeds); pos++ {
		m.writeRun(x, RunFile{ID: RunID(rs.Fault, l.Seq.Q, pos, l.Attempt), Experiment: x.Name, Fault: rs.Fault, Sequence: l.Seq.Q,
			Position: pos, Seed: seeds[pos-1], Attempt: l.Attempt, Status: StatusInvalid, Reason: "sequence broken: " + why,
			OperatorLabel: s.OperatorLabel(pos), Started: now, Ended: now})
	}
}

// wanted reports whether some experiment could build on host now (it has sequences left to run).
func (m *Manager) wanted(host string) bool {
	for _, x := range m.exps {
		if x.Spec == nil || x.Paused {
			continue
		}
		on := false
		for _, h := range m.inv.UsableHosts(x.Measured()) {
			on = on || h == host
		}
		if !on {
			continue
		}
		for _, q := range x.AllSeqs() {
			if !x.Progress(q).Ended() {
				return true
			}
		}
	}
	return false
}

// ownerDone reports whether the experiment last built on the lane has ended (finished or removed)
// and does not keep its lanes. A removed experiment keeps them when its spec did and the removal did not ask for a
// teardown (removed.json).
func (m *Manager) ownerDone(l *LaneFile) bool {
	if l.Prepared == nil {
		return false
	}
	x := m.exps[l.Prepared.Experiment]
	if x == nil {
		return true
	}
	if x.Spec == nil {
		return x.Removed == nil || !x.Removed.KeepLanes
	}
	if x.Measured().KeepLanes || x.Paused {
		return false
	}
	for _, q := range x.AllSeqs() {
		if !x.Progress(q).Ended() {
			return false
		}
	}
	return true
}

// gcHost removes from a host's staging what no lane and no experiment needs any more.
func (m *Manager) gcHost(ctx context.Context, host string) {
	var keep []string
	for _, l := range m.lanes {
		if l.Host != host {
			continue
		}
		if l.Step != nil {
			// The lock of a live step, whatever the lane's cluster: a lane's first build runs while it is Gone.
			keep = append(keep, filepath.Join(l.Lane, l.Step.ID), filepath.Join(l.Lane, "step.lock"))
		}
		if !l.Gone {
			keep = append(keep, filepath.Join(l.Lane, "lane"), filepath.Join(l.Lane, "step.lock"))
		}
	}
	for _, x := range m.exps {
		if x.Spec == nil {
			continue
		}
		for _, in := range []*Input{x.Lock.Instances, x.Lock.Baseline, x.Lock.Seed} {
			if in != nil {
				keep = append(keep, filepath.Join("inputs", in.SHA256))
			}
		}
	}
	removed, err := m.ops(host).GC(ctx, keep)
	if err != nil {
		m.logf("gc %s: %v", host, err)
		return
	}
	if len(removed) > 0 {
		m.logf("gc %s: removed %s", host, strings.Join(removed, ", "))
	}
}

func size(b int64) string { return fmt.Sprintf("%.0fG", float64(b)/(1<<30)) }

func (m *Manager) writeRun(x *Experiment, rf RunFile) {
	if err := writeJSON(filepath.Join(m.Root.RunDir(x.Name, rf.ID), "run.json"), rf); err != nil {
		m.logf("%s: run %s: %v", x.Name, rf.ID, err)
		return
	}
	x.Runs[rf.ID] = rf
	m.logf("%s: run %s %s %s", x.Name, rf.ID, rf.Status, rf.Reason)
}

// reusableApp: the build may keep the app copy on the lane when the experiment asks for it, the lane's last
// sequence (of any experiment: a kept lane is a warm pool) ran this app from the same pinned checkout in the same
// namespace and left the copy fit, and the cluster stays.
func reusableApp(s Spec, st Step, fault string) bool {
	prev := st.Prev
	return s.ReuseApp && !st.Create && !st.Recreate && !s.NeedsFreshApp(fault) &&
		prev != nil && prev.Ready &&
		prev.Namespace == st.New.Namespace && prev.AppPack == st.New.AppPack && !s.NeedsFreshApp(prev.Fault)
}

// freshBuildsFile lists the builds of claimants that made a fresh app copy instead of taking a pool lane.
const freshBuildsFile = "fresh-builds.jsonl"

// freshCopy reports whether a build's timeline holds a fresh app copy.
func freshCopy(ivs []Interval) bool {
	for _, iv := range ivs {
		if iv.Kind == "app" && iv.OK && iv.Detail == freshCopyDetail {
			return true
		}
	}
	return false
}

// appCopy is what a released lane remembers: the app copy the last sequence left, without its operator.
func appCopy(p *Prepared, gone bool) *Prepared {
	if p == nil || gone || !p.Ready {
		return nil
	}
	return &Prepared{Experiment: p.Experiment, Namespace: p.Namespace, AppPack: p.AppPack, Ready: true, Fault: p.Fault,
		Workload: p.Workload, FuzzopsRev: p.FuzzopsRev}
}

// claimable is a ready, unused lane of the Hold experiment x claims, on a host x can use, whose prepared operator
// matches x's in everything but the sources: a lane x takes by swap.
func (m *Manager) claimable(x *Experiment, s Spec) *LaneFile {
	if s.Claim == "" {
		return nil
	}
	pool := m.exps[s.Claim]
	if pool == nil || pool.Spec == nil || !pool.Measured().Hold || pool.Paused {
		return nil
	}
	for _, l := range m.sortedLanes() {
		if l.State != LaneReady || l.Step != nil || l.Experiment != s.Claim || l.Prepared == nil || !l.Prepared.Ready {
			continue
		}
		if !slices.Contains(m.inv.UsableHosts(s), l.Host) || !sameOperatorProfile(pool, x) {
			continue
		}
		return l
	}
	return nil
}

// awaitsRelease: a free lane whose pool operator (with a golden copy) is still on it; the release loop returns it to
// its golden copy by reset, or tears it down when that fails.
func awaitsRelease(l *LaneFile) bool {
	return l.Prepared != nil && l.Prepared.Hooks != "" && l.Prepared.Golden && !l.Prepared.Reset
}

// resettable is a free lane that a claimed run left at its golden copy (Prepared.Reset) whose operator profile is
// x's: the build takes it by swap, with no teardown, app copy or prepare. Subject to the host's build limits.
func (m *Manager) resettable(x *Experiment, s Spec, reachable map[string]bool) *LaneFile {
	for _, l := range m.sortedLanes() {
		if m.resetFits(l, x, s) && m.buildBlock(l.Host, s, reachable) == "" {
			return l
		}
	}
	return nil
}

// resetFits: l is a free lane at its golden copy whose operator profile is x's (resettable, without the host's limits).
func (m *Manager) resetFits(l *LaneFile, x *Experiment, s Spec) bool {
	if l.State != LaneFree || l.Step != nil || !resetAndReady(l) || l.Gone {
		return false
	}
	owner := m.exps[l.Prepared.Experiment]
	return owner != nil && owner.Spec != nil && sameOperatorProfile(owner, x)
}

// resetAndReady: the lane's operator was returned to its golden copy and passed its check. A swap needs both: a reset
// lane that is not ready (its last swap or check failed) holds no operator to swap into and is rebuilt instead.
func resetAndReady(l *LaneFile) bool {
	return l.Prepared != nil && l.Prepared.Reset && l.Prepared.Ready
}

// sameOperatorProfile: everything a swap cannot change on a prepared lane is equal: the app and where it runs, the
// fuzzops checkout, the hooks, the baseline memory and the operator settings (the sources' revision may differ).
func sameOperatorProfile(a, b *Experiment) bool {
	x, y := a.Measured(), b.Measured()
	if x.App != y.App || x.Fuzzops != y.Fuzzops || x.Namespace != y.Namespace ||
		x.Operator.Hooks == "" || x.Operator.Hooks != y.Operator.Hooks || !reflect.DeepEqual(x.Operator.Settings, y.Operator.Settings) {
		return false
	}
	return a.Lock.Baseline != nil && b.Lock.Baseline != nil && a.Lock.Baseline.SHA256 == b.Lock.Baseline.SHA256
}
