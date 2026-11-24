package bench

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// AddOption changes how Add validates.
type AddOption func(**RenderedChecks)

// WithChecks makes Add use a validation session that already holds answers (the manager shares one across the
// submits of a pass, so the hooks checkout and the hosts are asked once, not once per node).
func WithChecks(c *RenderedChecks) AddOption { return func(p **RenderedChecks) { *p = c } }

// Add validates a spec file and adds it: ValidateRendered (the rules `plan add` runs on every rendered spec), the
// hooks' own check of the stored inputs, and an unused name. Its inputs are stored by content and its
// identity frozen in spec.lock.yaml; then the spec is copied into specs/.
// Relative paths in the spec are read from the spec file's directory.
func Add(ctx context.Context, root Root, specFile string, log io.Writer, opts ...AddOption) error {
	s, err := LoadSpec(specFile)
	if err != nil {
		return err
	}
	release, err := lockAdds(root)
	if err != nil {
		return err
	}
	defer release()
	inv, err := LoadInventory(string(root))
	if err != nil {
		return err
	}
	for _, p := range []string{root.SpecPath(s.Name), root.path(s.Name)} {
		if _, err := os.Stat(p); err == nil {
			return fmt.Errorf("%s exists: experiment names are never reused", p)
		}
	}
	base := filepath.Dir(specFile)
	resolve := func(p string) string {
		if filepath.IsAbs(p) {
			return p
		}
		return filepath.Join(base, p)
	}
	// The rules on the spec are ValidateRendered's, the ones `plan add` runs on every rendered spec too.
	rs := s
	for _, p := range []*string{&rs.Faults.Instances, &rs.Operator.Baseline, &rs.Operator.Seed} {
		if *p != "" {
			*p = resolve(*p)
		}
	}
	checks := NewRenderedChecks(log)
	for _, o := range opts {
		o(&checks)
	}
	if err := ValidateRendered(ctx, inv, rs, checks); err != nil {
		return err
	}
	described, err := checks.describe(ctx, inv, s)
	if err != nil {
		return err
	}
	s.Described = described
	fp, err := checks.footprint(ctx, inv, s)
	if err != nil {
		return err
	}
	if fp != nil {
		s.Lane = fp
	}
	lock := Lock{Spec: s, Added: time.Now().UTC()}
	if s.Faults.Instances != "" {
		in, err := Store(string(root), resolve(s.Faults.Instances))
		if err != nil {
			return fmt.Errorf("faults.instances: %w", err)
		}
		lock.Instances = &in
	}
	if s.Operator.Baseline != "" {
		in, err := Store(string(root), resolve(s.Operator.Baseline))
		if err != nil {
			return fmt.Errorf("operator.baseline: %w", err)
		}
		lock.Baseline = &in
	}
	if s.Operator.Seed != "" {
		in, err := Store(string(root), resolve(s.Operator.Seed))
		if err != nil {
			return fmt.Errorf("operator.seed: %w", err)
		}
		lock.Seed = &in
	}
	// The hooks' own check runs on the stored inputs, the seed's content included: only a submit has them.
	if local := inv.LocalHost(); local != "" && s.Operator.Hooks != "" {
		if dir := inv.HooksPath(s.Operator.Hooks, s.Operator.Revision, local); dir != "" {
			if err := hookCheck(ctx, root, inv, s, lock, dir, log); err != nil {
				return err
			}
		}
	}
	if err := writeJSON(root.LockPath(s.Name), lock); err != nil {
		return err
	}
	b, err := os.ReadFile(specFile)
	if err != nil {
		return err
	}
	return writeFile(root.SpecPath(s.Name), b)
}

func hookCheck(ctx context.Context, root Root, inv Inventory, s Spec, lock Lock, dir string, log io.Writer) error {
	tmp, err := os.MkdirTemp("", "bench-check-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	op := HookOperator{Hooks: s.Operator.Hooks, Revision: s.Operator.Revision, Settings: s.Operator.Settings}
	if lock.Baseline != nil {
		op.Baseline = root.path(lock.Baseline.Path)
	}
	if lock.Seed != nil {
		op.Seed = root.path(lock.Seed.Path)
	}
	opPath := filepath.Join(tmp, "operator.json")
	if err := writeJSON(opPath, op); err != nil {
		return err
	}
	h := Hook{Dir: dir, Command: inv.HookCommand[s.Operator.Hooks], Env: []string{
		"BENCH_EXPERIMENT=" + s.Name, "FUZZOPS_NAMESPACE=" + s.Namespace, "BENCH_OPERATOR=" + opPath, "BENCH_OUT=" + tmp}}
	if r := h.Run(ctx, "check", nil, log); !r.OK() {
		return fmt.Errorf("hooks %s: %s", s.Operator.Hooks, r.Why("check"))
	}
	return nil
}

// checkLaneCapacity refuses a spec whose lane asks for more whole cores than any of its usable hosts' lane range
// holds, as far as hosts.yaml tells (a host without lanes.cpus, or a vcluster host, limits nothing; the range's
// hardware threads bound its cores). Foreign pins are live facts and are judged by the build itself.
func checkLaneCapacity(inv Inventory, s Spec) error {
	need := s.LaneLimit().CPUs
	var small []string
	for _, name := range inv.UsableHosts(s) {
		h := inv.Hosts[name]
		if h.IsVcluster() || h.IsEks() || !h.Lanes.Enabled() {
			return nil
		}
		cpus, err := h.Lanes.CPUList()
		if err != nil || len(cpus) >= need {
			return nil
		}
		small = append(small, fmt.Sprintf("%s (lanes.cpus %s)", name, h.Lanes.CPUs))
	}
	if len(small) == 0 {
		return nil
	}
	return fmt.Errorf("lane.cpus %d is more than the lane range of every usable host can ever hold: %s", need, strings.Join(small, ", "))
}

// checkPoolCapacity refuses a pool (hold) whose slot count (lanes) is above what its usable hosts' lane ranges can
// place: lane range / lane.cpus per host, as far as hosts.yaml tells (a host without lanes.cpus, or a vcluster host,
// limits nothing; the range's hardware threads bound its cores). A slot that cannot be placed builds for ever, waiting
// for cores while it holds a pool slot. Foreign pins are live facts and are judged by the build itself.
func checkPoolCapacity(inv Inventory, s Spec) error {
	if !s.Hold {
		return nil
	}
	per := s.LaneLimit().CPUs
	total := 0
	var parts []string
	for _, name := range inv.UsableHosts(s) {
		h := inv.Hosts[name]
		if h.IsVcluster() || h.IsEks() || !h.Lanes.Enabled() {
			return nil
		}
		cpus, err := h.Lanes.CPUList()
		if err != nil {
			return nil
		}
		n := len(cpus) / per
		total += n
		parts = append(parts, fmt.Sprintf("%s: %d cores (lanes.cpus %s) / %d per lane = %d", name, len(cpus), h.Lanes.CPUs, per, n))
	}
	if s.Lanes <= total {
		return nil
	}
	return fmt.Errorf("pool %s asks for %d slots but its hosts place only %d (%s); lower lanes to at most %d, or use more hosts", s.Name, s.Lanes, total, strings.Join(parts, "; "), total)
}

// Remove deletes an experiment's spec: its queued runs are dropped and runs in flight finish. Its lock and results
// stay. Its lanes return free: their operator is torn down and the app copy stays for a later reuse_app build or
// claim. An experiment with keep_lanes keeps their clusters as an ended one does; teardown ignores keep_lanes, so the
// clusters are deleted once no experiment needs their host (as for an experiment without keep_lanes). What remove
// decided is recorded in <exp>/removed.json before the spec goes, so the manager never sees it removed without it.
func Remove(root Root, name string, teardown bool) error {
	return remove(root, name, teardown, false)
}

// RemoveCancel is Remove that also kills the experiment's runs in flight (their lanes return free through the
// recover step, as for a dead step) and keeps the spec's keep_lanes: cancelling a run does not delete its cluster.
func RemoveCancel(root Root, name string) error {
	return remove(root, name, false, true)
}

func remove(root Root, name string, teardown, cancel bool) error {
	if _, err := os.Stat(root.SpecPath(name)); err != nil {
		return err
	}
	rm := Removal{At: time.Now().UTC(), Teardown: teardown, Cancel: cancel}
	if x, err := root.LoadExperiment(name); err == nil && x.Spec != nil {
		rm.KeepLanes = x.Measured().KeepLanes && !teardown
	}
	if err := writeJSON(root.RemovedPath(name), rm); err != nil {
		return err
	}
	if err := os.Remove(root.SpecPath(name)); err != nil {
		_ = os.Remove(root.RemovedPath(name))
		return err
	}
	_ = os.Remove(root.PausedPath(name))
	return nil
}

// Pause stops new runs of an experiment (runs in flight finish); Resume undoes it.
func Pause(root Root, name string) error {
	if _, err := os.Stat(root.SpecPath(name)); err != nil {
		return err
	}
	return writeFile(root.PausedPath(name), []byte("paused "+time.Now().UTC().Format(time.RFC3339)+"\n"))
}

// Resume deletes the pause mark.
func Resume(root Root, name string) error {
	err := os.Remove(root.PausedPath(name))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

// GC asks the manager to delete, on its next pass, every lane cluster no experiment needs (the
// host must have cluster_down) and the host staging that no lane or experiment uses. Results stay.
func GC(root Root) error {
	return writeFile(root.path("gc.request"), []byte(time.Now().UTC().Format(time.RFC3339)+"\n"))
}

// LaneReset returns a down lane to free after a human looked.
func LaneReset(root Root, lane string) error {
	var lf LaneFile
	if err := readJSON(root.LanePath(lane), &lf); err != nil {
		return err
	}
	if lf.State != LaneDown && lf.State != LaneRetired {
		return fmt.Errorf("lane %s is %s, not down or retired", lane, lf.State)
	}
	lf.AutoResets = 0
	lf.State, lf.Reason, lf.Since = LaneFree, "reset by hand after: "+lf.Reason, time.Now().UTC()
	lf.Experiment, lf.Seq, lf.Step, lf.BuildFailures, lf.GateFailures = "", nil, nil, 0, 0
	// What the lane file says is installed may not be: the next build checks the cluster and starts from nothing.
	lf.Prepared = nil
	return writeJSON(root.LanePath(lane), lf)
}

// releasePath is the request `lane release` leaves for the manager.
func (r Root) releasePath(lane string) string { return r.path("release-requests", lane) }

// LaneRelease asks the manager to release a free lane now: its cluster is deleted on the next pass whatever waits for
// the lane and however long it has been free (a lane is otherwise kept for idle_timeout and for waiting work). It
// refuses a lane that is not free; a request the manager finds the lane busy for is dropped, never kept for later.
func LaneRelease(root Root, lane string) error {
	var lf LaneFile
	if err := readJSON(root.LanePath(lane), &lf); err != nil {
		return err
	}
	if lf.State != LaneFree || lf.Step != nil {
		return fmt.Errorf("lane %s is %s, not free", lane, lf.State)
	}
	if lf.Gone {
		return fmt.Errorf("lane %s has no cluster to release", lane)
	}
	if err := os.MkdirAll(filepath.Dir(root.releasePath(lane)), 0o755); err != nil {
		return err
	}
	return writeFile(root.releasePath(lane), []byte(time.Now().UTC().Format(time.RFC3339)+"\n"))
}

// releaseRequested says whether `lane release` asked for lane l.
func (m *Manager) releaseRequested(l *LaneFile) bool { return fileExists(m.Root.releasePath(l.Lane)) }

// consumeRelease removes the request once the manager acts on it.
func (m *Manager) consumeRelease(l *LaneFile) { _ = os.Remove(m.Root.releasePath(l.Lane)) }

// HostStatus is what the manager last saw on a host (<root>/hosts.status.json).
type HostStatus struct {
	At        time.Time `json:"at"`
	Load5     float64   `json:"load5"`
	Cores     int       `json:"cores,omitempty"` // the host's CPU count, when it reports it
	Builds    int       `json:"builds"`
	Free      int64     `json:"free_bytes,omitempty"` // free space of the staging filesystem
	Error     string    `json:"error,omitempty"`
	DownSince time.Time `json:"down_since,omitempty"`
	// Unhealthy is why the host gets no new lanes or steps (the health gate, hosthealth.go); "" when it may.
	Unhealthy string `json:"unhealthy,omitempty"`
	// Overloaded is why an eks host starts no build or run: its manager's machine is over max_load5_per_core ("" when not).
	Overloaded string `json:"overloaded,omitempty"`
	// Warnings are findings that stop nothing (a shared RDMA netns).
	Warnings []string `json:"warnings,omitempty"`
	Inflight []string `json:"inflight,omitempty"` // in-flight markers on the host's lanes
	// LedgerReleased counts the lane-ledger entries released since the manager started because their lane left
	// hosts.yaml or their cluster was gone.
	LedgerReleased int `json:"ledger_released,omitempty"`
	// LanesRetired counts the lanes that left hosts.yaml and were retired since the manager started (their state
	// moved to lanes/retired).
	LanesRetired int `json:"lanes_retired,omitempty"`
	// EKS is the lane count, vCPUs, $/h and estimated spend of an eks host.
	EKS *EksStatus `json:"eks,omitempty"`
}

// Experiment states (Status).
const (
	ExpActive   = "active"
	ExpPaused   = "paused"
	ExpFinished = "finished"
	ExpRemoving = "removing" // removed with runs still in flight
	ExpRemoved  = "removed"
)

// BenchStatus is everything `status` shows, as data (the dashboard reads it too).
type BenchStatus struct {
	Experiments []ExperimentStatus `json:"experiments"`
	Hosts       []HostView         `json:"hosts"`
	Lanes       []LaneFile         `json:"lanes"` // every lane file, by host and lane
	Inflight    []string           `json:"inflight,omitempty"`
	// Inventory says why hosts.yaml does not load ("" when it does): the manager goes on with the last one that did.
	Inventory string `json:"inventory_error,omitempty"`
	// FreshBuilds counts the builds of claimants that made a fresh app copy (runs that paid a whole build).
	FreshBuilds int `json:"fresh_builds"`
	// Claimants is the number of experiments that claim a pool.
	Claimants int `json:"claimants,omitempty"`
}

// ExperimentStatus is one experiment's line.
type ExperimentStatus struct {
	Name     string `json:"name"`
	App      string `json:"app"`
	Operator string `json:"operator"` // the hooks name, or builtin:<name>
	State    string `json:"state"`
	Wave     int    `json:"wave,omitempty"` // waves started so far
	Counts   Counts `json:"counts"`
	// Recycled counts held lanes of this pool that a claim found not claimable (an incident open), by reason.
	Recycled map[string]int `json:"recycled,omitempty"`
	// FreshBuilds counts this claimant's builds that made a fresh app copy instead of taking a pool lane.
	FreshBuilds int `json:"fresh_builds,omitempty"`
	// ClaimWait says why a claimant with sequences to start has no lane: it waits for its pool.
	ClaimWait string `json:"claim_wait,omitempty"`
	SpecErr   string `json:"spec_error,omitempty"`
	Error     string `json:"error,omitempty"` // the experiment could not be read
}

// HostView is one host: its inventory limits and what the manager last saw.
type HostView struct {
	Name string `json:"name"`
	HostStatus
	Lanes     int     `json:"lanes"` // listed clusters
	Busy      int     `json:"busy"`  // lanes not free
	MaxBuilds int     `json:"max_builds"`
	MaxLoad5  float64 `json:"max_load5"`
	MinFree   string  `json:"min_free,omitempty"`
}

// StatusOf reads the root's status (one experiment's, or every one's).
func StatusOf(root Root, only string) (BenchStatus, error) {
	var st BenchStatus
	names, err := root.Experiments()
	if err != nil {
		return st, err
	}
	lanes, _ := root.Lanes()
	st.Lanes = lanes
	running, bound := map[string]int{}, map[string]int{}
	for _, l := range lanes {
		if l.State == LaneRunning {
			running[l.Experiment]++
		}
		if l.Seq != nil {
			bound[l.Experiment]++
		}
	}
	recycled := recycledByPool(root)
	fresh := freshBuildsByExperiment(root)
	for _, k := range fresh {
		st.FreshBuilds += k
	}
	for _, n := range names {
		if only != "" && n != only {
			continue
		}
		x, err := root.LoadExperiment(n)
		if err != nil {
			st.Experiments = append(st.Experiments, ExperimentStatus{Name: n, Error: err.Error()})
			continue
		}
		s := x.Measured()
		e := ExperimentStatus{Name: n, App: s.App, Counts: x.Count(running[n]), SpecErr: x.SpecErr, State: ExpActive, Recycled: recycled[n]}
		e.Operator = s.Operator.Hooks
		if s.Operator.Builtin != "" {
			e.Operator = "builtin:" + s.Operator.Builtin
		}
		c := e.Counts
		switch {
		case x.Spec == nil && c.Running == 0:
			e.State = ExpRemoved
		case x.Spec == nil:
			e.State = ExpRemoving
		case x.Paused:
			e.State = ExpPaused
		case c.Queued == 0 && c.Running == 0:
			e.State = ExpFinished
		}
		if wv, err := root.Waves(n); err == nil {
			e.Wave = len(wv)
		}
		if s.Claim != "" {
			st.Claimants++
			e.FreshBuilds = fresh[n]
			if e.State == ExpActive && s.ClaimWaits() && c.Queued > 0 && bound[n] < s.Lanes {
				e.ClaimWait = claimWait(root, x, lanes)
			}
		}
		st.Experiments = append(st.Experiments, e)
	}
	var hs map[string]HostStatus
	_ = readJSON(root.path("hosts.status.json"), &hs)
	inv, err := LoadInventory(string(root))
	if err != nil {
		st.Inventory = err.Error()
	}
	busy := map[string]int{}
	for _, l := range lanes {
		if l.State != LaneFree {
			busy[l.Host]++
		}
	}
	for name, h := range inv.Hosts {
		hv := HostView{Name: name, HostStatus: hs[name], Lanes: len(h.Clusters), Busy: busy[name], MaxBuilds: h.BuildLimit(),
			MaxLoad5: h.LoadBudget(hs[name].Cores), MinFree: h.MinFree}
		st.Hosts = append(st.Hosts, hv)
		st.Inflight = append(st.Inflight, hs[name].Inflight...)
	}
	if st.Inventory != "" {
		// hosts.yaml is refused: the hosts the manager last saw, without their limits.
		for name, h := range hs {
			st.Hosts = append(st.Hosts, HostView{Name: name, HostStatus: h, Busy: busy[name]})
			st.Inflight = append(st.Inflight, h.Inflight...)
		}
	}
	sort.Slice(st.Hosts, func(i, j int) bool { return st.Hosts[i].Name < st.Hosts[j].Name })
	return st, nil
}

// Status writes the summary of every experiment (or one) and the lanes.
func Status(root Root, only string, w io.Writer) error {
	st, err := StatusOf(root, only)
	if err != nil {
		return err
	}
	defer func() { PlansSummary(root, w); printAlarms(root, w) }()
	if st.Inventory != "" {
		fmt.Fprintf(w, "hosts.yaml REFUSED (the manager uses the last inventory that loaded): %s\n", st.Inventory)
	}
	fmt.Fprintf(w, "%-24s %-18s %6s %7s %5s %7s %7s\n", "experiment", "state", "queued", "running", "done", "invalid", "dropped")
	for _, e := range st.Experiments {
		if e.Error != "" {
			fmt.Fprintf(w, "%-24s error: %v\n", e.Name, e.Error)
			continue
		}
		c := e.Counts
		state := e.State
		if state == ExpActive && e.Wave > 0 {
			state = fmt.Sprintf("wave %d", e.Wave)
		}
		line := fmt.Sprintf("%-24s %-18s %6d %7d %5d %7d %7d", e.Name, state, c.Queued, c.Running, c.Done, c.Invalid, c.Dropped)
		if len(c.Reasons) > 0 {
			var rs []string
			for r, k := range c.Reasons {
				rs = append(rs, fmt.Sprintf("%d %s", k, r))
			}
			sort.Strings(rs)
			line += "   invalid: " + strings.Join(rs, ", ")
		}
		if len(e.Recycled) > 0 {
			var rs []string
			for r, k := range e.Recycled {
				rs = append(rs, fmt.Sprintf("%d %s", k, r))
			}
			sort.Strings(rs)
			line += "   recycled: " + strings.Join(rs, ", ")
		}
		if e.FreshBuilds > 0 {
			line += fmt.Sprintf("   fresh builds: %d", e.FreshBuilds)
		}
		fmt.Fprintln(w, line)
		if e.ClaimWait != "" {
			fmt.Fprintf(w, "    claim: %s\n", e.ClaimWait)
		}
		if e.SpecErr != "" {
			fmt.Fprintf(w, "    spec: %s\n", e.SpecErr)
		}
	}
	fmt.Fprint(w, "lanes ")
	for _, h := range st.Hosts {
		load := fmt.Sprintf("%.0f/%.0f", h.Load5, h.MaxLoad5)
		if h.MaxLoad5 <= 0 {
			load = fmt.Sprintf("%.0f, no gate", h.Load5)
		}
		fmt.Fprintf(w, " %s %d/%d (builds %d/%d, load5 %s, free %s", h.Name, h.Busy, h.Lanes, h.Builds, h.MaxBuilds, load, size(h.Free))
		if h.MinFree != "" {
			fmt.Fprintf(w, "/%s", h.MinFree)
		}
		fmt.Fprint(w, ")")
		if h.Error != "" {
			fmt.Fprintf(w, " [%s]", h.Error)
		}
		if h.Unhealthy != "" {
			fmt.Fprintf(w, " [NO NEW WORK, %s]", h.Unhealthy)
		}
		if h.Overloaded != "" {
			fmt.Fprintf(w, " [NO NEW BUILD OR RUN, %s]", h.Overloaded)
		}
		for _, wn := range h.Warnings {
			fmt.Fprintf(w, " [warning: %s]", wn)
		}
		if h.LedgerReleased > 0 {
			fmt.Fprintf(w, " [ledger: %d stale entries released]", h.LedgerReleased)
		}
		if h.EKS != nil {
			fmt.Fprintf(w, " [%s]", h.EKS)
		}
		if h.LanesRetired > 0 {
			fmt.Fprintf(w, " [%d lanes retired: left hosts.yaml]", h.LanesRetired)
		}
	}
	fmt.Fprintln(w)
	injected := 0
	for _, l := range st.Lanes {
		if l.State == LaneFree && l.Reason == "" && l.Step == nil && l.Idle == "" {
			continue
		}
		if l.Gone {
			l.Reason = strings.TrimSpace("cluster deleted " + l.Reason)
		}
		what := l.Experiment
		if l.Seq != nil {
			what += fmt.Sprintf(" %s r%d a%d", l.Seq, l.Position, l.Attempt)
		}
		step := ""
		if l.Step != nil {
			kind := l.Step.Kind
			if l.Step.Phase != nil {
				kind += ": " + l.Step.Phase.Name
				if l.Step.Phase.Detail != "" && (l.Step.Phase.Name == PhaseLaneWait || l.Step.Phase.Detail == shapeNote) {
					kind += " (" + l.Step.Phase.Detail + ")"
				}
			}
			step = fmt.Sprintf("(%s, %s)", kind, time.Since(l.Step.Started).Round(time.Second))
			if l.Step.Kind == "run" {
				injected++
			}
		}
		fmt.Fprintf(w, "  %s/%-8s %-9s %s %s", l.Host, l.Lane, l.State, what, step)
		if l.Reason != "" {
			fmt.Fprintf(w, "  %s", l.Reason)
		}
		if l.Idle != "" && l.State == LaneFree && l.Step == nil {
			fmt.Fprintf(w, "  idle %s, not bound: %s", time.Since(l.Since).Round(time.Minute), l.Idle)
		} else if l.Idle != "" && l.State == LaneReady && l.Step == nil {
			fmt.Fprintf(w, "  its next run waits: %s", l.Idle)
		}
		fmt.Fprintln(w)
	}
	o := "none"
	if len(st.Inflight) > 0 {
		o = strings.Join(st.Inflight, ", ")
	}
	fmt.Fprintf(w, "runs in flight: %d; in-flight markers: %s\n", injected, o)
	if st.Claimants > 0 {
		fmt.Fprintf(w, "runs on fresh builds: %d (claimant builds that made an app copy instead of taking a pool lane)\n", st.FreshBuilds)
	}
	return nil
}

// Lanes reads every lane file.
func (r Root) Lanes() ([]LaneFile, error) {
	ents, err := os.ReadDir(r.path("lanes"))
	if err != nil {
		return nil, err
	}
	var out []LaneFile
	for _, e := range ents {
		if !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		var lf LaneFile
		if err := readJSON(r.path("lanes", e.Name()), &lf); err == nil {
			out = append(out, lf)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Host+out[i].Lane < out[j].Host+out[j].Lane })
	return out, nil
}

// Wave is a set of sequences assigned together; the next wave starts only
// after every sequence of this one ended.
type Wave struct {
	Seqs    []Seq     `json:"seqs"`
	Started time.Time `json:"started"`
	Paused  bool      `json:"paused,omitempty"` // pause_after_wave paused the experiment after it
}

// Waves reads <exp>/waves.json.
func (r Root) Waves(exp string) ([]Wave, error) {
	var w []Wave
	err := readJSON(r.WavesPath(exp), &w)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	return w, err
}

// Gate is one failed ready gate or build check (<exp>/gates.jsonl).
type Gate struct {
	Run     string    `json:"run,omitempty"`
	Lane    string    `json:"lane"`
	Host    string    `json:"host"`
	Where   string    `json:"where"` // "gate" (before a run's inject) or "build" (the build's own check)
	At      time.Time `json:"at"`
	Reasons []string  `json:"reasons"`
	Alarm   bool      `json:"alarm"` // the operator raised an alarm (a reason starts "alarm:")
}

// Results grades an experiment's done runs with the pinned fuzzops (`fuzzops
// report`, interference fence on, the noop runs of the `with` experiments as
// baselines) into <exp>/results: runs.jsonl, report.md, grades.json and
// runs.csv. Runs the host interfered with become invalid "host interference".
func Results(ctx context.Context, root Root, name string, with []string, log io.Writer) error {
	inv, err := LoadInventory(string(root))
	if err != nil {
		return err
	}
	x, err := root.LoadExperiment(name)
	if err != nil {
		return err
	}
	xs := []*Experiment{x}
	for _, b := range with {
		bx, err := root.LoadExperiment(b)
		if err != nil {
			return err
		}
		if err := Compatible(x, bx); err != nil {
			return err
		}
		xs = append(xs, bx)
	}
	fz := inv.FuzzopsPath(x.Lock.Spec.Fuzzops, inv.LocalHost())
	if fz == "" {
		return fmt.Errorf("the local host has no fuzzops checkout for %s", x.Lock.Spec.Fuzzops)
	}
	dir := root.path(name, "results")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	report := func(skip map[string]bool) error {
		var lines []string
		for _, e := range xs {
			for _, rf := range sortedRuns(e) {
				if rf.Status != StatusDone || skip[rf.ID] {
					continue
				}
				rec, err := readRecord(root, e.Name, rf)
				if err != nil {
					return err
				}
				b, _ := json.Marshal(rec)
				lines = append(lines, string(b))
			}
		}
		if err := writeFile(filepath.Join(dir, "runs.jsonl"), []byte(strings.Join(lines, "\n")+"\n")); err != nil {
			return err
		}
		return NewWorkload(x.Lock.Spec.Workload, fz).Grade(ctx, dir, inv.Hosts[inv.LocalHost()].EnvFor(x.Lock.Spec.Workload), log)
	}
	if err := report(nil); err != nil {
		return fmt.Errorf("fuzzops report: %w", err)
	}
	// Runs of this experiment above the host's fence: invalid, then grade without them.
	var hits []struct {
		Fault, Operator, Lane, Why string
		Seed                       int
		Started                    time.Time
	}
	_ = readJSON(filepath.Join(dir, "interference.json"), &hits)
	skip := map[string]bool{}
	for _, h := range hits {
		for _, rf := range x.Runs {
			if rf.Status == StatusDone && rf.OperatorLabel == h.Operator && rf.Seed == h.Seed && rf.Host+"/"+rf.Lane == h.Lane && recordFault(rf) == h.Fault {
				rec, _ := readRecord(root, name, rf)
				if st, _ := rec["started"].(string); sameTime(st, h.Started) {
					rf.Status, rf.Reason = StatusInvalid, "host interference: "+h.Why
					if err := writeJSON(filepath.Join(root.RunDir(name, rf.ID), "run.json"), rf); err != nil {
						return err
					}
					x.Runs[rf.ID] = rf
					skip[rf.ID] = true
				}
			}
		}
	}
	if len(skip) > 0 {
		if err := report(skip); err != nil {
			return fmt.Errorf("fuzzops report: %w", err)
		}
	}
	done := 0
	for _, rf := range x.Runs {
		if rf.Status == StatusDone {
			done++
		}
	}
	if err := writeJSON(filepath.Join(dir, ResultsMetaFile), ResultsMeta{At: time.Now().UTC(), Fuzzops: x.Lock.Spec.Fuzzops,
		With: append([]string{}, with...), DoneRuns: done}); err != nil {
		return err
	}
	return writeTables(root, x, dir)
}

// ResultsMetaFile is <exp>/results/meta.json: how the results were graded.
const ResultsMetaFile = "meta.json"

// ResultsMeta says when `results` graded an experiment, with which fuzzops,
// against which baseline experiments (--with) and how many done runs it saw:
// a reader tells stale grades (a run done since) and names the references.
type ResultsMeta struct {
	At       time.Time `json:"at"`
	Fuzzops  string    `json:"fuzzops"`
	With     []string  `json:"with"`
	DoneRuns int       `json:"done_runs"`
}

func sameTime(s string, t time.Time) bool {
	p, err := time.Parse(time.RFC3339Nano, s)
	return err == nil && p.Equal(t)
}

// recordFault is the fault name fuzzops records for a run.
func recordFault(rf RunFile) string {
	if rf.Fault == Healthy {
		return "none"
	}
	return rf.Fault
}

func sortedRuns(x *Experiment) []RunFile {
	var out []RunFile
	for _, rf := range x.Runs {
		out = append(out, rf)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// readRecord reads a run's fuzzops record with its lane set to host/cluster,
// which the interference fence groups by.
func readRecord(root Root, exp string, rf RunFile) (map[string]any, error) {
	var rec map[string]any
	if err := readJSON(filepath.Join(root.RunDir(exp, rf.ID), "record.json"), &rec); err != nil {
		return nil, err
	}
	rec["lane"] = rf.Host + "/" + rf.Lane
	return rec, nil
}

// Compatible refuses a baseline (or a compared experiment) measured another way.
func Compatible(x, b *Experiment) error {
	a, c := x.Lock.Spec, b.Lock.Spec
	ra, _ := json.Marshal(a.Run)
	rc, _ := json.Marshal(c.Run)
	switch {
	case string(ra) != string(rc):
		return fmt.Errorf("baseline %s: its run block %s differs from %s", b.Name, rc, ra)
	case !strings.HasPrefix(a.Fuzzops, c.Fuzzops) && !strings.HasPrefix(c.Fuzzops, a.Fuzzops):
		return fmt.Errorf("baseline %s: fuzzops %s, not %s", b.Name, c.Fuzzops, a.Fuzzops)
	case a.App != c.App:
		return fmt.Errorf("baseline %s: app %s, not %s", b.Name, c.App, a.App)
	}
	mv := map[int]bool{}
	for _, e := range []*Experiment{x, b} {
		for _, rf := range e.Runs {
			if rf.Status == StatusDone {
				mv[rf.Fuzzops.MeasureVersion] = true
			}
		}
	}
	if len(mv) > 1 {
		return fmt.Errorf("baseline %s: runs of more than one measure version", b.Name)
	}
	return nil
}

// writeTables writes runs.csv and report.md's bench summary above fuzzops' report.
func writeTables(root Root, x *Experiment, dir string) error {
	var cells []struct {
		Fault, Operator, Grade string
	}
	_ = readJSON(filepath.Join(dir, "grades.json"), &cells)
	grade := map[string]string{}
	for _, c := range cells {
		grade[c.Fault+"|"+c.Operator] = c.Grade
	}
	f, err := os.Create(filepath.Join(dir, "runs.csv"))
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	_ = w.Write([]string{"fault", "sequence", "position", "attempt", "seed", "lane", "host", "status", "reason", "grade",
		"incidents", "rollouts", "transcripts_missing"})
	inRun, runsMissing, noReport := 0, 0, 0
	for _, rf := range sortedRuns(x) {
		g := ""
		if rf.Status == StatusDone {
			g = grade[recordFault(rf)+"|"+rf.OperatorLabel]
			if rf.Fault == Healthy {
				if rec, err := readRecord(root, x.Name, rf); err == nil {
					if life, ok := rec["lifecycle"].(map[string]any); ok {
						if said, ok := life["said"].([]any); ok && len(said) > 0 {
							inRun++
						}
					}
				}
			}
		}
		inc, roll, miss := "", "", ""
		if rf.Status == StatusDone {
			if tr, err := readTranscripts(root, x.Name, rf); err == nil {
				inc, roll, miss = strconv.Itoa(len(tr.Incidents)), strconv.Itoa(tr.Rollouts), strings.Join(tr.Missing, " ")
				if len(tr.Missing) > 0 {
					runsMissing++
				}
			} else if x.Lock.Spec.Operator.Hooks != "" {
				noReport++
				miss = "no transcripts.json"
			}
		}
		_ = w.Write([]string{rf.Fault, strconv.Itoa(rf.Sequence), strconv.Itoa(rf.Position), strconv.Itoa(rf.Attempt), strconv.Itoa(rf.Seed),
			rf.Lane, rf.Host, rf.Status, rf.Reason, g, inc, roll, miss})
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return err
	}
	gates, _ := readJSONL[Gate](root.GatesPath(x.Name))
	alarms, gateFails := 0, 0
	for _, g := range gates {
		gateFails++
		if g.Alarm {
			alarms++
		}
	}
	c := x.Count(0)
	var b strings.Builder
	fmt.Fprintf(&b, "# %s\n\n", x.Name)
	fmt.Fprintf(&b, "| done | invalid | dropped | queued | failed gates | gate-time false alarms | in-run false alarms (healthy runs) |\n|---|---|---|---|---|---|---|\n")
	fmt.Fprintf(&b, "| %d | %d | %d | %d | %d | %d | %d |\n\n", c.Done, c.Invalid, c.Dropped, c.Queued, gateFails, alarms, inRun)
	if len(c.Reasons) > 0 {
		b.WriteString("Invalid runs by reason (not graded):\n\n")
		var rs []string
		for r := range c.Reasons {
			rs = append(rs, r)
		}
		sort.Strings(rs)
		for _, r := range rs {
			fmt.Fprintf(&b, "- %s: %d\n", r, c.Reasons[r])
		}
		b.WriteString("\n")
	}
	if x.Lock.Spec.Operator.Hooks != "" {
		fmt.Fprintf(&b, "Transcripts (from the operator's sink on the host, `operator/transcripts.json`): %d done runs with an incident lacking its transcript, %d without a transcripts report (columns incidents, rollouts, transcripts_missing in runs.csv).\n\n", runsMissing, noReport)
	}
	b.WriteString("Only done runs are graded below (fuzzops report; per-run table in runs.csv).\n\n")
	rep, _ := os.ReadFile(filepath.Join(dir, "report.md"))
	if i := strings.Index(string(rep), "\n"); strings.HasPrefix(string(rep), "# ") && i > 0 {
		rep = rep[i+1:]
	}
	b.Write(rep)
	return writeFile(filepath.Join(dir, "report.md"), []byte(b.String()))
}

// Transcripts is what an operator's collect hook reports about the run's transcripts
// (operator/transcripts.json): its incidents, the rollouts the sink holds, and the incidents with an
// outcome and an agent but no transcript.
type Transcripts struct {
	Incidents []struct {
		ID        string `json:"id"`
		Outcome   bool   `json:"outcome"`
		AgentsRan bool   `json:"agents_ran"`
		Rollouts  int    `json:"rollouts"`
	} `json:"incidents"`
	Rollouts int      `json:"rollouts"`
	Missing  []string `json:"missing"`
}

func readTranscripts(root Root, exp string, rf RunFile) (Transcripts, error) {
	var t Transcripts
	err := readJSON(filepath.Join(root.RunDir(exp, rf.ID), "operator", "transcripts.json"), &t)
	return t, err
}

// freshBuildsByExperiment counts the claimant builds that made a fresh app copy (fresh-builds.jsonl), per experiment.
func freshBuildsByExperiment(root Root) map[string]int {
	out := map[string]int{}
	b, err := os.ReadFile(root.path(freshBuildsFile))
	if err != nil {
		return out
	}
	for _, line := range strings.Split(string(b), "\n") {
		var e struct{ Experiment string }
		if json.Unmarshal([]byte(line), &e) == nil && e.Experiment != "" {
			out[e.Experiment]++
		}
	}
	return out
}

// claimWait says what a waiting claimant waits for: its pool, and why that pool cannot serve it when it never will.
func claimWait(root Root, x *Experiment, lanes []LaneFile) string {
	name := x.Measured().Claim
	pool, err := root.LoadExperiment(name)
	switch {
	case err != nil || pool.Spec == nil:
		return fmt.Sprintf("waits for pool %s, which does not exist (add it, or set claim_only: false to build)", name)
	case !pool.Measured().Hold:
		return fmt.Sprintf("waits for pool %s, which is not a hold experiment", name)
	case !sameOperatorProfile(pool, x):
		return fmt.Sprintf("waits for pool %s, whose operator profile differs (app, namespace, fuzzops, hooks, baseline or settings): it can never be claimed", name)
	case pool.Paused:
		return fmt.Sprintf("waits for pool %s, which is paused", name)
	}
	ready, building := 0, 0
	for _, l := range lanes {
		if l.Experiment != name {
			continue
		}
		switch {
		case l.State == LaneReady && l.Step == nil:
			ready++
		case l.State == LaneBuilding:
			building++
		}
	}
	return fmt.Sprintf("waits for a ready lane of pool %s (ready %d, building %d)", name, ready, building)
}

// recycledByPool counts the held lanes a claim found not claimable (recycled.jsonl), per pool and reason.
func recycledByPool(root Root) map[string]map[string]int {
	out := map[string]map[string]int{}
	b, err := os.ReadFile(root.path("recycled.jsonl"))
	if err != nil {
		return out
	}
	for _, line := range strings.Split(string(b), "\n") {
		var e struct{ Pool, Why string }
		if json.Unmarshal([]byte(line), &e) != nil || e.Pool == "" {
			continue
		}
		if out[e.Pool] == nil {
			out[e.Pool] = map[string]int{}
		}
		out[e.Pool][e.Why]++
	}
	return out
}
