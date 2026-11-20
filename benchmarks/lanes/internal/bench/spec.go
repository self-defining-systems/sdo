// Package bench is fuzzops-bench: a long-running manager that turns
// experiment specs (one file each, in <root>/specs) into fuzzops runs on
// lanes (one kind cluster each) across listed hosts, and keeps every result
// under <root>. It knows no operator and no app: an operator takes part
// through a small hook executable (prepare, idle, watch, collect, teardown,
// check), an app through its fuzzops pack, and everything else comes from the
// spec. Measurement stays in the pinned fuzzops binary (docs/bench.md).
package bench

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/self-defining-systems/sdo/benchmarks/lanes/internal/miniyaml"
)

// Healthy is the fault ID of a run that injects nothing (`fuzzops run --fault none`).
const Healthy = "healthy"

// Spec is one experiment (<root>/specs/<name>.yaml).
type Spec struct {
	Name    string `json:"name"`
	App     string `json:"app"`     // fuzzops pack, a path in the pinned fuzzops checkout
	Fuzzops string `json:"fuzzops"` // pinned fuzzops revision (prefix)
	// Workload names the workload adapter (docs/contract.md): "" or "fuzzops" is the built-in one, anything else a
	// command run in the pinned checkout. Not part of the identity: the spec of an existing root never sets it.
	Workload  string   `json:"workload,omitempty"`
	Operator  Operator `json:"operator"`
	Namespace string   `json:"namespace"`
	Faults    Faults   `json:"faults"`
	// Sequences: per fault, the seed lists run in order on one fresh lane each.
	Sequences [][]int `json:"sequences"`
	// Run holds extra `fuzzops run` flags, --key=value.
	Run          map[string]any `json:"run"`
	ReadyFor     string         `json:"ready_for"`     // the gate: journeys clean and the operator idle this long
	ReadyTimeout string         `json:"ready_timeout"` // longest wait for the gate before the lane is rebuilt
	Cap          string         `json:"cap"`           // inject -> operator idle; past it the run is invalid "capped"
	// GateAfterCheck, when set, is the ready gate of a run that starts within 5 minutes of its build check: shorter
	// than ready_for, since the check already held that span. The gate's seconds also seed the ledger the run verifies
	// afterwards (the data written before the inject), so a short gate checks less pre-inject data: it is part of
	// the identity and off by default.
	GateAfterCheck string `json:"gate_after_check,omitempty"`

	// Live-editable.
	Lanes          int      `json:"lanes"`
	Hosts          []string `json:"hosts"`
	Retries        int      `json:"retries"`
	PauseAfterWave bool     `json:"pause_after_wave"`
	// BuildBadSeconds is how many bad seconds the build check tolerates in its ready_for window
	// (default DefaultBuildBadSeconds). It never reaches graded runs, so it is not part of the identity.
	BuildBadSeconds *int `json:"build_bad_seconds,omitempty"`
	// KeepLanes keeps the lanes' clusters after the experiment ends (default: deleted when the host can
	// recreate them, to free the disk).
	KeepLanes bool `json:"keep_lanes,omitempty"`
	// FreshCluster builds every sequence on a new cluster; FreshClusterFaults only those of these
	// faults (whose effects reach beyond the namespace: nodes, the network, cluster-scoped objects).
	// Default: a cluster is reused, the app and the operator reset in place.
	FreshCluster       bool     `json:"fresh_cluster,omitempty"`
	FreshClusterFaults []string `json:"fresh_cluster_faults,omitempty"`
	// ReuseApp rewinds a lane in place between sequences of this experiment: the operator is torn down and
	// restored from the baseline, the app copy stays (after recover, and only when the bare-app gate finds
	// it clean). FreshAppFaults are the faults whose effects reach past their undo (data, settings): their
	// sequences, and the sequence after them on a lane, start from a new copy.
	ReuseApp bool `json:"reuse_app,omitempty"`
	// Hold builds the experiment's lanes (app, operator, build check) and keeps them ready, running nothing: a warm
	// pool another experiment takes with Claim. Remove the experiment to release them.
	Hold bool `json:"hold,omitempty"`
	// Claim names a Hold experiment whose ready lane this experiment takes instead of building: the lane's operator
	// gets this experiment's sources by the hooks' swap. It must match the pool in app, namespace, fuzzops, hooks,
	// baseline and operator settings (the revision may differ). With no such lane the sequence waits for one (see
	// ClaimOnly).
	Claim string `json:"claim,omitempty"`
	// ClaimOnly (default true with Claim): a claimant's sequence waits for a ready or reset lane of its pool and never
	// builds a lane of its own (a fresh app copy and prepare, minutes to tens of minutes per run). false lets it fall back
	// to the ordinary build when no pool lane is ready. Infrastructure, not part of the identity.
	ClaimOnly *bool `json:"claim_only,omitempty"`
	// ClaimWaitMinutes bounds how long a claimant waits with no pool lane for a sequence (default 240): then the
	// experiment is paused with a "claim wait exceeded" error. Infrastructure, not part of the identity.
	ClaimWaitMinutes int `json:"claim_wait_minutes,omitempty"`
	// Lane limits what each lane's nodes may use on a host that lists lanes.cpus: whole cores and memory in total.
	// Default: the app's (DefaultLaneLimit). Infrastructure, not part of the identity.
	Lane           *LaneLimit `json:"lane,omitempty"`
	FreshAppFaults []string   `json:"fresh_app_faults,omitempty"`
	// AcceptOOMKills keeps a run whose lane had processes killed at its memory limit during the run (run.json
	// isolation.oom_kills > 0) as done. Default: such a run is invalid (retried like any invalid run), since a
	// killed kubelet, etcd or app process changes what was measured.
	AcceptOOMKills bool `json:"accept_oom_kills,omitempty"`
	// Isolation "two-way" requires hosts whose other slices exclude the lane block; "one-way-ok" accepts a host with no
	// reservation (its rows are flagged). Default: a reserved host, one-way or two-way. Infrastructure, not part of the identity.
	Isolation string `json:"isolation,omitempty"`
}

// ClaimWaits reports whether the spec claims a pool and waits for its lanes instead of building its own.
func (s Spec) ClaimWaits() bool {
	return s.Claim != "" && (s.ClaimOnly == nil || *s.ClaimOnly)
}

// DefaultClaimWaitMinutes is the default bound of ClaimWaitMinutes.
const DefaultClaimWaitMinutes = 240

// ClaimWaitBound is how long a claimant waits for a pool lane.
func (s Spec) ClaimWaitBound() time.Duration {
	if s.ClaimWaitMinutes <= 0 {
		return DefaultClaimWaitMinutes * time.Minute
	}
	return time.Duration(s.ClaimWaitMinutes) * time.Minute
}

// NeedsFreshApp reports whether a sequence of fault leaves the app copy unfit for reuse.
func (s Spec) NeedsFreshApp(fault string) bool {
	return slices.Contains(s.FreshAppFaults, fault)
}

// DefaultBuildBadSeconds is the bad seconds the build check tolerates by default.
const DefaultBuildBadSeconds = 2

// BuildBad is the bad seconds the build check tolerates.
func (s Spec) BuildBad() int {
	if s.BuildBadSeconds == nil {
		return DefaultBuildBadSeconds
	}
	return *s.BuildBadSeconds
}

// NeedsFreshCluster reports whether a sequence of fault must start on a new cluster.
func (s Spec) NeedsFreshCluster(fault string) bool {
	if s.FreshCluster {
		return true
	}
	for _, f := range s.FreshClusterFaults {
		if f == fault {
			return true
		}
	}
	return false
}

// Operator is the operator under test: hooks (an external operator) or a
// fuzzops built-in operator (noop, best-fix, ...).
type Operator struct {
	Hooks    string `json:"hooks,omitempty"`
	Builtin  string `json:"builtin,omitempty"`
	Revision string `json:"revision,omitempty"` // pinned hooks checkout
	Baseline string `json:"baseline,omitempty"` // a directory shipped to hosts by content hash
	// Seed is a directory in the baseline's format (a learned memory) that a claim's swap imports into the pool lane's
	// accepted repository; it is not part of what a claim must match.
	Seed     string         `json:"seed,omitempty"`
	Settings map[string]any `json:"settings,omitempty"` // opaque; passed to the hooks
}

// Faults are the fuzzops instances run.
type Faults struct {
	Instances string   `json:"instances,omitempty"`
	IDs       []string `json:"ids"`
}

var (
	nameRE  = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,39}$`)
	revRE   = regexp.MustCompile(`^[0-9a-f]{7,40}$`)
	labelRE = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,30}$`)
)

// reservedRunFlags are set by the bench itself.
var reservedRunFlags = []string{"pack", "namespace", "operator", "skip-reset", "ready-for", "ready-cmd", "ready-timeout", "window-end-cmd",
	"instance-file", "instance", "fault", "seed", "set", "composite-file", "composite", "nodeport"}

// LoadSpec reads a spec file (YAML subset or JSON, docs/app-packs.md).
func LoadSpec(path string) (Spec, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Spec{}, err
	}
	s, err := ParseSpec(b)
	if err != nil {
		return Spec{}, fmt.Errorf("%s: %w", path, err)
	}
	return s, nil
}

// ParseSpec decodes and validates spec text (JSON or the YAML subset).
func ParseSpec(b []byte) (Spec, error) {
	var s Spec
	if err := miniyaml.Decode(b, &s); err != nil {
		return Spec{}, err
	}
	if err := s.Validate(); err != nil {
		return Spec{}, err
	}
	return s, nil
}

// Validate checks the spec on its own (no files, no inventory).
func (s Spec) Validate() error {
	var errs []string
	if s.ClaimWaitMinutes < 0 {
		errs = append(errs, "claim_wait_minutes must not be negative")
	}
	bad := func(f string, a ...any) { errs = append(errs, fmt.Sprintf(f, a...)) }
	if !nameRE.MatchString(s.Name) {
		bad("name %q: lowercase letters, digits and '-', at most 40", s.Name)
	}
	if s.App == "" {
		bad("app is required")
	}
	if !revRE.MatchString(s.Fuzzops) {
		bad("fuzzops %q: a revision (7-40 hex digits)", s.Fuzzops)
	}
	switch o := s.Operator; {
	case (o.Hooks == "") == (o.Builtin == ""):
		bad("operator: exactly one of hooks and builtin")
	case o.Hooks != "" && !labelRE.MatchString(o.Hooks):
		bad("operator.hooks %q: lowercase letters, digits and '-'", o.Hooks)
	case o.Hooks != "" && !revRE.MatchString(o.Revision):
		bad("operator.revision %q: the pinned hooks revision (7-40 hex digits)", o.Revision)
	case o.Builtin != "" && (o.Revision != "" || o.Baseline != "" || len(o.Settings) > 0):
		bad("operator: a builtin operator takes no revision, baseline or settings")
	}
	if s.Namespace == "" {
		bad("namespace is required")
	}
	if len(s.Faults.IDs) == 0 {
		bad("faults.ids is empty")
	}
	seen := map[string]bool{}
	needInstances := false
	for _, id := range s.Faults.IDs {
		if seen[id] {
			bad("faults.ids lists %q twice", id)
		}
		seen[id] = true
		needInstances = needInstances || id != Healthy
	}
	if needInstances && s.Faults.Instances == "" {
		bad("faults.instances is required (a fuzzops instances file)")
	}
	if len(s.Sequences) == 0 {
		bad("sequences is empty (for example [[1]])")
	}
	for i, q := range s.Sequences {
		if len(q) == 0 {
			bad("sequences[%d] is empty", i)
		}
		for _, seed := range q {
			if seed < 1 {
				bad("sequences[%d]: seeds start at 1", i)
			}
		}
	}
	for k := range s.Run {
		for _, r := range reservedRunFlags {
			if k == r {
				bad("run.%s is set by the bench", k)
			}
		}
	}
	rf, e1 := parseDur("ready_for", s.ReadyFor)
	rt, e2 := parseDur("ready_timeout", s.ReadyTimeout)
	_, e3 := parseDur("cap", s.Cap)
	var e4 error
	if s.GateAfterCheck != "" {
		_, e4 = parseDur("gate_after_check", s.GateAfterCheck)
	}
	for _, e := range []error{e1, e2, e3, e4} {
		if e != nil {
			errs = append(errs, e.Error())
		}
	}
	if e1 == nil && e2 == nil && rt < rf {
		bad("ready_timeout must be at least ready_for")
	}
	if s.Lanes < 1 {
		bad("lanes must be at least 1")
	}
	if len(s.Hosts) == 0 {
		bad("hosts is empty")
	}
	if s.BuildBadSeconds != nil && *s.BuildBadSeconds < 0 {
		bad("build_bad_seconds must not be negative")
	}
	if s.ClaimOnly != nil && s.Claim == "" {
		bad("claim_only needs claim")
	}
	if s.Retries < 0 {
		bad("retries must not be negative")
	}
	if s.Lane != nil {
		errs = append(errs, s.Lane.validate()...)
	}
	if len(errs) > 0 {
		return fmt.Errorf("spec: %s", strings.Join(errs, "; "))
	}
	return nil
}

func parseDur(field, v string) (time.Duration, error) {
	if v == "" {
		return 0, fmt.Errorf("%s is required (a duration such as 2m)", field)
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%s %q: a positive duration", field, v)
	}
	return d, nil
}

// DefaultStartupBound is SDO's own response.startup_bound default: how long a new install may report busy
// ("startup baseline pending") before its start-up period ends.
const DefaultStartupBound = 30 * time.Minute

// startupMargin is the wait added past the subject's start-up bound.
const startupMargin = 10 * time.Minute

// StartupBound is the subject's declared start-up bound: operator.settings.startup_bound (a duration; the hooks
// render it as response.startup_bound), else SDO's default. A spec with no hooks has none (0), and 0s means the
// subject has no start-up period.
func (s Spec) StartupBound() time.Duration {
	if s.Operator.Hooks == "" {
		return 0
	}
	if v, ok := s.Operator.Settings["startup_bound"].(string); ok {
		if d, err := time.ParseDuration(v); err == nil && d >= 0 {
			return d
		}
	}
	return DefaultStartupBound
}

// Durations returns ready_for, the effective ready timeout and cap (the spec is valid). The ready timeout is
// never shorter than the subject's start-up bound plus a margin: a lane is not failed for a wait the subject has
// declared it may need.
func (s Spec) Durations() (readyFor, readyTimeout, cap time.Duration) {
	readyFor, _ = time.ParseDuration(s.ReadyFor)
	readyTimeout, _ = time.ParseDuration(s.ReadyTimeout)
	cap, _ = time.ParseDuration(s.Cap)
	if b := s.StartupBound(); b > 0 {
		readyTimeout = max(readyTimeout, b+startupMargin)
	}
	return
}

// RunFlags are the spec's extra `fuzzops run` flags, sorted.
func (s Spec) RunFlags() []string {
	keys := make([]string, 0, len(s.Run))
	for k := range s.Run {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var out []string
	for _, k := range keys {
		out = append(out, fmt.Sprintf("--%s=%v", k, s.Run[k]))
	}
	return out
}

// identity is what a column is measured by: frozen at `add`.
type identity struct {
	App       string         `json:"app"`
	Fuzzops   string         `json:"fuzzops"`
	Operator  Operator       `json:"operator"`
	Namespace string         `json:"namespace"`
	Instances string         `json:"instances"`
	Sequences [][]int        `json:"sequences"`
	Run       map[string]any `json:"run"`
	ReadyFor  string         `json:"ready_for"`
	ReadyTO   string         `json:"ready_timeout"`
	Cap       string         `json:"cap"`
	Gate      string         `json:"gate_after_check,omitempty"`
}

func (s Spec) identity() identity {
	return identity{s.App, s.Fuzzops, s.Operator, s.Namespace, s.Faults.Instances, s.Sequences, s.Run, s.ReadyFor, s.ReadyTimeout, s.Cap, s.GateAfterCheck}
}

// withIdentityOf is s with every identity field taken from l (the live fields stay s's).
func (s Spec) withIdentityOf(l Spec) Spec {
	ids := s.Faults.IDs
	s.App, s.Fuzzops, s.Operator, s.Namespace, s.Faults, s.Sequences, s.Run = l.App, l.Fuzzops, l.Operator, l.Namespace, l.Faults, l.Sequences, l.Run
	s.ReadyFor, s.ReadyTimeout, s.Cap, s.GateAfterCheck = l.ReadyFor, l.ReadyTimeout, l.Cap, l.GateAfterCheck
	s.Faults.IDs = ids
	return s
}

// IdentityDiff lists the identity fields in which s differs from the frozen identity in l.
func (s Spec) IdentityDiff(l Lock) []string {
	a, b := s.identity(), l.Spec.identity()
	fields := []struct {
		name string
		x, y any
	}{{"app", a.App, b.App}, {"fuzzops", a.Fuzzops, b.Fuzzops}, {"operator", a.Operator, b.Operator}, {"namespace", a.Namespace, b.Namespace},
		{"faults.instances", a.Instances, b.Instances}, {"sequences", a.Sequences, b.Sequences}, {"run", a.Run, b.Run},
		{"ready_for", a.ReadyFor, b.ReadyFor}, {"ready_timeout", a.ReadyTO, b.ReadyTO}, {"cap", a.Cap, b.Cap}, {"gate_after_check", a.Gate, b.Gate}}
	var out []string
	for _, f := range fields {
		x, _ := json.Marshal(f.x)
		y, _ := json.Marshal(f.y)
		if string(x) != string(y) {
			out = append(out, f.name)
		}
	}
	return out
}

// Seq names one sequence of one fault: Q counts from 1 over spec.Sequences.
type Seq struct {
	Fault string `json:"fault"`
	Q     int    `json:"q"`
}

func (q Seq) String() string { return fmt.Sprintf("%s q%d", q.Fault, q.Q) }

// Seqs lists the spec's sequences, fault by fault in spec order.
func (s Spec) Seqs() []Seq {
	var out []Seq
	for _, f := range s.Faults.IDs {
		for q := range s.Sequences {
			out = append(out, Seq{f, q + 1})
		}
	}
	return out
}

// Seeds of sequence q.
func (s Spec) Seeds(q int) []int { return s.Sequences[q-1] }

// Slug is a fault ID as a file name: readable and unique (a short hash of the ID).
func Slug(fault string) string {
	if fault == Healthy {
		return Healthy
	}
	s := strings.Trim(regexp.MustCompile(`[^a-z0-9]+`).ReplaceAllString(strings.ToLower(fault), "-"), "-")
	if len(s) > 50 {
		s = strings.TrimRight(s[:50], "-")
	}
	h := sha256.Sum256([]byte(fault))
	return s + "-" + hex.EncodeToString(h[:])[:8]
}

// RunID is <fault-slug>-q<sequence>-r<position>-a<attempt>.
func RunID(fault string, q, position, attempt int) string {
	return fmt.Sprintf("%s-q%d-r%d-a%d", Slug(fault), q, position, attempt)
}

// OperatorLabel is the fuzzops --operator of position r: every position of a
// sequence is graded in its own cell.
func (s Spec) OperatorLabel(position int) string {
	if s.Operator.Builtin != "" {
		return s.Operator.Builtin
	}
	return fmt.Sprintf("exec:%s-%s-r%d", s.Operator.Hooks, s.Name, position)
}
