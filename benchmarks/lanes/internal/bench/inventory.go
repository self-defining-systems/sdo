package bench

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/self-defining-systems/sdo/benchmarks/lanes/internal/miniyaml"
)

// Inventory is <root>/hosts.yaml: the hosts, their clusters (one lane each),
// their limits and the pinned checkouts. The bench owns exactly the listed
// clusters and nothing else.
type Inventory struct {
	Hosts     map[string]Host `json:"hosts"`
	Checkouts Checkouts       `json:"checkouts"`
	// HookCommand per hooks name, run in the pinned hooks checkout: `<command> <verb>`.
	HookCommand map[string]string `json:"hook_command"`
}

// Host is one machine with kind clusters.
type Host struct {
	Local   bool   `json:"local,omitempty"`
	SSH     string `json:"ssh,omitempty"`
	SSHJump string `json:"ssh_jump,omitempty"` // ssh -J: the host is reached through this jump host
	// Env is added to the environment of every step on the host: its cluster commands, fuzzops calls and hooks
	// (e.g. FUZZOPS_REGISTRY_PORT when the default registry port is taken).
	Env map[string]string `json:"env,omitempty"`
	// WorkloadEnv is added, over Env, to the steps of one workload only (the key is the spec's workload name, or
	// DefaultWorkloadID ("default") for a spec that names none): a workload's own settings do not belong in the host's env, which every workload
	// sees. Host-level work (ledger, preflight) never carries it.
	WorkloadEnv map[string]map[string]string `json:"workload_env,omitempty"`
	Staging     string                       `json:"staging"`     // host-local directory for steps and inputs
	MaxBuilds   int                          `json:"max_builds"`  // builds (app copy + prepare) at once
	MaxLoad5    float64                      `json:"max_load5"`   // no build starts at or above this 5-minute load; 0 or omitted: no load gate (hosts with reserve: only)
	Kubeconfigs string                       `json:"kubeconfigs"` // directory with one kubeconfig file per cluster, named as the cluster
	Clusters    []string                     `json:"clusters"`
	// MinFree is the disk floor of the staging filesystem ("150G"): no build starts below it; gc runs first.
	MinFree string `json:"min_free,omitempty"`
	// MaxBuildFailures is how many builds in a row may fail on one of the host's lanes before the lane is taken
	// down until a human looks (default DefaultMaxBuildFailures). Each failed build of a large app costs many
	// minutes of a lane on a host that may be contended, so the default is low.
	MaxBuildFailures int `json:"max_build_failures,omitempty"`
	// ClusterUp and ClusterDown create and delete one lane's cluster ({cluster} and {kubeconfig} are
	// replaced; run in the pinned workload checkout with $LANES_INSTALL and $BENCH_WORKLOAD_DIR set, e.g.
	// "$LANES_INSTALL/deploy/kind-lane.sh up {cluster} {kubeconfig}" or a script of the workload).
	// Without them the bench never recreates or deletes a cluster.
	ClusterUp   string `json:"cluster_up,omitempty"`
	ClusterDown string `json:"cluster_down,omitempty"`
	// Evidence is the host directory under which each lane's operator evidence lands
	// (<evidence>/<cluster>, mounted on every node of the cluster at /bench-evidence). The bench never
	// cleans it. Required for a host with clusters. With provider vcluster nothing is mounted: the hooks pull the
	// evidence into this directory (BENCH_EVIDENCE_MODE=pull).
	Evidence string `json:"evidence"`
	// Provider says how the host's lane clusters are made: "kind" (default; docker containers as nodes) or
	// "vcluster" (one virtual cluster per lane on a shared host cluster; see docs/bench.md "Lane providers").
	Provider string `json:"provider,omitempty"`
	// EntryHost (provider vcluster) is the address of a node of the shared cluster: the app's NodePorts are
	// reached there (FUZZOPS_ENTRY_HOST).
	EntryHost string `json:"entry_host,omitempty"`
	// HostKubeconfig (provider vcluster) is the shared cluster's admin kubeconfig, an absolute path on the host
	// (VCLUSTER_HOST_KUBECONFIG). A file only a group can read is read through `sg <group>`
	// by deploy/vcluster-lane.sh (VCLUSTER_HOST_GROUP, default docker).
	HostKubeconfig string `json:"host_kubeconfig,omitempty"`
	// Lanes limits what each lane's nodes may use: disjoint whole cores out of lanes.cpus and the lane's memory,
	// applied when the lane is built (docs/bench.md, "Lane limits").
	Lanes HostLanes `json:"lanes,omitempty"`
	// Reserve is the host's lane block, applied by `lanes host reserve` through the root helper (reserve.go).
	Reserve *HostReserve `json:"reserve,omitempty"`
	// CrashLoop tunes the crash-loop detector (crashloop.go); the defaults suit every host.
	CrashLoop CrashLoopConf `json:"crashloop,omitempty"`
	// KernelMinimums are kernel settings the host must have at least (added to or overriding DefaultKernelMinimums);
	// admission reads the effective values (kernelmin.go).
	KernelMinimums map[string]int64 `json:"kernel_minimums,omitempty"`
	// Health tunes the host health gate (hosthealth.go); the defaults suit every host.
	Health HostHealthConf `json:"health,omitempty"`
	// Isolation "one-way-ok" lets lanes run on the host with no active reservation; its rows are flagged.
	Isolation string `json:"isolation,omitempty"`
}

// Lane providers.
const (
	ProviderKind     = "kind"
	ProviderVcluster = "vcluster"
)

// IsVcluster reports whether the host's lanes are virtual clusters on a shared cluster.
func (h Host) IsVcluster() bool { return h.Provider == ProviderVcluster }

// Default cluster commands of a vcluster host (run like any host command; they name their script through $LANES_INSTALL).
const (
	vclusterUp   = "$LANES_INSTALL/deploy/vcluster-lane.sh up {cluster} {kubeconfig} {evidence}"
	vclusterDown = "$LANES_INSTALL/deploy/vcluster-lane.sh down {cluster} {kubeconfig}"
)

// LaneEvidence is the lane's evidence directory on the host.
func (h Host) LaneEvidence(cluster string) string { return filepath.Join(h.Evidence, cluster) }

// EnvFor is the host env of a step of workload: EnvList with the workload's WorkloadEnv entries on top.
// DefaultWorkloadID is the workload_env key of a spec that names no workload.
const DefaultWorkloadID = "default"

func (h Host) EnvFor(workload string) []string {
	if workload == "" {
		workload = DefaultWorkloadID
	}
	extra := h.WorkloadEnv[workload]
	if len(extra) == 0 {
		return h.EnvList()
	}
	merged := h
	merged.Env = map[string]string{}
	for k, v := range h.Env {
		merged.Env[k] = v
	}
	for k, v := range extra {
		merged.Env[k] = v
	}
	return merged.EnvList()
}

// EnvList is Env as sorted KEY=VALUE pairs, plus the lane provider's variables (an explicit Env entry wins).
func (h Host) EnvList() []string {
	all := map[string]string{}
	if h.IsVcluster() {
		all["FUZZOPS_LANE_PROVIDER"] = ProviderVcluster
		all["BENCH_LANE_PROVIDER"] = ProviderVcluster
		all["BENCH_EVIDENCE_MODE"] = "pull"
		all["FUZZOPS_ENTRY_HOST"] = h.EntryHost
		all["VCLUSTER_HOST_KUBECONFIG"] = h.HostKubeconfig
	}
	for k, v := range h.Env {
		all[k] = v
	}
	var out []string
	for k, v := range all {
		out = append(out, k+"="+v)
	}
	sort.Strings(out)
	return out
}

// DefaultMaxBuildFailures is how many builds in a row may fail on a lane before it goes down, unless its host says.
const DefaultMaxBuildFailures = 2

// BuildFailureLimit is the host's MaxBuildFailures, or the default.
func (h Host) BuildFailureLimit() int {
	if h.MaxBuildFailures > 0 {
		return h.MaxBuildFailures
	}
	return DefaultMaxBuildFailures
}

// MinFreeBytes is MinFree in bytes (0: no floor).
func (h Host) MinFreeBytes() int64 {
	n, _ := ParseSize(h.MinFree)
	return n
}

// ParseSize reads "150G", "500M", "2T" or a byte count.
func ParseSize(s string) (int64, error) {
	s = strings.TrimSpace(strings.TrimSuffix(strings.ToUpper(s), "B"))
	if n := len(s); n >= 2 && s[n-1] == 'I' && strings.ContainsRune("KMGT", rune(s[n-2])) {
		s = s[:n-1] // Ki, Mi, Gi, Ti (KiB ...) mean the same as K, M, G, T
	}
	if s == "" {
		return 0, nil
	}
	mult := int64(1)
	switch s[len(s)-1] {
	case 'K':
		mult = 1 << 10
	case 'M':
		mult = 1 << 20
	case 'G':
		mult = 1 << 30
	case 'T':
		mult = 1 << 40
	}
	if mult > 1 {
		s = s[:len(s)-1]
	}
	var n float64
	if _, err := fmt.Sscan(s, &n); err != nil || n < 0 {
		return 0, fmt.Errorf("size %q: want a number with K, M, G or T", s)
	}
	return int64(n * float64(mult)), nil
}

// ClusterCommand is the cluster_up or cluster_down command for a lane ("" when the host has none).
func (h Host) ClusterCommand(tmpl, cluster string) string {
	if tmpl == "" {
		return ""
	}
	r := strings.NewReplacer("{cluster}", shellQuote(cluster), "{kubeconfig}", shellQuote(filepath.Join(h.Kubeconfigs, cluster)),
		"{evidence}", shellQuote(h.LaneEvidence(cluster)))
	return r.Replace(tmpl)
}

// Checkouts maps a pinned revision to its path on each host.
type Checkouts struct {
	Fuzzops map[string]map[string]string            `json:"fuzzops"`
	Hooks   map[string]map[string]map[string]string `json:"hooks"`
}

// Lane is one cluster on one host.
type Lane struct {
	Host, Cluster string
}

// LoadInventory reads and checks <root>/hosts.yaml.
func LoadInventory(root string) (Inventory, error) {
	p := filepath.Join(root, "hosts.yaml")
	b, err := os.ReadFile(p)
	if err != nil {
		return Inventory{}, err
	}
	var inv Inventory
	if err := miniyaml.Decode(b, &inv); err != nil {
		return Inventory{}, fmt.Errorf("%s: %w", p, err)
	}
	for name, h := range inv.Hosts {
		if h.IsVcluster() && h.ClusterUp == "" && h.ClusterDown == "" {
			h.ClusterUp, h.ClusterDown = vclusterUp, vclusterDown
			inv.Hosts[name] = h
		}
	}
	if err := inv.Validate(); err != nil {
		return Inventory{}, fmt.Errorf("%s: %w", p, err)
	}
	return inv, nil
}

// Validate checks the inventory: every host reachable one way, absolute
// paths, limits set, and every cluster on one host only.
func (inv Inventory) Validate() error {
	var errs []string
	owner := map[string]string{}
	for name, h := range inv.Hosts {
		if h.Local == (h.SSH != "") {
			errs = append(errs, fmt.Sprintf("host %s: exactly one of local and ssh", name))
		}
		for f, v := range map[string]string{"staging": h.Staging, "kubeconfigs": h.Kubeconfigs} {
			if !filepath.IsAbs(v) {
				errs = append(errs, fmt.Sprintf("host %s: %s must be an absolute path", name, f))
			}
		}
		if err := h.Reserve.validate(); err != nil {
			errs = append(errs, fmt.Sprintf("host %s: %v", name, err))
		}
		if h.Isolation != "" && h.Isolation != IsolationOneWayO {
			errs = append(errs, fmt.Sprintf("host %s: isolation %q: want one-way-ok or nothing", name, h.Isolation))
		}
		if _, err := ParseSize(h.MinFree); err != nil {
			errs = append(errs, fmt.Sprintf("host %s: min_free: %v", name, err))
		}
		if len(h.Clusters) > 0 && !filepath.IsAbs(h.Evidence) {
			errs = append(errs, fmt.Sprintf("host %s: evidence must be an absolute host directory (every lane mounts it)", name))
		}
		switch h.Provider {
		case "", ProviderKind:
			if h.EntryHost != "" || h.HostKubeconfig != "" {
				errs = append(errs, fmt.Sprintf("host %s: entry_host and host_kubeconfig belong to provider vcluster", name))
			}
		case ProviderVcluster:
			if h.EntryHost == "" {
				errs = append(errs, fmt.Sprintf("host %s: provider vcluster needs entry_host (a node address of the shared cluster)", name))
			}
			if !filepath.IsAbs(h.HostKubeconfig) {
				errs = append(errs, fmt.Sprintf("host %s: provider vcluster needs host_kubeconfig (absolute path)", name))
			}
		default:
			errs = append(errs, fmt.Sprintf("host %s: provider %q: want kind or vcluster", name, h.Provider))
		}
		for _, e := range h.CrashLoop.validate() {
			errs = append(errs, fmt.Sprintf("host %s: %s", name, e))
		}
		for _, e := range h.kernelValidate() {
			errs = append(errs, fmt.Sprintf("host %s: %s", name, e))
		}
		for _, e := range h.Health.validate() {
			errs = append(errs, fmt.Sprintf("host %s: %s", name, e))
		}
		for _, e := range h.Lanes.validate() {
			errs = append(errs, fmt.Sprintf("host %s: %s", name, e))
		}
		if h.ClusterUp != "" && !h.IsVcluster() && !strings.Contains(h.ClusterUp, "{evidence}") {
			errs = append(errs, fmt.Sprintf("host %s: cluster_up must mount {evidence} on every node", name))
		}
		if (h.ClusterUp == "") != (h.ClusterDown == "") {
			errs = append(errs, fmt.Sprintf("host %s: cluster_up and cluster_down go together", name))
		}
		// On a reserved host capacity is the reservation and load5 is a report, so the load gate is optional there.
		switch {
		case h.MaxBuilds < 1:
			errs = append(errs, fmt.Sprintf("host %s: max_builds must be positive", name))
		case h.MaxLoad5 < 0 || (h.MaxLoad5 == 0 && h.Reserve == nil):
			errs = append(errs, fmt.Sprintf("host %s: max_load5 must be positive (it may be omitted or 0, meaning no load gate, only on a host with reserve:)", name))
		}
		if h.MaxBuildFailures < 0 {
			errs = append(errs, fmt.Sprintf("host %s: max_build_failures must be at least 1 (omit it for the default %d)", name, DefaultMaxBuildFailures))
		}
		for _, c := range h.Clusters {
			if o, ok := owner[c]; ok {
				errs = append(errs, fmt.Sprintf("cluster %s is listed on %s and %s", c, o, name))
			}
			owner[c] = name
		}
	}
	if n := len(inv.localHosts()); n > 1 {
		errs = append(errs, "at most one local host")
	}
	if len(errs) > 0 {
		sort.Strings(errs)
		return fmt.Errorf("inventory: %s", strings.Join(errs, "; "))
	}
	return nil
}

func (inv Inventory) localHosts() []string {
	var out []string
	for n, h := range inv.Hosts {
		if h.Local {
			out = append(out, n)
		}
	}
	return out
}

// LocalHost is the host the manager runs on ("" when none is local).
func (inv Inventory) LocalHost() string {
	if l := inv.localHosts(); len(l) == 1 {
		return l[0]
	}
	return ""
}

// Lanes lists every lane, sorted by host then cluster.
func (inv Inventory) Lanes() []Lane {
	var out []Lane
	for name, h := range inv.Hosts {
		for _, c := range h.Clusters {
			out = append(out, Lane{name, c})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Host != out[j].Host {
			return out[i].Host < out[j].Host
		}
		return out[i].Cluster < out[j].Cluster
	})
	return out
}

// FuzzopsPath is the pinned fuzzops checkout for rev on host ("" when the host has none).
func (inv Inventory) FuzzopsPath(rev, host string) string {
	return lookupRev(inv.Checkouts.Fuzzops, rev)[host]
}

// HooksPath is the pinned checkout of hooks name at rev on host ("" when none).
func (inv Inventory) HooksPath(name, rev, host string) string {
	return lookupRev(inv.Checkouts.Hooks[name], rev)[host]
}

// lookupRev finds rev in m, by equal prefix either way (a short pin matches a long key and back).
func lookupRev(m map[string]map[string]string, rev string) map[string]string {
	for k, v := range m {
		if rev != "" && (strings.HasPrefix(k, rev) || strings.HasPrefix(rev, k)) {
			return v
		}
	}
	return nil
}

// UsableHosts are the spec's hosts, in its order, that have the pins it needs.
func (inv Inventory) UsableHosts(s Spec) []string {
	var out []string
	for _, h := range s.Hosts {
		if _, ok := inv.Hosts[h]; !ok || inv.FuzzopsPath(s.Fuzzops, h) == "" {
			continue
		}
		if s.Operator.Hooks != "" && inv.HooksPath(s.Operator.Hooks, s.Operator.Revision, h) == "" {
			continue
		}
		out = append(out, h)
	}
	return out
}
