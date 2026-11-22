package bench

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/self-defining-systems/sdo/benchmarks/lanes/internal/plan"
)

// Provider eks: every lane is its own Amazon EKS cluster, made and deleted by deploy/eks-lane.sh through eksctl and
// the aws CLI. There is no host to protect (the nodes are not shared and the node group autoscales into the account's
// quota), so the host-safety gates do not apply to it, by design: no core reservation or cpuset, no core and memory
// ledger, no kernel minimums, no ip-link canary or PSI (docs/host-safety.md, "EKS hosts"). Capacity is instead what an
// account allows: a cap on concurrent lanes, a vCPU quota and a $/h budget, with a running spend estimate.
//
// Steps run on the manager's machine (the host is `local`) with the lane's kubeconfig; there is no ssh host.

// ProviderEks is the lane provider whose lanes are EKS clusters.
const ProviderEks = "eks"

// IsEks reports whether the host's lanes are EKS clusters.
func (h Host) IsEks() bool { return h.Provider == ProviderEks }

// eksControlPlaneUSDPerHour is the price of one EKS control plane (standard support).
const eksControlPlaneUSDPerHour = 0.10

// Default cluster commands of an eks host (run like any host command; they name their script through $LANES_INSTALL).
const (
	eksClusterUp   = "$LANES_INSTALL/deploy/eks-lane.sh up {cluster} {kubeconfig} {evidence}"
	eksClusterDown = "$LANES_INSTALL/deploy/eks-lane.sh down {cluster} {kubeconfig}"
)

// DefaultEksIdleTimeout is how long a free lane's cluster is kept after its last use when no experiment wants the host.
const DefaultEksIdleTimeout = 10 * time.Minute

// DefaultMaxIdleWithWaiting is the longest a free lane's cluster is kept for work that is only waiting (a plan node
// blocked on an edge or retrying) before it is deleted anyway, with an alarm.
const DefaultMaxIdleWithWaiting = 60 * time.Minute

// HostEKS is the `eks:` block of a host with provider eks.
type HostEKS struct {
	Region string `json:"region"`
	// Account is checked against the credentials (aws sts get-caller-identity) by preflight and `up`.
	Account string `json:"account,omitempty"`
	// BudgetPerHour caps the estimated $/h of all lanes that exist (control planes and nodes); BudgetTotal, when
	// positive, caps the estimated spend of this host in total (what lanes already cost plus what the running ones cost).
	BudgetPerHour float64 `json:"budget_per_hour"`
	BudgetTotal   float64 `json:"budget_total,omitempty"`
	// VCPUQuota is the vCPUs of on-demand standard instances lanes may hold at once (the account's quota, less what
	// others use).
	VCPUQuota int `json:"vcpu_quota"`
	// MaxLanes caps the clusters that exist at once (0: the number of the host's clusters).
	MaxLanes int `json:"max_lanes,omitempty"`
	// InstanceFamilies are the families a node may be (cheapest that fits wins); default [m6i].
	InstanceFamilies []string `json:"instance_families,omitempty"`
	// MinNodeVCPUs is the smallest node (pod density: a 2 vCPU instance holds too few pods); default 4.
	MinNodeVCPUs int `json:"min_node_vcpus,omitempty"`
	// IdleTimeout keeps a free lane's cluster this long when no experiment wants the host ("10m"); a plan between two
	// problems does not delete it. `lanes gc` deletes at once.
	IdleTimeout string `json:"idle_timeout,omitempty"`
	// MaxIdleWithWaiting is the hard ceiling on a free lane kept for waiting work ("60m"): idle_timeout does not delete a
	// lane while a plan has nodes (queued, blocked on an edge, retrying) that the lane can serve, but a plan stalled for
	// ever must not hold clusters for ever. At the ceiling the lane is deleted and the alarm idle-ceiling is raised.
	MaxIdleWithWaiting string `json:"max_idle_with_waiting,omitempty"`
	// Subnets/VPC of the clusters, passed to the platform script; used by vcluster lanes (eks-vcluster.sh); a cluster lane uses the shared stack.
	VPC     string   `json:"vpc,omitempty"`
	Subnets []string `json:"subnets,omitempty"`
	// Lanes says what a lane is: "cluster" (default) is an EKS cluster of its own; "vcluster" is a virtual cluster on the
	// host's one long-lived EKS cluster (HostCluster), with nodes of its own that Karpenter makes (eks_vcluster.go).
	Lanes string `json:"lanes,omitempty"`
	// HostCluster is the long-lived cluster of lanes: vcluster (default lanes-host-<host>), made once by `lanes host adopt`.
	HostCluster string `json:"host_cluster,omitempty"`
	// SystemInstance is the node of the host cluster that holds Karpenter and the lanes' control planes (default m6i.xlarge).
	SystemInstance string `json:"system_instance,omitempty"`
	// FullLanes are the lanes of a vcluster host that are clusters of their own (a real kube-system and real nodes), for
	// the plans that need them (the fault matrices): they are labeled node-access=real and taken by a spec with that selector.
	FullLanes []string `json:"full_lanes,omitempty"`
	// LocalKind makes the host cluster a kind cluster on this machine instead of an EKS cluster (lanes: vcluster only): no
	// AWS account, credentials, eksctl or Karpenter, and no cost. Each lane still is a vcluster with nodes of its own (kind
	// workers labeled and tainted for it), with the same virtual-lane facts, NetworkPolicy sync and local-path storage, and
	// its images go to a local registry the nodes mirror from instead of ECR. It rehearses the vcluster path of a plan
	// for free (deploy/eks-vcluster.sh, EKS_LOCAL_KIND).
	LocalKind bool `json:"local_kind,omitempty"`
	// LocalRegistryPort is the port of the local registry of a LocalKind host (default 5611).
	LocalRegistryPort int `json:"local_registry_port,omitempty"`
}

// DefaultLocalRegistryPort is the registry port of a LocalKind host.
const DefaultLocalRegistryPort = 5611

// eksFamily is the price and memory shape of an instance family (general purpose, compute, memory optimised).
type eksFamily struct {
	USDPerVCPUHour float64
	GiBPerVCPU     float64
}

// eksFamilies are the on-demand (Linux, us-west-2) rates per vCPU: an estimate, not a bill.
var eksFamilies = map[string]eksFamily{
	"m5": {0.048, 4}, "m6i": {0.048, 4}, "m7i": {0.0504, 4},
	"c5": {0.0425, 2}, "c6i": {0.0425, 2}, "c7i": {0.04463, 2},
	"r5": {0.063, 8}, "r6i": {0.063, 8}, "r7i": {0.06615, 8},
}

// eksSizes are the instance sizes of a family and their vCPUs.
var eksSizes = []struct {
	Name  string
	VCPUs int
}{{"large", 2}, {"xlarge", 4}, {"2xlarge", 8}, {"4xlarge", 16}, {"8xlarge", 32}, {"12xlarge", 48}, {"16xlarge", 64}}

func (e *HostEKS) families() []string {
	if len(e.InstanceFamilies) == 0 {
		return []string{"m6i"}
	}
	return e.InstanceFamilies
}

func (e *HostEKS) minNodeVCPUs() int {
	if e.MinNodeVCPUs > 0 {
		return e.MinNodeVCPUs
	}
	return 4
}

// Registry is the ECR host of the account and region: images are pushed there and nodes pull from it.
func (e *HostEKS) Registry() string {
	if e.LocalKind {
		port := e.LocalRegistryPort
		if port == 0 {
			port = DefaultLocalRegistryPort
		}
		return "localhost:" + strconv.Itoa(port)
	}
	return e.Account + ".dkr.ecr." + e.Region + ".amazonaws.com"
}

// BuildLimit is how many builds (and releases, resets) may run on the host at once. A kind host shares one kernel's
// network-namespace lock, so max_builds bounds its churn; each eks lane is its own cluster and its builds wait on AWS,
// not on this machine, so a plan's lanes are made in parallel, bounded by max_lanes, the vCPU quota and the budget.
func (h Host) BuildLimit() int {
	if h.IsEks() {
		if h.EKS.MaxLanes > 0 {
			return h.EKS.MaxLanes
		}
		return max(len(h.Clusters), 1)
	}
	return h.MaxBuilds
}

// RecreatesAfterFailure says whether a failed build or gate sends the lane to a new cluster (delete, then create) as a last
// resort: a kind host can make one in a minute or two. An eks cluster takes about twenty minutes to make, so a failure is
// retried in place and, when it repeats, the lane goes Down and its cluster is deleted (eks.go): the cost of a retry is
// a reset in place, not a cluster.
func (h Host) RecreatesAfterFailure(lane string) bool {
	return h.ClusterUp != "" && (!h.IsEks() || h.EKS.virtualLane(lane))
}

// Idle is the idle timeout (default DefaultEksIdleTimeout; a virtual lane is made in seconds, so DefaultVirtualIdleTimeout).
func (e *HostEKS) Idle() time.Duration {
	d, err := time.ParseDuration(e.IdleTimeout)
	if err != nil || d <= 0 {
		if e.virtual() {
			return DefaultVirtualIdleTimeout
		}
		return DefaultEksIdleTimeout
	}
	return d
}

// MaxIdleWaiting is the ceiling of a lane kept for waiting work (default DefaultMaxIdleWithWaiting); never below Idle.
func (e *HostEKS) MaxIdleWaiting() time.Duration {
	d, err := time.ParseDuration(e.MaxIdleWithWaiting)
	if err != nil || d <= 0 {
		d = DefaultMaxIdleWithWaiting
	}
	return max(d, e.Idle())
}

func (e *HostEKS) validate(h Host) []string {
	var errs []string
	if e == nil {
		return []string{"provider eks needs an eks: block (region, budget_per_hour, vcpu_quota)"}
	}
	if e.Region == "" {
		errs = append(errs, "eks.region is required")
	}
	if e.Account == "" && !e.LocalKind {
		errs = append(errs, "eks.account is required (the image registry is <account>.dkr.ecr.<region>.amazonaws.com)")
	}
	if e.LocalKind && !e.virtual() {
		errs = append(errs, "eks.local_kind needs lanes: vcluster (the host cluster is one local kind cluster)")
	}
	if e.LocalRegistryPort != 0 && !e.LocalKind {
		errs = append(errs, "eks.local_registry_port belongs to eks.local_kind")
	}
	if e.BudgetPerHour <= 0 {
		errs = append(errs, "eks.budget_per_hour must be positive")
	}
	if e.BudgetTotal < 0 {
		errs = append(errs, "eks.budget_total must not be negative")
	}
	if e.VCPUQuota < 1 {
		errs = append(errs, "eks.vcpu_quota must be positive")
	}
	if e.MaxLanes < 0 {
		errs = append(errs, "eks.max_lanes must not be negative")
	}
	for _, f := range e.InstanceFamilies {
		if _, ok := eksFamilies[f]; !ok {
			errs = append(errs, fmt.Sprintf("eks.instance_families: unknown family %q (known: %s)", f, strings.Join(knownEksFamilies(), ", ")))
		}
	}
	if e.IdleTimeout != "" {
		if d, err := time.ParseDuration(e.IdleTimeout); err != nil || d <= 0 {
			errs = append(errs, fmt.Sprintf("eks.idle_timeout %q: want a duration such as 10m", e.IdleTimeout))
		}
	}
	if e.MaxIdleWithWaiting != "" {
		if d, err := time.ParseDuration(e.MaxIdleWithWaiting); err != nil || d <= 0 {
			errs = append(errs, fmt.Sprintf("eks.max_idle_with_waiting %q: want a duration such as 60m", e.MaxIdleWithWaiting))
		}
	}
	errs = append(errs, e.validateLanes()...)
	if len(e.Subnets) > 0 && e.VPC == "" {
		errs = append(errs, "eks.subnets needs eks.vpc")
	}
	// The host-safety gates do not exist for an eks host; a setting for one is a mistake, not a no-op.
	switch {
	case h.Reserve != nil:
		errs = append(errs, "provider eks has no host to reserve: remove reserve")
	case h.Lanes.Enabled():
		errs = append(errs, "provider eks has no core or memory ledger: remove lanes")
	case len(h.KernelMinimums) > 0:
		errs = append(errs, "provider eks has no kernel: remove kernel_minimums")
	case !h.Local:
		errs = append(errs, "provider eks runs its steps on the manager's machine: set local: true, not ssh")
	}
	if e.MaxLanes > 0 && len(h.Clusters) < e.MaxLanes {
		errs = append(errs, fmt.Sprintf("eks.max_lanes %d, but clusters lists only %d lane names", e.MaxLanes, len(h.Clusters)))
	}
	return errs
}

func knownEksFamilies() []string {
	var out []string
	for f := range eksFamilies {
		out = append(out, f)
	}
	sort.Strings(out)
	return out
}

// EksPlan is the cluster a lane gets: nodes of one instance type.
type EksPlan struct {
	InstanceType string  `json:"instance_type"`
	Nodes        int     `json:"nodes"`
	VCPUs        int     `json:"vcpus"` // all nodes together
	USDPerHour   float64 `json:"usd_per_hour"`
	// Virtual: the lane is a vcluster of the host cluster (no control plane of its own; eks_vcluster.go).
	Virtual bool `json:"virtual,omitempty"`
}

// PlanLane sizes a lane's cluster from the workload's footprint: two nodes of the cheapest instance type that holds
// half of it (one node when it asks for fewer than 4 cores), as a kind lane has two nodes. The control plane is in
// USDPerHour.
func (e *HostEKS) PlanLane(l LaneLimit) (EksPlan, error) { return e.planLane(l, e.virtual()) }

// PlanLaneFor is PlanLane for the named lane of a host (a full-cluster lane of a vcluster host has a control plane of its own).
func (e *HostEKS) PlanLaneFor(lane string, l LaneLimit) (EksPlan, error) {
	return e.planLane(l, e.virtualLane(lane))
}

func (e *HostEKS) planLane(l LaneLimit, virtual bool) (EksPlan, error) {
	mem, err := ParseSize(l.Memory)
	if err != nil || mem <= 0 || l.CPUs < 1 {
		return EksPlan{}, fmt.Errorf("lane footprint %d cores, memory %q is not a size", l.CPUs, l.Memory)
	}
	memGiB := float64(mem) / (1 << 30)
	nodes := 2
	if l.CPUs < 4 {
		nodes = 1
	}
	needCPU := int(math.Ceil(float64(l.CPUs) / float64(nodes)))
	needMem := memGiB / float64(nodes)
	var best EksPlan
	for _, fam := range e.families() {
		f := eksFamilies[fam]
		for _, sz := range eksSizes {
			if sz.VCPUs < max(needCPU, e.minNodeVCPUs()) || float64(sz.VCPUs)*f.GiBPerVCPU < needMem {
				continue
			}
			cost := float64(nodes) * float64(sz.VCPUs) * f.USDPerVCPUHour
			if !virtual {
				cost += eksControlPlaneUSDPerHour // a virtual lane shares the host cluster's control plane
			}
			if best.InstanceType == "" || cost < best.USDPerHour {
				best = EksPlan{InstanceType: fam + "." + sz.Name, Nodes: nodes, VCPUs: nodes * sz.VCPUs, USDPerHour: round4(cost), Virtual: virtual}
			}
			break // the smallest size of this family that fits
		}
	}
	if best.InstanceType == "" {
		return EksPlan{}, fmt.Errorf("no instance type of %v holds a node of %d cores and %.0f GiB", e.families(), needCPU, needMem)
	}
	return best, nil
}

func round4(f float64) float64 { return math.Round(f*1e4) / 1e4 }

// eksLedger is <root>/eks/<host>.json: the clusters that exist now and what the finished ones cost.
type eksLedger struct {
	Open map[string]eksOpen `json:"open"`
	// ClosedUSD is the estimated cost of the lanes deleted so far.
	ClosedUSD float64   `json:"closed_usd"`
	Closed    []eksSpan `json:"closed,omitempty"`
	// Lanes: vcluster. HostSince is when the host cluster began to bill (its control plane); SystemUSD is what its system
	// node cost while lanes existed; Settled is the last pass that added to it.
	HostSince time.Time `json:"host_since,omitempty"`
	SystemUSD float64   `json:"system_usd,omitempty"`
	Settled   time.Time `json:"settled,omitempty"`
}

type eksOpen struct {
	Since time.Time `json:"since"`
	EksPlan
}

type eksSpan struct {
	Lane         string    `json:"lane"`
	InstanceType string    `json:"instance_type"`
	Nodes        int       `json:"nodes"`
	Since        time.Time `json:"since"`
	Until        time.Time `json:"until"`
	USD          float64   `json:"usd"`
}

func (r Root) eksLedgerPath(host string) string { return r.path("eks", host+".json") }

func (m *Manager) eksLedger(host string) *eksLedger {
	if m.eks == nil {
		m.eks = map[string]*eksLedger{}
	}
	if l := m.eks[host]; l != nil {
		return l
	}
	l := &eksLedger{Open: map[string]eksOpen{}}
	_ = readJSON(m.Root.eksLedgerPath(host), l)
	if l.Open == nil {
		l.Open = map[string]eksOpen{}
	}
	m.eks[host] = l
	return l
}

func (l *eksLedger) vcpus() (n int) {
	for _, o := range l.Open {
		n += o.VCPUs
	}
	return n
}

func (l *eksLedger) hourly() (usd float64) {
	for _, o := range l.Open {
		usd += o.USDPerHour
	}
	return usd
}

// spend is the estimated cost so far: finished lanes plus the running ones up to now.
func (l *eksLedger) spend(now time.Time) float64 {
	usd := l.ClosedUSD + l.SystemUSD
	if !l.HostSince.IsZero() {
		usd += eksControlPlaneUSDPerHour * now.Sub(l.HostSince).Hours()
	}
	for _, o := range l.Open {
		usd += o.USDPerHour * now.Sub(o.Since).Hours()
	}
	return round4(usd)
}

// EksStatus is what `lanes status` shows of an eks host.
type EksStatus struct {
	Lanes       int     `json:"lanes"`
	MaxLanes    int     `json:"max_lanes"`
	VCPUs       int     `json:"vcpus"`
	VCPUQuota   int     `json:"vcpu_quota"`
	USDPerHour  float64 `json:"usd_per_hour"`
	BudgetPerHr float64 `json:"budget_per_hour"`
	SpendUSD    float64 `json:"spend_usd"`
	BudgetTotal float64 `json:"budget_total,omitempty"`
	// Virtual: lanes are vclusters; USDPerHour and VCPUs then include the host cluster (its control plane, and its system node while lanes exist).
	Virtual bool `json:"virtual,omitempty"`
}

func (e EksStatus) String() string {
	s := fmt.Sprintf("eks: %d/%d lanes, %d/%d vCPU, $%.2f/h of $%.2f/h, spent ~$%.2f", e.Lanes, e.MaxLanes, e.VCPUs, e.VCPUQuota, e.USDPerHour, e.BudgetPerHr, e.SpendUSD)
	if e.Virtual {
		s = strings.Replace(s, "eks: ", "eks (vcluster lanes, host included): ", 1)
	}
	if e.BudgetTotal > 0 {
		s += fmt.Sprintf(" of $%.2f", e.BudgetTotal)
	}
	return s
}

func (m *Manager) eksStatus(host string) EksStatus {
	h := m.inv.Hosts[host]
	l := m.eksLedger(host)
	max := h.EKS.MaxLanes
	if max == 0 {
		max = len(h.Clusters)
	}
	return EksStatus{Lanes: len(l.Open), MaxLanes: max, VCPUs: l.vcpus() + l.hostVCPUs(h.EKS, false), VCPUQuota: h.EKS.VCPUQuota, USDPerHour: round4(l.hourly() + l.hostHourly(h.EKS, false)),
		BudgetPerHr: h.EKS.BudgetPerHour, SpendUSD: l.spend(m.now()), BudgetTotal: h.EKS.BudgetTotal, Virtual: h.EKS.virtual()}
}

// eksAdmit says why a cluster of plan may not be made on host now ("" when it may): too many lanes, the vCPU quota, the
// $/h budget or the total budget.
func (m *Manager) eksAdmit(host string, p EksPlan) string {
	h := m.inv.Hosts[host]
	l := m.eksLedger(host)
	st := m.eksStatus(host)
	if p.Virtual && l.HostSince.IsZero() {
		return fmt.Sprintf("host %s: its host cluster %s is not adopted: run `lanes host adopt %s`", host, h.EKS.hostCluster(host), host)
	}
	// A virtual lane is on top of the host cluster: its control plane and the system node it runs lanes from.
	hostVCPUs, hostUSD := l.hostVCPUs(h.EKS, p.Virtual), l.hostHourly(h.EKS, p.Virtual)
	switch {
	case st.Lanes >= st.MaxLanes:
		return fmt.Sprintf("host %s runs max_lanes (%d) EKS clusters", host, st.MaxLanes)
	case l.vcpus()+hostVCPUs+p.VCPUs > h.EKS.VCPUQuota:
		return fmt.Sprintf("host %s: a lane of %d vCPUs does not fit the vCPU quota (%d of %d in use)", host, p.VCPUs, l.vcpus()+hostVCPUs, h.EKS.VCPUQuota)
	case l.hourly()+hostUSD+p.USDPerHour > h.EKS.BudgetPerHour+1e-9:
		return fmt.Sprintf("host %s: a lane at $%.2f/h does not fit the budget ($%.2f/h of $%.2f/h in use)", host, p.USDPerHour, l.hourly()+hostUSD, h.EKS.BudgetPerHour)
	case h.EKS.BudgetTotal > 0 && l.spend(m.now())+p.USDPerHour > h.EKS.BudgetTotal:
		return fmt.Sprintf("host %s: spent ~$%.2f of the total budget $%.2f", host, l.spend(m.now()), h.EKS.BudgetTotal)
	}
	return ""
}

// eksPlanFor is the cluster the lane gets for the experiment it is bound to (the default footprint when it has none).
func (m *Manager) eksPlanFor(host, lane string, s Spec) (EksPlan, error) {
	return m.inv.Hosts[host].EKS.PlanLaneFor(lane, s.LaneLimit())
}

// eksExists is whether lane l has a cluster, or a build is making it.
func eksExists(l *LaneFile) bool {
	return !l.Gone || (l.Step != nil && l.Step.Kind == StepBuild)
}

// eksSettle makes the ledger say which clusters exist: a lane with a cluster (or one being made) is open from the pass
// that sees it, a lane without one is closed and its cost added. Run once per pass.
func (m *Manager) eksSettle() {
	for name, h := range m.inv.Hosts {
		if !h.IsEks() {
			continue
		}
		l := m.eksLedger(name)
		changed := m.eksHostSettle(name, h, l)
		for _, lf := range m.sortedLanes() {
			if lf.Host != name {
				continue
			}
			o, open := l.Open[lf.Lane]
			switch {
			case eksExists(lf) && !open:
				var spec Spec
				if x := m.exps[lf.Experiment]; x != nil {
					spec = x.Measured()
				}
				p, err := m.eksPlanFor(name, lf.Lane, spec)
				if err != nil {
					p, _ = h.EKS.PlanLaneFor(lf.Lane, DefaultLaneLimit())
				}
				l.Open[lf.Lane] = eksOpen{Since: m.now().UTC(), EksPlan: p}
				changed = true
			case !eksExists(lf) && open:
				usd := round4(o.USDPerHour * m.now().Sub(o.Since).Hours())
				l.Closed = append(l.Closed, eksSpan{Lane: lf.Lane, InstanceType: o.InstanceType, Nodes: o.Nodes, Since: o.Since, Until: m.now().UTC(), USD: usd})
				l.ClosedUSD = round4(l.ClosedUSD + usd)
				delete(l.Open, lf.Lane)
				m.logf("eks %s: lane %s deleted; %s x %d for %s cost ~$%.2f (host total ~$%.2f)", name, lf.Lane, o.InstanceType, o.Nodes, m.now().Sub(o.Since).Round(time.Second), usd, l.spend(m.now()))
				changed = true
			}
		}
		if changed || m.eksSaved[name].IsZero() || m.now().Sub(m.eksSaved[name]) > time.Minute {
			if m.eksSaved == nil {
				m.eksSaved = map[string]time.Time{}
			}
			m.eksSaved[name] = m.now()
			_ = os.MkdirAll(filepath.Dir(m.Root.eksLedgerPath(name)), 0o755)
			_ = writeJSON(m.Root.eksLedgerPath(name), l)
		}
		if st := m.eksStatus(name); st.USDPerHour > h.EKS.BudgetPerHour+1e-9 || st.VCPUs > h.EKS.VCPUQuota {
			m.alarm("eks-over-budget", name, st.String())
		} else {
			m.alarmClear("eks-over-budget", name)
		}
	}
}

// eksIdle says whether a free lane's cluster may be deleted for being unused: it has been free for the host's idle
// timeout and no plan has waiting work the lane could serve. Waiting work (a node queued, blocked on an edge or
// retrying) keeps the lane until the ceiling max_idle_with_waiting, where it is deleted with the alarm idle-ceiling.
func (m *Manager) eksIdle(l *LaneFile) bool {
	h := m.inv.Hosts[l.Host]
	if !h.IsEks() {
		return false
	}
	free := m.now().Sub(l.Since)
	if free < h.EKS.Idle() {
		return false
	}
	if n := m.planWaitingFor(l); n > 0 {
		if free < h.EKS.MaxIdleWaiting() {
			return false
		}
		m.alarm("idle-ceiling", l.Host+"/"+l.Lane, fmt.Sprintf("lane free for %s with %d plan node(s) still waiting for it; deleting its cluster (max_idle_with_waiting %s)",
			free.Round(time.Minute), n, h.EKS.MaxIdleWaiting()))
	}
	return true
}

// planWaitingFor counts the waiting nodes of the plans (queued, blocked on an edge, retrying) whose spec the lane can
// serve: its host is usable for the spec and its labels match the spec's selector. A lane that exists serves any such
// node whatever its footprint (a lane's cluster is sized when it is made).
func (m *Manager) planWaitingFor(l *LaneFile) int {
	n := 0
	for _, name := range plan.Names(m.Root.PlansDir()) {
		specs, err := plan.PendingSpecs(plan.Dir(m.Root.PlansDir(), name))
		if err != nil {
			continue
		}
		for _, b := range specs {
			s, err := ParseSpec(b)
			if err != nil {
				continue
			}
			if slices.Contains(m.inv.UsableHosts(s), l.Host) && m.inv.LaneAccepts(l.Host, l.Lane, s) {
				n++
			}
		}
	}
	return n
}

// eksEnv are the cluster-up variables of a lane: its size, from the workload's footprint, the account, and the directory
// where the host's shared network ids are recorded (stateDir).
func eksEnv(h Host, p EksPlan, stateDir string) []string {
	e := h.EKS
	env := []string{"EKS_REGION=" + e.Region, "EKS_INSTANCE_TYPE=" + p.InstanceType, "EKS_NODES=" + strconv.Itoa(p.Nodes),
		"EKS_NODE_MAX=" + strconv.Itoa(p.Nodes+1), "EKS_STATE_DIR=" + stateDir}
	if e.virtual() && p.Nodes > 0 {
		env = append(env, "EKS_NODE_VCPUS="+strconv.Itoa(p.VCPUs/p.Nodes))
	}
	if e.Account != "" {
		env = append(env, "EKS_ACCOUNT="+e.Account)
	}
	if e.VPC != "" {
		env = append(env, "EKS_VPC="+e.VPC, "EKS_SUBNETS="+strings.Join(e.Subnets, ","))
	}
	return env
}

func shQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'" }

// eksSharedScript runs a shared-resource verb (host-setup, host-teardown) of the platform script on an eks host.
func eksSharedScript(h Host, root Root, platform, verb string) string {
	env := []string{"EKS_REGION=" + h.EKS.Region, "EKS_STATE_DIR=" + root.path("eks")}
	if h.EKS.Account != "" {
		env = append(env, "EKS_ACCOUNT="+h.EKS.Account)
	}
	var b strings.Builder
	for _, kv := range env {
		k, v, _ := strings.Cut(kv, "=")
		fmt.Fprintf(&b, "export %s=%s; ", k, shQuote(v))
	}
	fmt.Fprintf(&b, "bash %s %s", shQuote(platform+"/deploy/eks-lane.sh"), verb)
	return b.String()
}

// eksSharedHost is `lanes host adopt|teardown` of an eks host: the account's shared network and roles that every lane of the
// host reuses are made (idempotently) or removed (refused while a lane cluster exists).
func eksSharedHost(ctx context.Context, root Root, h Host, host, verb string, run runFn, out io.Writer) error {
	if run == nil {
		return errors.New("no host runner")
	}
	dir, err := InstallPlatformScripts(h.Staging)
	if err != nil {
		return fmt.Errorf("install the platform scripts: %w", err)
	}
	script := "host-setup"
	if verb == "teardown" {
		script = "host-teardown"
	}
	o, err := run(ctx, host, eksSharedScript(h, root, dir, script))
	fmt.Fprint(out, host+":\n"+o)
	return err
}

// eksProbeBound bounds one lane health read.
const eksProbeBound = 45 * time.Second

// observeEksLanes reads each existing EKS lane's cluster: the API must answer and a node be Ready. A lane that fails
// gets no new work (the reason is its idle reason, alarm eks-lane-unhealthy once); nothing is deleted. A failed read of
// a lane that is mid-build is no reading (its cluster is being made), and a lane with no kubeconfig has no cluster to read.
func (m *Manager) observeEksLanes(ctx context.Context, reachable map[string]bool) {
	if m.eksWhy == nil {
		m.eksWhy = map[string]string{}
	}
	type read struct{ err error }
	reads := map[string]*read{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, l := range m.sortedLanes() {
		h := m.inv.Hosts[l.Host]
		if !h.IsEks() || !reachable[l.Host] || l.Gone || l.Step != nil {
			continue
		}
		lane, kc := l.Lane, filepath.Join(h.Kubeconfigs, l.Lane)
		if _, err := os.Stat(kc); errors.Is(err, os.ErrNotExist) {
			// No kubeconfig, no cluster (a cluster-up that failed removes what it made): nothing to read, and the lane's
			// next build makes it. A read of it would fail for ever and gate the very build that fixes it.
			delete(m.eksWhy, lane)
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := m.eksCheck(ctx, lane, kc)
			mu.Lock()
			reads[lane] = &read{err}
			mu.Unlock()
		}()
	}
	wg.Wait()
	for _, l := range m.sortedLanes() {
		r := reads[l.Lane]
		if r == nil {
			continue
		}
		was := m.eksWhy[l.Lane]
		why := ""
		if r.err != nil {
			why = fmt.Sprintf("lane %s is unhealthy: %v", l.Lane, r.err)
		}
		m.eksWhy[l.Lane] = why
		switch {
		case why != "" && was == "":
			m.logf("ALARM %s, no new steps on it", why)
			m.raiseLaneAlarm("eks-lane-unhealthy", l, why)
		case why == "" && was != "":
			m.logf("lane %s: API answers and a node is Ready again, new steps resume", l.Lane)
		}
	}
}

// eksCheck is the EKS lane's health: the API server answers and at least one node is Ready.
func (m *Manager) eksCheck(ctx context.Context, lane, kubeconfig string) error {
	if m.EksCheck != nil {
		return m.EksCheck(ctx, lane, kubeconfig)
	}
	ctx, cancel := context.WithTimeout(ctx, eksProbeBound)
	defer cancel()
	// The host's env (its PATH) decides which kubectl runs, as for every step on the host: sh resolves it, not this process.
	cmd := exec.CommandContext(ctx, "sh", "-c", `exec kubectl "$@"`, "sh", "--kubeconfig", kubeconfig, "--context", "kind-"+lane, "--request-timeout=30s", "get", "nodes",
		"-o", `jsonpath={range .items[*]}{.metadata.name}{"="}{range .status.conditions[?(@.type=="Ready")]}{.status}{end}{"\n"}{end}`)
	cmd.Env = os.Environ()
	if l := m.lanes[lane]; l != nil {
		cmd.Env = append(cmd.Env, m.inv.Hosts[l.Host].EnvList()...)
	}
	out, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("EKS API not reachable: %v", err)
	}
	return judgeNodesReady(string(out))
}

// judgeNodesReady reads `name=True|False|Unknown` lines.
func judgeNodesReady(out string) error {
	ready, total := 0, 0
	for _, ln := range strings.Split(out, "\n") {
		if strings.TrimSpace(ln) == "" {
			continue
		}
		total++
		if strings.HasSuffix(ln, "=True") {
			ready++
		}
	}
	if ready == 0 {
		return fmt.Errorf("no Ready node (%d nodes)", total)
	}
	return nil
}

// eksLaneAdmitted says whether a build of s may take lane l: a lane that has its cluster always may; one that needs a
// new cluster only when the host's quota and budget hold it.
func (m *Manager) eksLaneAdmitted(l *LaneFile, s Spec) bool {
	h := m.inv.Hosts[l.Host]
	if !h.IsEks() || !l.Gone {
		return true
	}
	p, err := h.EKS.PlanLaneFor(l.Lane, s.LaneLimit())
	if err != nil {
		return false
	}
	if why := m.eksAdmit(l.Host, p); why != "" {
		m.eksHeld(l.Host, why)
		return false
	}
	return true
}

// eksHeld logs why a new cluster waits, once per reason.
func (m *Manager) eksHeld(host, why string) {
	if m.eksHeldWhy == nil {
		m.eksHeldWhy = map[string]string{}
	}
	if m.eksHeldWhy[host] != why {
		m.eksHeldWhy[host] = why
		m.logf("%s: new EKS lane waits", why)
	}
}

// eksAccountScript prints the account the credentials belong to ("account ID"), empty when they do not work.
func eksAccountScript(e *HostEKS) string {
	if e != nil && e.LocalKind {
		return `echo "account local-kind"` // no AWS: the host cluster is a local kind cluster
	}
	return `a=$(aws sts get-caller-identity --query Account --output text 2>/dev/null) && echo "account $a"; true`
}

// judgeEksAccount: the credentials work and, when the host names an account, belong to it.
func judgeEksAccount(out string, err error, e *HostEKS) (bool, string) {
	if err != nil {
		return false, fmt.Sprintf("probe failed: %v", err)
	}
	var account string
	for _, ln := range strings.Split(out, "\n") {
		if f := strings.Fields(ln); len(f) == 2 && f[0] == "account" {
			account = f[1]
		}
	}
	switch {
	case account == "":
		return false, "aws sts get-caller-identity failed: no usable AWS credentials in the step environment"
	case e != nil && e.LocalKind:
	case e != nil && e.Account != "" && account != e.Account:
		return false, fmt.Sprintf("credentials belong to account %s, the host is configured for %s", account, e.Account)
	}
	return true, fmt.Sprintf("account %s, region %s, up to %d vCPUs and $%.2f/h", account, e.Region, e.VCPUQuota, e.BudgetPerHour)
}

// statusEksHost is `lanes host status` of an eks host: whether the credentials work and what the clusters cost so far.
func statusEksHost(ctx context.Context, root Root, inv Inventory, host string, run runFn, out io.Writer) {
	h := inv.Hosts[host]
	if run != nil {
		o, err := run(ctx, host, eksAccountScript(h.EKS))
		ok, detail := judgeEksAccount(o, err, h.EKS)
		state := "credentials ok"
		if !ok {
			state = "NOT ADMITTED, credentials"
		}
		fmt.Fprintf(out, "%s: %s: %s\n", host, state, detail)
	}
	l := &eksLedger{Open: map[string]eksOpen{}}
	_ = readJSON(root.eksLedgerPath(host), l)
	max := h.EKS.MaxLanes
	if max == 0 {
		max = len(h.Clusters)
	}
	if h.EKS.virtual() && l.HostSince.IsZero() {
		var rec eksHostRecord
		if readJSON(root.eksHostRecordPath(host), &rec) == nil {
			l.HostSince = rec.Since
		}
	}
	st := EksStatus{Lanes: len(l.Open), MaxLanes: max, VCPUs: l.vcpus() + l.hostVCPUs(h.EKS, false), VCPUQuota: h.EKS.VCPUQuota,
		USDPerHour: round4(l.hourly() + l.hostHourly(h.EKS, false)), BudgetPerHr: h.EKS.BudgetPerHour, SpendUSD: l.spend(time.Now()),
		BudgetTotal: h.EKS.BudgetTotal, Virtual: h.EKS.virtual()}
	fmt.Fprintf(out, "%s: %s\n", host, st)
	for lane, o := range l.Open {
		fmt.Fprintf(out, "%s: lane %s: %s x %d since %s\n", host, lane, o.InstanceType, o.Nodes, o.Since.Format(time.RFC3339))
	}
	fmt.Fprintf(out, "%s: no cpuset, ledger, kernel or PSI gates (provider eks)\n", host)
}

// eksCleanupRetry is how long to wait before asking again to delete a Down lane's cluster.
const eksCleanupRetry = 10 * time.Minute

// eksOrphanLanes are the lanes of eks hosts that are Down yet may still have a cluster (a failed or killed cluster-up):
// nothing will build on a Down lane, so its cluster is deleted, not left to bill. Asked again every eksCleanupRetry.
func (m *Manager) eksOrphanLanes() []*LaneFile {
	var out []*LaneFile
	for _, l := range m.sortedLanes() {
		h := m.inv.Hosts[l.Host]
		if !h.IsEks() || h.ClusterDown == "" || l.State != LaneDown || l.Gone || l.Step != nil {
			continue
		}
		if t, ok := m.eksCleanAt[l.Lane]; ok && m.now().Sub(t) < eksCleanupRetry {
			continue
		}
		out = append(out, l)
	}
	return out
}

// eksCleanDown deletes the clusters of Down eks lanes (a release that deletes the cluster).
func (m *Manager) eksCleanDown(ctx context.Context, admit map[string]bool) {
	for _, l := range m.eksOrphanLanes() {
		if !admit[l.Host] {
			continue
		}
		if m.eksCleanAt == nil {
			m.eksCleanAt = map[string]time.Time{}
		}
		m.eksCleanAt[l.Lane] = m.now()
		m.logf("lane %s is down: deleting its cluster so it does not bill", l.Lane)
		m.start(ctx, l, Step{Kind: StepRelease, Prev: l.Prepared, DeleteCluster: true})
	}
}
