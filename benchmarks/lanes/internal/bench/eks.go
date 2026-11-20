package bench

import (
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
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
	// Subnets/VPC of the clusters, passed to the platform script; empty: eksctl makes a VPC.
	VPC     string   `json:"vpc,omitempty"`
	Subnets []string `json:"subnets,omitempty"`
}

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
	return e.Account + ".dkr.ecr." + e.Region + ".amazonaws.com"
}

// Idle is the idle timeout (default DefaultEksIdleTimeout).
func (e *HostEKS) Idle() time.Duration {
	d, err := time.ParseDuration(e.IdleTimeout)
	if err != nil || d <= 0 {
		return DefaultEksIdleTimeout
	}
	return d
}

func (e *HostEKS) validate(h Host) []string {
	var errs []string
	if e == nil {
		return []string{"provider eks needs an eks: block (region, budget_per_hour, vcpu_quota)"}
	}
	if e.Region == "" {
		errs = append(errs, "eks.region is required")
	}
	if e.Account == "" {
		errs = append(errs, "eks.account is required (the image registry is <account>.dkr.ecr.<region>.amazonaws.com)")
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
}

// PlanLane sizes a lane's cluster from the workload's footprint: two nodes of the cheapest instance type that holds
// half of it (one node when it asks for fewer than 4 cores), as a kind lane has two nodes. The control plane is in
// USDPerHour.
func (e *HostEKS) PlanLane(l LaneLimit) (EksPlan, error) {
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
			cost := float64(nodes)*float64(sz.VCPUs)*f.USDPerVCPUHour + eksControlPlaneUSDPerHour
			if best.InstanceType == "" || cost < best.USDPerHour {
				best = EksPlan{InstanceType: fam + "." + sz.Name, Nodes: nodes, VCPUs: nodes * sz.VCPUs, USDPerHour: round4(cost)}
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
	usd := l.ClosedUSD
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
}

func (e EksStatus) String() string {
	s := fmt.Sprintf("eks: %d/%d lanes, %d/%d vCPU, $%.2f/h of $%.2f/h, spent ~$%.2f", e.Lanes, e.MaxLanes, e.VCPUs, e.VCPUQuota, e.USDPerHour, e.BudgetPerHr, e.SpendUSD)
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
	return EksStatus{Lanes: len(l.Open), MaxLanes: max, VCPUs: l.vcpus(), VCPUQuota: h.EKS.VCPUQuota, USDPerHour: round4(l.hourly()),
		BudgetPerHr: h.EKS.BudgetPerHour, SpendUSD: l.spend(m.now()), BudgetTotal: h.EKS.BudgetTotal}
}

// eksAdmit says why a cluster of plan may not be made on host now ("" when it may): too many lanes, the vCPU quota, the
// $/h budget or the total budget.
func (m *Manager) eksAdmit(host string, p EksPlan) string {
	h := m.inv.Hosts[host]
	l := m.eksLedger(host)
	st := m.eksStatus(host)
	switch {
	case st.Lanes >= st.MaxLanes:
		return fmt.Sprintf("host %s runs max_lanes (%d) EKS clusters", host, st.MaxLanes)
	case l.vcpus()+p.VCPUs > h.EKS.VCPUQuota:
		return fmt.Sprintf("host %s: a lane of %d vCPUs does not fit the vCPU quota (%d of %d in use)", host, p.VCPUs, l.vcpus(), h.EKS.VCPUQuota)
	case l.hourly()+p.USDPerHour > h.EKS.BudgetPerHour+1e-9:
		return fmt.Sprintf("host %s: a lane at $%.2f/h does not fit the budget ($%.2f/h of $%.2f/h in use)", host, p.USDPerHour, l.hourly(), h.EKS.BudgetPerHour)
	case h.EKS.BudgetTotal > 0 && l.spend(m.now())+p.USDPerHour > h.EKS.BudgetTotal:
		return fmt.Sprintf("host %s: spent ~$%.2f of the total budget $%.2f", host, l.spend(m.now()), h.EKS.BudgetTotal)
	}
	return ""
}

// eksPlanFor is the cluster the lane gets for the experiment it is bound to (the default footprint when it has none).
func (m *Manager) eksPlanFor(host string, s Spec) (EksPlan, error) {
	return m.inv.Hosts[host].EKS.PlanLane(s.LaneLimit())
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
		changed := false
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
				p, err := m.eksPlanFor(name, spec)
				if err != nil {
					p, _ = h.EKS.PlanLane(DefaultLaneLimit())
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

// eksIdle says whether a free lane's cluster has been unused for the host's idle timeout.
func (m *Manager) eksIdle(l *LaneFile) bool {
	h := m.inv.Hosts[l.Host]
	return h.IsEks() && m.now().Sub(l.Since) >= h.EKS.Idle()
}

// eksEnv are the cluster-up variables of a lane: its size, from the workload's footprint, and the account.
func eksEnv(h Host, p EksPlan) []string {
	e := h.EKS
	env := []string{"EKS_REGION=" + e.Region, "EKS_INSTANCE_TYPE=" + p.InstanceType, "EKS_NODES=" + strconv.Itoa(p.Nodes),
		"EKS_NODE_MAX=" + strconv.Itoa(p.Nodes+1)}
	if e.Account != "" {
		env = append(env, "EKS_ACCOUNT="+e.Account)
	}
	if e.VPC != "" {
		env = append(env, "EKS_VPC="+e.VPC, "EKS_SUBNETS="+strings.Join(e.Subnets, ","))
	}
	return env
}

// eksProbeBound bounds one lane health read.
const eksProbeBound = 45 * time.Second

// observeEksLanes reads each existing EKS lane's cluster: the API must answer and a node be Ready. A lane that fails
// gets no new work (the reason is its idle reason, alarm eks-lane-unhealthy once); nothing is deleted. A failed read of
// a lane that is mid-build is no reading (its cluster is being made).
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
	p, err := h.EKS.PlanLane(s.LaneLimit())
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
func eksAccountScript() string {
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
	case e != nil && e.Account != "" && account != e.Account:
		return false, fmt.Sprintf("credentials belong to account %s, the host is configured for %s", account, e.Account)
	}
	return true, fmt.Sprintf("account %s, region %s, up to %d vCPUs and $%.2f/h", account, e.Region, e.VCPUQuota, e.BudgetPerHour)
}

// statusEksHost is `lanes host status` of an eks host: whether the credentials work and what the clusters cost so far.
func statusEksHost(ctx context.Context, root Root, inv Inventory, host string, run runFn, out io.Writer) {
	h := inv.Hosts[host]
	if run != nil {
		o, err := run(ctx, host, eksAccountScript())
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
	st := EksStatus{Lanes: len(l.Open), MaxLanes: max, VCPUs: l.vcpus(), VCPUQuota: h.EKS.VCPUQuota, USDPerHour: round4(l.hourly()),
		BudgetPerHr: h.EKS.BudgetPerHour, SpendUSD: l.spend(time.Now()), BudgetTotal: h.EKS.BudgetTotal}
	fmt.Fprintf(out, "%s: %s\n", host, st)
	for lane, o := range l.Open {
		fmt.Fprintf(out, "%s: lane %s: %s x %d since %s\n", host, lane, o.InstanceType, o.Nodes, o.Since.Format(time.RFC3339))
	}
	fmt.Fprintf(out, "%s: no cpuset, ledger, kernel or PSI gates (provider eks)\n", host)
}
