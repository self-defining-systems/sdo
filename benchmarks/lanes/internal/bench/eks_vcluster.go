package bench

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Provider eks with `lanes: vcluster`: a lane is a virtual cluster on the host's one long-lived EKS cluster, not a cluster
// of its own. The host cluster is made once (`lanes host adopt HOST`); its control plane stays up and its system node
// group (Karpenter, CoreDNS, the node agent and the lanes' control planes) is scaled to zero when no lane exists. A lane
// is made in seconds (deploy/eks-vcluster.sh): a vcluster in its own namespace and a Karpenter NodePool of its own that
// gives it dedicated nodes, sized like a cluster lane's, released when the lane goes. The node agent and the ECR mirror
// run once on the host cluster and serve every lane.
//
// A lane on a vcluster has no real kube-system and no node access (the nodes and kube-system namespace are the host's),
// so it is labeled `kube-system: virtual, node-access: virtual`; a full-cluster lane `kube-system: real, node-access:
// real`. Both are `shared: "true"`, so a spec without a selector takes either; a spec whose items need the real ones sets
// `selector: {node-access: real}` (the fault matrices) and is refused at add time when no lane could take it.

// Values of eks.lanes.
const (
	EksLanesCluster  = "cluster"
	EksLanesVcluster = "vcluster"
)

// Capability labels of an eks lane (generic lane labels, labels.go).
const (
	LabelKubeSystem = "kube-system"
	LabelNodeAccess = "node-access"
	CapReal         = "real"
	CapVirtual      = "virtual"
)

// DefaultVirtualIdleTimeout is how long a free virtual lane is kept: it is remade in seconds, and its nodes bill.
const DefaultVirtualIdleTimeout = 3 * time.Minute

// Default cluster commands of an eks host whose lanes are vclusters.
const (
	eksVclusterUp   = "$LANES_INSTALL/deploy/eks-vcluster.sh up {cluster} {kubeconfig} {evidence}"
	eksVclusterDown = "$LANES_INSTALL/deploy/eks-vcluster.sh down {cluster} {kubeconfig}"
)

func (e *HostEKS) virtual() bool { return e != nil && e.Lanes == EksLanesVcluster }

// IsEksVirtual reports whether the host's lanes are vclusters on one EKS host cluster.
func (h Host) IsEksVirtual() bool { return h.IsEks() && h.EKS.virtual() }

func (e *HostEKS) validateLanes() []string {
	switch e.Lanes {
	case "", EksLanesCluster, EksLanesVcluster:
	default:
		return []string{fmt.Sprintf("eks.lanes %q: want %s or %s", e.Lanes, EksLanesCluster, EksLanesVcluster)}
	}
	if len(e.FullLanes) > 0 && !e.virtual() {
		return []string{"eks.full_lanes names the full-cluster lanes of a host with lanes: vcluster"}
	}
	return nil
}

// hostCluster is the name of the host cluster of host (default lanes-host-<host>).
func (e *HostEKS) hostCluster(host string) string {
	if e.HostCluster != "" {
		return e.HostCluster
	}
	return "lanes-host-" + host
}

// hostKubeconfig is where the script keeps the host cluster's admin kubeconfig.
func (h Host) hostKubeconfig() string { return filepath.Join(h.Kubeconfigs, ".host", "admin") }

// systemNode is the system node of the host cluster: its vCPUs and $/h.
func (e *HostEKS) systemNode() (vcpus int, usd float64) {
	inst := e.SystemInstance
	if inst == "" {
		inst = "m6i.xlarge"
	}
	fam, size, _ := strings.Cut(inst, ".")
	for _, sz := range eksSizes {
		if sz.Name == size {
			if f, ok := eksFamilies[fam]; ok {
				return sz.VCPUs, round4(float64(sz.VCPUs) * f.USDPerVCPUHour)
			}
		}
	}
	return 4, 0.192
}

// virtualOpen is how many vcluster lanes exist: the host's system node runs while there is one.
func (l *eksLedger) virtualOpen() (n int) {
	for _, o := range l.Open {
		if o.Virtual {
			n++
		}
	}
	return n
}

// hostHourly is the host cluster's running cost: its control plane once adopted, and its system node while vcluster lanes
// exist (or, adding, once one more does).
func (l *eksLedger) hostHourly(e *HostEKS, adding bool) float64 {
	if !e.virtual() || l.HostSince.IsZero() {
		return 0
	}
	usd := eksControlPlaneUSDPerHour
	if adding || l.virtualOpen() > 0 {
		_, node := e.systemNode()
		usd += node
	}
	return usd
}

// hostVCPUs are the vCPUs the host cluster's system node holds against the quota while vcluster lanes exist.
func (l *eksLedger) hostVCPUs(e *HostEKS, adding bool) int {
	if !e.virtual() || (!adding && l.virtualOpen() == 0) {
		return 0
	}
	n, _ := e.systemNode()
	return n
}

// virtualLane says whether lane of the host is a vcluster (all of them, but the host's full_lanes).
func (e *HostEKS) virtualLane(lane string) bool {
	if !e.virtual() {
		return false
	}
	for _, f := range e.FullLanes {
		if f == lane {
			return false
		}
	}
	return true
}

func (r Root) eksHostRecordPath(host string) string { return r.path("eks", host+".host.json") }

// eksHostRecord is <root>/eks/<host>.host.json, written by `lanes host adopt`: the host cluster and when it began to bill.
type eksHostRecord struct {
	Cluster string    `json:"cluster"`
	Since   time.Time `json:"since"`
}

// eksHostSettle reads the host record into the ledger and adds what the system node cost since the last pass while
// lanes existed. It reports whether the ledger changed.
func (m *Manager) eksHostSettle(host string, h Host, l *eksLedger) bool {
	if !h.EKS.virtual() {
		return false
	}
	changed := false
	if l.HostSince.IsZero() {
		var rec eksHostRecord
		if readJSON(m.Root.eksHostRecordPath(host), &rec) == nil && !rec.Since.IsZero() {
			l.HostSince, changed = rec.Since, true
		}
	}
	now := m.now().UTC()
	if l.virtualOpen() > 0 && !l.Settled.IsZero() {
		_, node := h.EKS.systemNode()
		l.SystemUSD = round4(l.SystemUSD + node*now.Sub(l.Settled).Hours())
		changed = true
	}
	l.Settled = now
	return changed
}

// eksLabels are the lane labels an eks lane gets when hosts.yaml gives it none.
func eksLabels(e *HostEKS, lane string) map[string]string {
	c := CapReal
	if e.virtualLane(lane) {
		c = CapVirtual
	}
	return map[string]string{SharedLabel: "true", LabelKubeSystem: c, LabelNodeAccess: c}
}

// SelectorProblems say, for each host of s, why none of its lanes can take a spec whose selector no lane satisfies. A
// spec nothing could take would wait forever, so it is refused at add time with the labels every lane has. A host that has
// a lane which accepts the spec clears the spec.
func (inv Inventory) SelectorProblems(s Spec) []string {
	if len(s.Selector) == 0 {
		return nil
	}
	var out []string
	for _, h := range inv.UsableHosts(s) {
		host := inv.Hosts[h]
		var have []string
		for _, c := range host.Clusters {
			if inv.LaneAccepts(h, c, s) {
				return nil
			}
			have = append(have, "["+labelsText(host.LaneLabels[c])+"]")
		}
		why := fmt.Sprintf("host %s: no lane has labels satisfying the selector [%s] (lane labels %s)", h, labelsText(s.Selector), strings.Join(uniq(have), ", "))
		if host.IsEksVirtual() {
			why += "; its lanes are vclusters with no real kube-system and no node access: use a full-cluster eks host (lanes: cluster) for it"
		}
		out = append(out, why)
	}
	return out
}

func uniq(in []string) []string {
	sort.Strings(in)
	var out []string
	for i, s := range in {
		if i == 0 || s != in[i-1] {
			out = append(out, s)
		}
	}
	return out
}

// adoptEksHost is `lanes host adopt HOST` for an eks host with vcluster lanes: it makes the host cluster once (or attaches
// the one that exists, scaling its system nodes up) and records when it began to bill.
func adoptEksHost(ctx context.Context, root Root, inv Inventory, host string, run runFn, out io.Writer) error {
	h := inv.Hosts[host]
	if !h.IsEksVirtual() {
		fmt.Fprintf(out, "%s: eks host with cluster lanes: nothing to adopt (each lane is a cluster of its own)\n", host)
		return nil
	}
	dir, err := InstallPlatformScripts(h.Staging)
	if err != nil {
		return err
	}
	env := append(append(h.EnvList(), eksAccountEnv(h.EKS)...), "LANES_INSTALL="+dir)
	var pre strings.Builder
	for _, kv := range env {
		k, v, _ := strings.Cut(kv, "=")
		fmt.Fprintf(&pre, "export %s=%s; ", k, shellQuote(v))
	}
	t0 := time.Now()
	o, err := run(ctx, host, pre.String()+`bash "$LANES_INSTALL/deploy/eks-vcluster.sh" host-up`)
	fmt.Fprint(out, o)
	if err != nil {
		return fmt.Errorf("host %s: host cluster %s: %w", host, h.EKS.hostCluster(host), err)
	}
	rec := eksHostRecord{Cluster: h.EKS.hostCluster(host), Since: time.Now().UTC()}
	var old eksHostRecord
	if readJSON(root.eksHostRecordPath(host), &old) == nil && !old.Since.IsZero() && old.Cluster == rec.Cluster {
		rec.Since = old.Since // adopting again keeps the date the host began to bill
	}
	if err := os.MkdirAll(filepath.Dir(root.eksHostRecordPath(host)), 0o755); err != nil {
		return err
	}
	if err := writeJSON(root.eksHostRecordPath(host), rec); err != nil {
		return err
	}
	fmt.Fprintf(out, "%s: host cluster %s adopted in %s; lanes are vclusters on it (kube-system and node access are virtual)\n", host, rec.Cluster, time.Since(t0).Round(time.Second))
	return nil
}
