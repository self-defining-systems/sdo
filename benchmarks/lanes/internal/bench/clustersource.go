package bench

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

// A kind host with no cluster_up never makes a cluster (the clusters are adopted): each of its lanes needs its cluster to
// exist already, shown by its kubeconfig in the host's kubeconfigs directory. Without one the first step on that lane
// fails much later, at recover, with "context kind-<lane> does not exist". These checks say it up front, at add and in
// host status, and tell what to set. Nothing changes for a host that has cluster_up (it makes them) or a vcluster host.

const clusterSourceTimeout = 30 * time.Second

// hostClusterProblems says, for the lanes of host that accepts selects, which have no cluster. run asks the host;
// a host that cannot be asked says nothing (reachability has its own alarm).
func (inv Inventory) hostClusterProblems(ctx context.Context, host string, accepts func(lane string) bool, run runFn) []string {
	h := inv.Hosts[host]
	if h.ClusterUp != "" || h.IsVcluster() || len(h.Clusters) == 0 {
		return nil
	}
	var out []string
	for _, lane := range h.Clusters {
		if !accepts(lane) {
			continue
		}
		kc := filepath.Join(h.Kubeconfigs, lane)
		c, cancel := context.WithTimeout(ctx, clusterSourceTimeout)
		ans, err := run(c, host, "if test -s "+shellQuote(kc)+"; then echo present; else echo missing; fi")
		cancel()
		if err != nil || !strings.Contains(ans, "missing") {
			continue
		}
		out = append(out, fmt.Sprintf("host %s has no cluster_up and lane %s has no cluster: set cluster_up (e.g. $LANES_INSTALL/deploy/kind-lane.sh up)", host, lane))
	}
	return out
}

// ClusterSourceProblems are the lanes of the spec's usable hosts that the spec may take and that have no cluster on a
// host with no cluster_up.
func (inv Inventory) ClusterSourceProblems(ctx context.Context, s Spec, run runFn) []string {
	var out []string
	for _, host := range inv.UsableHosts(s) {
		out = append(out, inv.hostClusterProblems(ctx, host, func(lane string) bool { return inv.LaneAccepts(host, lane, s) }, run)...)
	}
	return out
}

// HostClusterProblems are the same for host status: every lane of the host.
func (inv Inventory) HostClusterProblems(ctx context.Context, host string, run runFn) []string {
	return inv.hostClusterProblems(ctx, host, func(string) bool { return true }, run)
}
