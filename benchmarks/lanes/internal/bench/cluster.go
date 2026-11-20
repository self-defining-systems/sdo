package bench

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// clusterCheckTimeout bounds one existence probe.
const clusterCheckTimeout = 30 * time.Second

// clusterExists says whether the lane's cluster is there: for a kind host its name is in `kind get clusters` (a
// busy API server does not make a cluster missing); for a virtual cluster, or when kind cannot be run, its API
// answers /readyz.
func (r *stepRunner) clusterExists(ctx context.Context) (bool, error) {
	if r.exists != nil {
		return r.exists(ctx)
	}
	ctx, cancel := context.WithTimeout(ctx, clusterCheckTimeout)
	defer cancel()
	env := append(append(os.Environ(), r.s.HostEnv...), "KUBECONFIG="+r.s.Kubeconfig)
	if r.s.Provider == ProviderEks {
		// AWS knows whether the cluster exists; a kubeconfig that does not match says nothing about it.
		cmd := exec.CommandContext(ctx, "bash", filepath.Join(r.platform(), "deploy", "eks-lane.sh"), "exists", r.s.Lane)
		cmd.Env = env
		err := cmd.Run()
		var ee *exec.ExitError
		switch {
		case err == nil:
			return true, nil
		case errors.As(err, &ee) && ee.ExitCode() == 1:
			return false, nil
		}
		return true, fmt.Errorf("cluster %s: existence not known (inconclusive): %v", r.s.Lane, err)
	}
	if r.s.Provider != ProviderVcluster && r.s.Provider != ProviderEks {
		cmd := exec.CommandContext(ctx, "kind", "get", "clusters")
		cmd.Env = env
		if out, err := cmd.Output(); err == nil {
			for _, name := range strings.Fields(string(out)) {
				if name == r.s.Lane {
					return true, nil
				}
			}
			return false, nil
		}
	}
	cmd := exec.CommandContext(ctx, "kubectl", "--context", "kind-"+r.s.Lane, "--request-timeout=20s", "get", "--raw", "/readyz")
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	switch {
	case err == nil:
		return true, nil
	case strings.Contains(string(out), "does not exist"):
		return false, nil // the kubeconfig has no such context: the cluster was never made, or its entry is gone
	}
	return true, fmt.Errorf("cluster %s: API not reachable (inconclusive): %v", r.s.Lane, err)
}

// clusterMissing is true only when the probe answered that the cluster does not exist: an error (a busy API
// server, a slow docker, no kind on the host) is inconclusive and counts as present, so the caller goes on as before.
func (r *stepRunner) clusterMissing(ctx context.Context) bool {
	ok, err := r.clusterExists(ctx)
	if err != nil {
		r.logf("cluster probe: %v", err)
		return false
	}
	return !ok && ctx.Err() == nil
}

// distrustPrepared checks, before a build or release trusts what the lane file says is installed on it, that the
// lane's cluster exists. When it does not (a cluster-up that died and removed its half-built cluster, a cluster
// deleted by hand) the lane's record is stale: it reports true, and the step forgets it. A host with no
// cluster_up does not manage its clusters (they are whatever the operator of the bench says), so it is not probed.
func (r *stepRunner) distrustPrepared(ctx context.Context) bool {
	s := r.s
	if s.Create || s.ClusterUp == "" || !r.clusterMissing(ctx) {
		return false
	}
	r.logf("cluster %s does not exist: the lane's prepared record is dropped", s.Lane)
	r.res.Prepared = nil
	return true
}

// clusterVanished fails the step with a clear error when the cluster went away while it waited for it.
func (r *stepRunner) clusterVanished(ctx context.Context, waiting string) bool {
	if r.s.ClusterUp == "" || !r.clusterMissing(ctx) {
		return false
	}
	r.down("cluster %s no longer exists while waiting for %s", r.s.Lane, waiting)
	return true
}
