//go:build integration

package deploy_test

// kind-lane.sh checks the host kernel before it makes a cluster (and before the registry or any preload staging): kube-proxy needs
// the netfilter mangle table and comment match, and a kernel without them (a custom build) otherwise shows only as a 15-minute helm
// timeout with kindnet and kube-proxy crash-looping inside the nodes. Only a netfilter rejection by the probe is a verdict on the
// kernel and it names the option that was rejected; any other probe failure is inconclusive and the lane goes on.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func (e *env) laneUp(kv ...string) (string, error) {
	return e.run(e.with(append([]string{"KIND_NODE_PRELOAD=", "KIND_NODE_IMAGE=" + base}, kv...)...), "kind-lane.sh", "up", "lane",
		filepath.Join(e.dir, "kc"), filepath.Join(e.dir, "ev"))
}

func (e *env) clusterCreated() bool {
	_, err := os.Stat(filepath.Join(e.dir, "kind-config"))
	return err == nil
}

func TestKindLaneRefusesAKernelThatLacksOneKubeProxyNetfilterOption(t *testing.T) {
	for name, tc := range map[string]struct {
		env             string
		want, notBlamed []string
	}{
		// the verdict line (before the probe's own output) names the option that was rejected and no other
		"comment missing": {"STUB_NO_COMMENT=1", []string{"the comment match", "CONFIG_NETFILTER_XT_MATCH_COMMENT", "xt_comment"}, []string{"mangle", "MANGLE"}},
		"mangle missing":  {"STUB_NO_MANGLE=1", []string{"the mangle table", "CONFIG_IP_NF_MANGLE", "iptable_mangle"}, []string{"comment", "COMMENT"}},
	} {
		t.Run(name, func(t *testing.T) {
			e := setup(t)
			out, err := e.laneUp(tc.env)
			if err == nil {
				t.Fatalf("a kernel that rejects the option must be refused: %s", out)
			}
			verdict := strings.Split(out, "probe output:")[0]
			if !strings.Contains(out, "KIND_SKIP_KERNEL_CHECK=1") {
				t.Errorf("the refusal should say how to skip: %s", out)
			}
			for _, w := range tc.want {
				if !strings.Contains(verdict, w) {
					t.Errorf("the refusal should name %q: %s", w, out)
				}
			}
			for _, w := range append(tc.notBlamed, "br_netfilter", "inconclusive") {
				if strings.Contains(verdict, w) {
					t.Errorf("the refusal must not mention %q (not probed or not the cause): %s", w, out)
				}
			}
			if e.clusterCreated() {
				t.Error("no cluster may be created on a kernel that cannot run kube-proxy")
			}
			// before the registry and any preload staging
			if log := e.log(); strings.Contains(log, "registry") {
				t.Errorf("the refusal must come before the registry is touched: %s", log)
			}
		})
	}
}

func TestKindLaneKernelCheckPassesOrIsSkipped(t *testing.T) {
	for name, kv := range map[string][]string{
		"good kernel":   {},
		"skipped":       {"STUB_NO_MANGLE=1", "KIND_SKIP_KERNEL_CHECK=1"},
		"default image": {"KIND_NODE_IMAGE="},
	} {
		t.Run(name, func(t *testing.T) {
			e := setup(t)
			out, err := e.laneUp(kv...)
			if err != nil {
				t.Fatalf("%v: %s", err, out)
			}
			if !e.clusterCreated() {
				t.Fatal("the cluster was not created")
			}
			if strings.Contains(out, "inconclusive") {
				t.Errorf("a passing or skipped check says nothing: %s", out)
			}
		})
	}
}

// With KIND_NODE_IMAGE unset the probe still runs, with kind's default node image (here a local kindest/node: the stub kind binary
// embeds none), and still refuses.
func TestKindLaneKernelCheckRunsWithTheDefaultNodeImage(t *testing.T) {
	e := setup(t)
	out, err := e.laneUp("KIND_NODE_IMAGE=", "STUB_NO_MANGLE=1")
	if err == nil || !strings.Contains(out, "mangle table") {
		t.Fatalf("a bad kernel must be refused with the default node image too: %v: %s", err, out)
	}
	if !strings.Contains(e.log(), "docker run --rm --privileged --name fzkernelcheck-") || !strings.Contains(e.log(), base) {
		t.Fatalf("the probe should have run with the local kindest/node image: %s", e.log())
	}
}

func TestKindLaneKernelCheckIsInconclusiveWhenDockerFailsOrHangs(t *testing.T) {
	for name, tc := range map[string]struct {
		env  []string
		want string
	}{
		"docker error (rc 125)": {[]string{"STUB_DOCKER_FAIL=125"}, "stub failure"},
		"timeout":               {[]string{"STUB_DOCKER_HANG=1", "KIND_KERNEL_CHECK_TIMEOUT=1"}, "timed out after 1s"},
	} {
		t.Run(name, func(t *testing.T) {
			e := setup(t)
			start := time.Now()
			out, err := e.laneUp(tc.env...)
			if err != nil {
				t.Fatalf("an inconclusive probe must not stop the lane: %v: %s", err, out)
			}
			if !e.clusterCreated() {
				t.Fatal("the lane should go on to create the cluster")
			}
			if !strings.Contains(out, "kernel check inconclusive") || !strings.Contains(out, tc.want) {
				t.Errorf("want an inconclusive warning containing %q: %s", tc.want, out)
			}
			for _, claim := range []string{"cannot run kube-proxy", "CONFIG_"} {
				if strings.Contains(out, claim) {
					t.Errorf("a probe that proved nothing must not blame the kernel (%q): %s", claim, out)
				}
			}
			if time.Since(start) > 20*time.Second {
				t.Errorf("the probe was not bounded: %v", time.Since(start))
			}
		})
	}
}

// An existing cluster is not probed.
func TestKindLaneDoesNotProbeAnExistingCluster(t *testing.T) {
	e := setup(t)
	os.WriteFile(filepath.Join(e.dir, "bin", "kind"), []byte("#!/bin/sh\ncase \"$1 $2\" in \"get clusters\") echo lane ;; \"get nodes\") echo lane-control-plane ;; \"get kubeconfig\") : ;; esac\n"), 0o755)
	if out, err := e.laneUp("STUB_NO_MANGLE=1"); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if strings.Contains(e.log(), "fzkernelcheck") {
		t.Errorf("an existing cluster must not be probed: %s", e.log())
	}
}
