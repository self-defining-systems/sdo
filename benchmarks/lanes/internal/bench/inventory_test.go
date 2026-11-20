//go:build integration

package bench

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func loadInv(t *testing.T, body string) (Inventory, error) {
	t.Helper()
	d := t.TempDir()
	if err := os.WriteFile(filepath.Join(d, "hosts.yaml"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return LoadInventory(d)
}

const vcHost = `hosts:
  h:
    local: true
    provider: vcluster
    entry_host: 192.168.1.108
    host_kubeconfig: /etc/shared/kubeconfig.yaml
    staging: /s
    kubeconfigs: /k
    evidence: /e
    max_builds: 1
    max_load5: 10
    clusters: [vc1]
`

func TestVclusterHostDefaultsItsClusterCommandsAndEnv(t *testing.T) {
	inv, err := loadInv(t, vcHost)
	if err != nil {
		t.Fatal(err)
	}
	h := inv.Hosts["h"]
	if !h.IsVcluster() || !strings.HasPrefix(h.ClusterCommand(h.ClusterUp, "vc1"), "$LANES_INSTALL/deploy/vcluster-lane.sh up 'vc1' '/k/vc1'") ||
		!strings.HasPrefix(h.ClusterCommand(h.ClusterDown, "vc1"), "$LANES_INSTALL/deploy/vcluster-lane.sh down 'vc1'") {
		t.Fatalf("cluster commands %q %q", h.ClusterUp, h.ClusterDown)
	}
	env := strings.Join(h.EnvList(), " ")
	for _, want := range []string{"FUZZOPS_LANE_PROVIDER=vcluster", "BENCH_LANE_PROVIDER=vcluster", "BENCH_EVIDENCE_MODE=pull",
		"FUZZOPS_ENTRY_HOST=192.168.1.108", "VCLUSTER_HOST_KUBECONFIG=/etc/shared/kubeconfig.yaml"} {
		if !strings.Contains(env, want) {
			t.Errorf("env %q lacks %s", env, want)
		}
	}
}

func TestKindHostEnvStaysAsWritten(t *testing.T) {
	h := Host{Env: map[string]string{"A": "1"}}
	if got := strings.Join(h.EnvList(), " "); got != "A=1" {
		t.Fatalf("env = %q", got)
	}
}

const workloadEnvHost = `hosts:
  h:
    local: true
    staging: /s
    kubeconfigs: /k
    evidence: /e
    max_builds: 1
    max_load5: 10
    clusters: [c1]
    env: {A: host, B: host}
    workload_env:
      wl-one: {B: one, C: one}
      wl-two: {D: two}
`

// A host's workload_env reaches only the steps of that workload, over the host env; a host without the section
// (every existing hosts.yaml) is unchanged.
func TestWorkloadEnvIsMergedOnlyIntoStepsOfThatWorkload(t *testing.T) {
	inv, err := loadInv(t, workloadEnvHost)
	if err != nil {
		t.Fatal(err)
	}
	h := inv.Hosts["h"]
	for workload, want := range map[string]string{
		"wl-one":  "A=host B=one C=one",
		"wl-two":  "A=host B=host D=two",
		"other":   "A=host B=host",
		"":        "A=host B=host",
		"default": "A=host B=host",
	} {
		if got := strings.Join(h.EnvFor(workload), " "); got != want {
			t.Errorf("EnvFor(%q) = %q, want %q", workload, got, want)
		}
	}
	if got := strings.Join(h.EnvList(), " "); got != "A=host B=host" {
		t.Errorf("EnvList (host work: ledger, preflight) = %q, must not carry a workload's env", got)
	}
	plain := Host{Env: map[string]string{"A": "1"}}
	if got := strings.Join(plain.EnvFor("wl-one"), " "); got != "A=1" {
		t.Errorf("host without workload_env: %q", got)
	}
}

func TestVclusterHostValidation(t *testing.T) {
	for name, tc := range map[string]struct{ from, to, want string }{
		"no entry host":   {"    entry_host: 192.168.1.108\n", "", "needs entry_host"},
		"no kubeconfig":   {"    host_kubeconfig: /etc/shared/kubeconfig.yaml\n", "", "needs host_kubeconfig"},
		"unknown":         {"provider: vcluster", "provider: nomad", "want kind, vcluster or eks"},
		"kind with entry": {"provider: vcluster", "provider: kind", "belong to provider vcluster"},
	} {
		_, err := loadInv(t, strings.Replace(vcHost, tc.from, tc.to, 1))
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want %q", name, err, tc.want)
		}
	}
}

func TestMaxBuildFailuresDefaultsAndValidation(t *testing.T) {
	inv, err := loadInv(t, vcHost)
	if err != nil {
		t.Fatal(err)
	}
	if got := inv.Hosts["h"].BuildFailureLimit(); got != 2 {
		t.Fatalf("default limit %d", got)
	}
	inv, err = loadInv(t, strings.Replace(vcHost, "    max_builds: 1\n", "    max_builds: 1\n    max_build_failures: 4\n", 1))
	if err != nil || inv.Hosts["h"].BuildFailureLimit() != 4 {
		t.Fatalf("max_build_failures 4: %v %+v", err, inv.Hosts["h"])
	}
	if _, err := loadInv(t, strings.Replace(vcHost, "    max_builds: 1\n", "    max_builds: 1\n    max_build_failures: -1\n", 1)); err == nil || !strings.Contains(err.Error(), "max_build_failures") {
		t.Fatalf("negative max_build_failures: %v", err)
	}
}
