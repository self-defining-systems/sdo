//go:build integration

package bench

import (
	"strings"
	"testing"
)

// A vcluster host whose host cluster is a local kind cluster (eks.local_kind): no AWS account, credentials or tools.

const localKindHost = `hosts:
  lk:
    local: true
    provider: eks
    eks: {region: us-west-2, lanes: vcluster, local_kind: true, host_cluster: lk-host, budget_per_hour: 3.0, vcpu_quota: 80}
    staging: /s
    kubeconfigs: /k
    evidence: /e
    max_builds: 2
    clusters: [vc1, vc2]
`

func TestLocalKindHostNeedsNoAccountAndSaysItsRegistryAndKind(t *testing.T) {
	inv, err := loadInv(t, localKindHost)
	if err != nil {
		t.Fatal(err)
	}
	h := inv.Hosts["lk"]
	if !h.IsEksVirtual() || h.EKS.Registry() != "localhost:5611" {
		t.Fatalf("local kind host: virtual=%v registry=%q", h.IsEksVirtual(), h.EKS.Registry())
	}
	env := strings.Join(h.EnvList(), " ")
	for _, want := range []string{"EKS_LOCAL_KIND=1", "EKS_LOCAL_REGISTRY_PORT=5611", "BENCH_IMAGE_REGISTRY=localhost:5611", "BENCH_LANE_PROVIDER=eks"} {
		if !strings.Contains(env, want) {
			t.Errorf("env %q lacks %s", env, want)
		}
	}
	if got := strings.Join(platformTools(h), " "); strings.Contains(got, "aws") || strings.Contains(got, "eksctl") || !strings.Contains(got, "kind") || !strings.Contains(got, "vcluster") {
		t.Errorf("a local kind host needs kind, helm and vcluster and no AWS tools: %q", got)
	}
	if ok, why := judgeEksAccount("account local-kind\n", nil, h.EKS); !ok {
		t.Errorf("a local kind host has no credentials to check: %s", why)
	}
	if !strings.Contains(eksAccountScript(h.EKS), "local-kind") || strings.Contains(eksAccountScript(h.EKS), "aws") {
		t.Errorf("credentials probe %q", eksAccountScript(h.EKS))
	}
}

func TestLocalKindMisuseIsRefusedAtLoad(t *testing.T) {
	for name, body := range map[string]string{
		"cluster lanes":   strings.Replace(localKindHost, "lanes: vcluster, ", "", 1),
		"port without it": strings.Replace(localKindHost, "local_kind: true", "local_registry_port: 5700, account: '1'", 1),
	} {
		if _, err := loadInv(t, body); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
	inv, err := loadInv(t, strings.Replace(localKindHost, "local_kind: true", "local_kind: true, local_registry_port: 5700", 1))
	if err != nil || inv.Hosts["lk"].EKS.Registry() != "localhost:5700" {
		t.Fatalf("custom port: %v", err)
	}
}
