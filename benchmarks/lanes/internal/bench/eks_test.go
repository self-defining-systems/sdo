//go:build integration

package bench

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const eksHost = `hosts:
  e:
    local: true
    provider: eks
    eks: {region: us-west-2, account: "123456789012", budget_per_hour: 2.0, budget_total: 10, vcpu_quota: 24, max_lanes: 3, idle_timeout: 5m}
    staging: /s
    kubeconfigs: /k
    evidence: /e
    max_builds: 2
    clusters: [eks1, eks2, eks3]
`

func TestEksHostDefaultsItsClusterCommandsAndEnv(t *testing.T) {
	inv, err := loadInv(t, eksHost)
	if err != nil {
		t.Fatal(err)
	}
	h := inv.Hosts["e"]
	if !h.IsEks() || !strings.HasPrefix(h.ClusterCommand(h.ClusterUp, "eks1"), "$LANES_INSTALL/deploy/eks-lane.sh up 'eks1' '/k/eks1' '/e/eks1'") ||
		!strings.HasPrefix(h.ClusterCommand(h.ClusterDown, "eks1"), "$LANES_INSTALL/deploy/eks-lane.sh down 'eks1'") {
		t.Fatalf("cluster commands %q %q", h.ClusterUp, h.ClusterDown)
	}
	env := strings.Join(h.EnvList(), " ")
	for _, want := range []string{"BENCH_LANE_PROVIDER=eks", "BENCH_EVIDENCE_MODE=pull", "EKS_REGION=us-west-2", "AWS_REGION=us-west-2",
		"BENCH_IMAGE_REGISTRY=123456789012.dkr.ecr.us-west-2.amazonaws.com"} {
		if !strings.Contains(env, want) {
			t.Errorf("env %q lacks %s", env, want)
		}
	}
}

// The host-safety gates are skipped for an eks host by design, so a setting for one is refused, not ignored.
func TestEksHostRefusesTheHostSafetySettings(t *testing.T) {
	for name, tc := range map[string]struct{ old, new, want string }{
		"reserve":  {"    clusters:", "    reserve: {cpus: \"0-3\"}\n    clusters:", "provider eks has no host to reserve"},
		"lanes":    {"    clusters:", "    lanes: {cpus: \"0-3\", memory: 4Gi}\n    clusters:", "no core or memory ledger"},
		"kernel":   {"    clusters:", "    kernel_minimums: {vm.max_map_count: 1}\n    clusters:", "no kernel"},
		"ssh":      {"    local: true", "    ssh: somewhere", "set local: true"},
		"no eks":   {"    eks: {region: us-west-2, account: \"123456789012\", budget_per_hour: 2.0, budget_total: 10, vcpu_quota: 24, max_lanes: 3, idle_timeout: 5m}\n", "", "needs an eks: block"},
		"no acct":  {`account: "123456789012", `, "", "eks.account is required"},
		"no quota": {"vcpu_quota: 24", "vcpu_quota: 0", "vcpu_quota must be positive"},
		"family":   {"max_lanes: 3", "max_lanes: 3, instance_families: [t2]", "unknown family"},
		"lanes>cl": {"max_lanes: 3", "max_lanes: 9", "lists only 3 lane names"},
	} {
		body := strings.Replace(eksHost, tc.old, tc.new, 1)
		if tc.old == "    local: true" {
			body = strings.Replace(body, "    local: true\n", tc.new+"\n", 1)
		}
		_, err := loadInv(t, body)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want %q", name, err, tc.want)
		}
	}
	if _, err := loadInv(t, strings.Replace(vcHost, "provider: vcluster", "provider: kind\n    eks: {region: r}", 1)); err == nil || !strings.Contains(err.Error(), "eks belongs to provider eks") {
		t.Errorf("a kind host with eks: err = %v", err)
	}
}

func TestEksPlanSizesTheLaneFromItsFootprint(t *testing.T) {
	e := &HostEKS{Region: "r", BudgetPerHour: 9, VCPUQuota: 99}
	for _, tc := range []struct {
		cpus  int
		mem   string
		want  string
		nodes int
		usd   float64
	}{
		{5, "12Gi", "m6i.xlarge", 2, 0.4840},   // the default footprint: two 4 vCPU nodes
		{8, "30Gi", "m6i.xlarge", 2, 0.4840},   // half of it fits 4 vCPUs and 16 GiB
		{16, "64Gi", "m6i.2xlarge", 2, 0.8680}, // 8 vCPUs, 32 GiB per node
		{2, "4Gi", "m6i.xlarge", 1, 0.2920},    // below 4 cores: one node, never under min_node_vcpus
		{8, "100Gi", "m6i.4xlarge", 2, 1.6360}, // memory decides: 50 GiB per node needs 16 vCPUs
		{30, "30Gi", "m6i.4xlarge", 2, 1.6360}, // 15 cores per node
	} {
		p, err := e.PlanLane(LaneLimit{CPUs: tc.cpus, Memory: tc.mem})
		if err != nil || p.InstanceType != tc.want || p.Nodes != tc.nodes || p.USDPerHour != tc.usd {
			t.Errorf("%d cores %s: %+v (%v), want %s x %d at $%.4f/h", tc.cpus, tc.mem, p, err, tc.want, tc.nodes, tc.usd)
		}
	}
	// the cheapest family that fits wins: compute-optimised for a CPU-bound footprint, memory-optimised for a big one
	e.InstanceFamilies = []string{"m6i", "c6i", "r6i"}
	if p, _ := e.PlanLane(LaneLimit{CPUs: 8, Memory: "8Gi"}); p.InstanceType != "c6i.xlarge" {
		t.Errorf("cpu-bound plan = %+v, want c6i.xlarge", p)
	}
	if p, _ := e.PlanLane(LaneLimit{CPUs: 8, Memory: "120Gi"}); p.InstanceType != "r6i.2xlarge" {
		t.Errorf("memory-bound plan = %+v, want r6i.2xlarge", p)
	}
	if _, err := e.PlanLane(LaneLimit{CPUs: 8, Memory: "2Ti"}); err == nil {
		t.Error("a footprint no instance holds was planned")
	}
}

func TestJudgeNodesReady(t *testing.T) {
	if err := judgeNodesReady("a=True\nb=False\n"); err != nil {
		t.Errorf("one Ready node is healthy: %v", err)
	}
	if err := judgeNodesReady("a=False\nb=Unknown\n"); err == nil || !strings.Contains(err.Error(), "no Ready node") {
		t.Errorf("no Ready node: %v", err)
	}
	if err := judgeNodesReady(""); err == nil {
		t.Error("an empty node list is not healthy")
	}
}

func eksManager(t *testing.T) *Manager {
	t.Helper()
	root := Root(t.TempDir())
	must(t, os.WriteFile(root.path("hosts.yaml"), []byte(eksHost+"checkouts:\n  fuzzops: {abc: {e: /fz}}\n"), 0o644))
	now := time.Date(2026, 11, 20, 8, 0, 0, 0, time.UTC)
	m := &Manager{Root: root, Now: func() time.Time { return now }}
	must(t, m.loadInventory())
	m.lanes = map[string]*LaneFile{}
	for _, c := range []string{"eks1", "eks2", "eks3"} {
		m.lanes[c] = &LaneFile{Lane: c, Host: "e", State: LaneFree, Gone: true, Since: now}
	}
	return m
}

func (m *Manager) advance(d time.Duration) {
	now := m.now().Add(d)
	m.Now = func() time.Time { return now }
}

// Admission: a new cluster is held by max_lanes, the vCPU quota, the $/h budget and the total budget, in that order,
// and the reason names the limit; a lane that already has its cluster is never held.
func TestEksAdmissionHoldsNewClustersByLanesQuotaAndBudget(t *testing.T) {
	m := eksManager(t)
	h := m.inv.Hosts["e"]
	small, _ := h.EKS.PlanLane(LaneLimit{CPUs: 5, Memory: "12Gi"}) // 8 vCPUs, $0.484/h
	big, _ := h.EKS.PlanLane(LaneLimit{CPUs: 16, Memory: "64Gi"})  // 16 vCPUs, $0.868/h
	l := m.eksLedger("e")
	if why := m.eksAdmit("e", small); why != "" {
		t.Fatalf("an empty host holds a lane: %s", why)
	}
	l.Open["eks1"] = eksOpen{Since: m.now(), EksPlan: small}
	l.Open["eks2"] = eksOpen{Since: m.now(), EksPlan: small}
	if why := m.eksAdmit("e", big); !strings.Contains(why, "vCPU quota (16 of 24") {
		t.Errorf("16 + 16 vCPUs over a quota of 24: %q", why)
	}
	l.Open["eks3"] = eksOpen{Since: m.now(), EksPlan: small}
	if why := m.eksAdmit("e", small); !strings.Contains(why, "max_lanes (3)") {
		t.Errorf("a fourth lane: %q", why)
	}
	delete(l.Open, "eks3")
	m.inv.Hosts["e"].EKS.BudgetPerHour = 1.0 // two lanes use $0.968/h
	if why := m.eksAdmit("e", small); !strings.Contains(why, "budget ($0.97/h of $1.00/h in use)") {
		t.Errorf("$/h budget: %q", why)
	}
	m.inv.Hosts["e"].EKS.BudgetPerHour = 2.0
	m.advance(20 * time.Hour) // $0.968/h for 20 h is ~$19.4, over the total of $10
	if why := m.eksAdmit("e", small); !strings.Contains(why, "of the total budget $10.00") {
		t.Errorf("total budget: %q", why)
	}
	if !m.eksLaneAdmitted(&LaneFile{Lane: "eks1", Host: "e"}, Spec{}) {
		t.Error("a lane that has its cluster was held")
	}
	if m.eksLaneAdmitted(&LaneFile{Lane: "eks3", Host: "e", Gone: true}, Spec{}) {
		t.Error("a new cluster over the total budget was admitted")
	}
}

// The ledger follows the lanes: a lane with a cluster, or one being made, is open from the pass that sees it; when the
// cluster is gone its cost is recorded, and the status line shows the running estimate.
func TestEksLedgerRecordsTheCostOfAClusterWhenItIsDeleted(t *testing.T) {
	m := eksManager(t)
	m.lanes["eks1"].Gone = false
	m.eksSettle()
	st := m.eksStatus("e")
	if st.Lanes != 1 || st.VCPUs != 8 || st.USDPerHour != 0.484 {
		t.Fatalf("status = %+v", st)
	}
	m.advance(2 * time.Hour)
	if got := m.eksStatus("e").SpendUSD; got != 0.968 {
		t.Fatalf("two hours of a $0.484/h lane = $%.4f", got)
	}
	m.lanes["eks1"].Gone = true
	m.eksSettle()
	var saved eksLedger
	must(t, readJSON(m.Root.eksLedgerPath("e"), &saved))
	if len(saved.Open) != 0 || saved.ClosedUSD != 0.968 || len(saved.Closed) != 1 || saved.Closed[0].Lane != "eks1" || saved.Closed[0].InstanceType != "m6i.xlarge" {
		t.Fatalf("ledger = %+v", saved)
	}
	if s := m.eksStatus("e").String(); !strings.Contains(s, "0/3 lanes") || !strings.Contains(s, "spent ~$0.97 of $10.00") {
		t.Errorf("status line %q", s)
	}
}

func TestEksKeepsAFreeLaneUntilItsIdleTimeout(t *testing.T) {
	m := eksManager(t)
	l := &LaneFile{Lane: "eks1", Host: "e", Since: m.now()}
	if m.eksIdle(l) {
		t.Fatal("a lane freed just now is idle")
	}
	m.advance(5 * time.Minute)
	if !m.eksIdle(l) {
		t.Fatal("the idle timeout (5m) passed")
	}
}

// --- the platform script, against fake aws, eksctl, kubectl and docker ---

type eksScript struct {
	t     *testing.T
	state string
	env   []string
}

func newEksScript(t *testing.T) *eksScript {
	t.Helper()
	s := &eksScript{t: t, state: t.TempDir()}
	s.env = append(os.Environ(), "PATH="+testdata("fakeeks")+string(os.PathListSeparator)+os.Getenv("PATH"), "FAKE_STATE="+s.state,
		"EKS_REGION=us-west-2", "EKS_INSTANCE_TYPE=m6i.xlarge", "EKS_NODES=2", "EKS_NODEPORT_CIDR=203.0.113.7/32")
	return s
}

func (s *eksScript) run(extra []string, args ...string) (string, error) {
	cmd := exec.Command("bash", append([]string{filepath.Join(pkgDir, "..", "..", "deploy", "eks-lane.sh")}, args...)...)
	cmd.Env = append(append([]string(nil), s.env...), extra...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func (s *eksScript) log() string {
	b, _ := os.ReadFile(filepath.Join(s.state, "eks.log"))
	return string(b)
}

func (s *eksScript) file(name string) string {
	b, _ := os.ReadFile(filepath.Join(s.state, name))
	return string(b)
}

func TestEksLaneUpMakesWhatAKindLaneHas(t *testing.T) {
	s := newEksScript(t)
	kc := filepath.Join(t.TempDir(), "kube", "eks1")
	ev := filepath.Join(t.TempDir(), "ev")
	out, err := s.run(nil, "up", "eks1", kc, ev)
	if err != nil || !strings.Contains(out, "EKS_LANE_READY cluster=sdo-lane-eks1 instance=m6i.xlarge nodes=2") {
		t.Fatalf("up: %v\n%s", err, out)
	}
	// the lane makes only a control plane and a node group on the host's shared network and roles: no eksctl, no per-lane VPC
	if strings.Contains(s.log(), "eksctl create") {
		t.Errorf("the lane was made with eksctl:\n%s", s.log())
	}
	cl, ng, addons := s.file("clargs-sdo-lane-eks1"), s.file("ng-sdo-lane-eks1"), s.file("addons")
	for _, want := range []string{"--role-arn arn:aws:iam::123456789012:role/shared-cluster", "subnetIds=subnet-0a,subnet-0b", "supportType=STANDARD", "authenticationMode=API_AND_CONFIG_MAP", "sdo-lane=sdo-lane-eks1"} {
		if !strings.Contains(cl, want) {
			t.Errorf("create-cluster lacks %q:\n%s", want, cl)
		}
	}
	for _, want := range []string{"--subnets subnet-0a", "--node-role arn:aws:iam::123456789012:role/shared-node", "--instance-types m6i.xlarge", "desiredSize=2", "maxSize=3", "minSize=0", "--ami-type AL2023_x86_64_STANDARD", "id=lt-0abc"} {
		if !strings.Contains(ng, want) {
			t.Errorf("create-nodegroup lacks %q:\n%s", want, ng)
		}
	}
	if strings.Contains(ng, "subnet-0b") {
		t.Errorf("the node group spans more than the lane's one zone:\n%s", ng)
	}
	for _, want := range []string{"enableNetworkPolicy", "--addon-name coredns", "--addon-name kube-proxy", "--addon-name eks-pod-identity-agent", "--addon-name metrics-server", "--addon-name aws-ebs-csi-driver", "--addon-name vpc-cni"} {
		if !strings.Contains(addons, want) {
			t.Errorf("add-ons lack %q:\n%s", want, addons)
		}
	}
	log := s.log()
	// CC5: the first node is the infra node, the others are workers
	for _, want := range []string{"label node ip-10-0-0-1 node-role.kubernetes.io/control-plane=", "label node ip-10-0-0-2 node-role.kubernetes.io/worker="} {
		if !strings.Contains(log, want) {
			t.Errorf("no %q in\n%s", want, log)
		}
	}
	if strings.Contains(log, "label node ip-10-0-0-1 node-role.kubernetes.io/worker") {
		t.Error("the infra node of a two-node lane is also a worker")
	}
	// the kubeconfig is a token one with the context the workloads derive from the lane name, mode 0600 on the admin one
	b, err := os.ReadFile(kc)
	if err != nil || !strings.Contains(string(b), "name: kind-eks1") || !strings.Contains(string(b), "token: token") {
		t.Fatalf("kubeconfig (%v):\n%s", err, b)
	}
	if st, err := os.Stat(kc + ".admin"); err != nil || st.Mode().Perm() != 0o600 {
		t.Errorf("admin kubeconfig: %v %v", st, err)
	}
	if _, err := os.Stat(ev); err != nil {
		t.Errorf("evidence directory not made: %v", err)
	}
	// the node agent mirrors the upstream registries into this account's ECR
	applied := s.file("applied.yaml")
	for _, want := range []string{"123456789012.dkr.ecr.us-west-2.amazonaws.com", `--region "us-west-2"`, "docker.io ghcr.io quay.io registry.k8s.io", "sdo-mirror"} {
		if !strings.Contains(applied, want) {
			t.Errorf("node agent manifest lacks %q", want)
		}
	}
	if strings.Contains(applied, "@ECR@") || strings.Contains(applied, "@REGION@") {
		t.Error("node agent placeholders left in the manifest")
	}
	for _, want := range []string{"volumeBindingMode: WaitForFirstConsumer", "provisioner: ebs.csi.aws.com", "is-default-class: \"false\""} {
		if !strings.Contains(applied, want) {
			t.Errorf("the gp3 class lacks %q", want)
		}
	}
	// the default class is node-local: SDO's pods share ReadWriteOnce claims and the claim must place them
	if !strings.Contains(log, "apply -f") || !strings.Contains(log, "local-path-storage.yaml") || !strings.Contains(log, "rollout status deployment/local-path-provisioner") {
		t.Errorf("the local-path default class was not installed:\n%s", log)
	}
	if !strings.Contains(log, "ec2 authorize-security-group-ingress --group-id sg-0123 --protocol tcp --port 30000-32767 --cidr 203.0.113.7/32") {
		t.Errorf("node ports not opened:\n%s", log)
	}
	if b, _ := os.ReadFile(kc + ".entry"); strings.TrimSpace(string(b)) != "54.0.0.1" {
		t.Errorf("entry address = %q", b)
	}
	// every stage of the up is measured
	for _, want := range []string{"EKS_STAGE shared_ready", "EKS_STAGE control_plane_active", "EKS_STAGE nodegroup_active", "EKS_STAGE addons_running", "EKS_STAGE cluster_and_nodegroup_created", "EKS_STAGE nodes_ready", "EKS_STAGE node_agent_mirror_ready", "EKS_STAGE up_total"} {
		if !strings.Contains(out, want) {
			t.Errorf("no %q in\n%s", want, out)
		}
	}
	if b, _ := os.ReadFile(kc + ".stages"); !strings.Contains(string(b), "up_total ") {
		t.Errorf("stages file = %q", b)
	}
	// idempotent: a second up attaches and creates nothing
	if out, err := s.run(nil, "up", "eks1", kc, ev); err != nil || strings.Count(s.log(), "eks create-cluster") != 1 || strings.Count(s.log(), "cloudformation create-stack") != 1 {
		t.Fatalf("second up: %v\n%s\n%s", err, out, s.log())
	}
}

// A single-node lane's only node is both the infra node and a worker.
func TestEksLaneUpOfOneNodeLabelsItInfraAndWorker(t *testing.T) {
	s := newEksScript(t)
	if out, err := s.run([]string{"EKS_NODES=1"}, "up", "eks1", filepath.Join(t.TempDir(), "kc")); err != nil {
		t.Fatalf("up: %v\n%s", err, out)
	}
	log := s.log()
	if !strings.Contains(log, "label node ip-10-0-0-1 node-role.kubernetes.io/control-plane=") || !strings.Contains(log, "label node ip-10-0-0-1 node-role.kubernetes.io/worker=") {
		t.Errorf("labels:\n%s", log)
	}
}

// Nothing may bill after a failed up: the half-built cluster is deleted.
func TestEksLaneUpFailureDeletesTheHalfBuiltCluster(t *testing.T) {
	for name, extra := range map[string]string{"create fails": "FAKE_EKS_FAIL_CREATE=1", "no network policy": "FAKE_NO_NETPOL=1"} {
		s := newEksScript(t)
		out, err := s.run([]string{extra}, "up", "eks1", filepath.Join(t.TempDir(), "kc"))
		if err == nil {
			t.Fatalf("%s: up succeeded:\n%s", name, out)
		}
		if _, statErr := os.Stat(filepath.Join(s.state, "cluster-sdo-lane-eks1")); statErr == nil || !strings.Contains(s.log(), "eks delete-cluster --name sdo-lane-eks1") {
			t.Errorf("%s: the cluster was not deleted:\n%s\n%s", name, out, s.log())
		}
	}
}

func TestEksLaneUpRefusesAnotherAccountBeforeCreatingAnything(t *testing.T) {
	s := newEksScript(t)
	out, err := s.run([]string{"EKS_ACCOUNT=999999999999"}, "up", "eks1", filepath.Join(t.TempDir(), "kc"))
	if err == nil || !strings.Contains(out, "credentials belong to account 123456789012, expected 999999999999") || strings.Contains(s.log(), "create-") {
		t.Fatalf("up: %v\n%s\n%s", err, out, s.log())
	}
}

func TestEksLaneDownDeletesTheClusterAndTheKubeconfig(t *testing.T) {
	s := newEksScript(t)
	kc := filepath.Join(t.TempDir(), "eks1")
	if out, err := s.run(nil, "up", "eks1", kc); err != nil {
		t.Fatalf("up: %v\n%s", err, out)
	}
	out, err := s.run(nil, "down", "eks1", kc)
	if err != nil || !strings.Contains(out, "EKS_LANE_DELETED cluster=sdo-lane-eks1") {
		t.Fatalf("down: %v\n%s", err, out)
	}
	for _, p := range []string{kc, kc + ".admin", kc + ".entry", filepath.Join(s.state, "cluster-sdo-lane-eks1")} {
		if _, err := os.Stat(p); err == nil {
			t.Errorf("%s still exists", p)
		}
	}
	if out, err := s.run(nil, "down", "eks1", kc); err != nil || !strings.Contains(out, "does not exist") {
		t.Errorf("a second down: %v\n%s", err, out)
	}
}

// Image delivery: an app image keeps its declared name (the mirror repository is derived from it, with Docker's implicit
// registry spelled out), a pinned image goes to <prefix>/<name>, and an upstream the lane does not mirror is refused.
func TestEksLanePushPublishesUnderTheDeclaredName(t *testing.T) {
	s := newEksScript(t)
	out, err := s.run(nil, "push", "busybox:1.36", "yinfangchen/hotelreservation:latest", "ghcr.io/open-telemetry/demo:2.2.0-frontend", "kube-proxy")
	if err != nil {
		t.Fatalf("push: %v\n%s", err, out)
	}
	log := s.log()
	for _, want := range []string{
		"docker tag busybox:1.36 123456789012.dkr.ecr.us-west-2.amazonaws.com/sdo-mirror/docker.io/library/busybox:1.36",
		"docker push 123456789012.dkr.ecr.us-west-2.amazonaws.com/sdo-mirror/docker.io/yinfangchen/hotelreservation:latest",
		"sdo-mirror/ghcr.io/open-telemetry/demo:2.2.0-frontend",
		"sdo-mirror/docker.io/library/kube-proxy:latest",
		"docker login --username AWS --password-stdin 123456789012.dkr.ecr.us-west-2.amazonaws.com",
	} {
		if !strings.Contains(log, want) {
			t.Errorf("no %q in\n%s", want, log)
		}
	}
	if out, err := s.run(nil, "push", "example.org/a/b:1"); err == nil || !strings.Contains(out, "mirrors [docker.io ghcr.io quay.io registry.k8s.io], not example.org") {
		t.Errorf("an unmirrored upstream: %v\n%s", err, out)
	}
	pin, err := s.run(nil, "pin", "sdo-controller:v1")
	if err != nil || !strings.Contains(pin, "123456789012.dkr.ecr.us-west-2.amazonaws.com/sdo/sdo-controller@sha256:aaaa") {
		t.Errorf("pin: %v\n%s", err, pin)
	}
}

// --- a plan item through the manager on an eks host ---

// The eks host makes its lane with the platform script (sized from the footprint), the host-safety gates do not run,
// the cluster is kept while the lane is fresh, deleted once the host is unwanted and idle, and its cost is recorded.
func TestBenchEksHostLaneLifecycle(t *testing.T) {
	l := newLab(t)
	inv, err := os.ReadFile(l.root.path("hosts.yaml"))
	must(t, err)
	s := strings.Replace(string(inv), "clusters: [c1]}", `clusters: [c1], provider: eks, eks: {region: us-west-2, account: "123456789012", budget_per_hour: 3, vcpu_quota: 16, idle_timeout: 1s, max_load5_per_core: 0}}`, 1)
	must(t, os.WriteFile(l.root.path("hosts.yaml"), []byte(s), 0o644))
	t.Setenv("PATH", testdata("fakeeks")+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("HOST_CANARY_PROCSYS", "/nonexistent") // a canary read would fail: the eks host must not take one
	l.add("e1", l.hooked, "a@x", "[[1]]", 0)
	l.until("run done", func() bool { return l.exp("e1").Count(0).Done == 1 })
	st, err := StatusOf(l.root, "")
	must(t, err)
	if len(st.Hosts) != 1 || st.Hosts[0].EKS == nil || st.Hosts[0].Unhealthy != "" {
		t.Fatalf("host status = %+v", st.Hosts)
	}
	log := l.readPath(filepath.Join(l.state, "eks.log"))
	if !strings.Contains(log, "eks create-cluster --name sdo-lane-c1") || !strings.Contains(l.readPath(filepath.Join(l.state, "ng-sdo-lane-c1")), "--instance-types m6i.xlarge") {
		t.Fatalf("no cluster made sized for the footprint:\n%s", log)
	}
	l.until("cluster deleted and its cost recorded", func() bool {
		var led eksLedger
		return readJSON(l.root.eksLedgerPath("h1"), &led) == nil && len(led.Closed) == 1 && len(led.Open) == 0
	})
	if !strings.Contains(l.readPath(filepath.Join(l.state, "eks.log")), "eks delete-cluster --name sdo-lane-c1") {
		t.Fatal("the cluster was not deleted at the end")
	}
	var led eksLedger
	must(t, readJSON(l.root.eksLedgerPath("h1"), &led))
	if c := led.Closed[0]; c.Lane != "c1" || c.InstanceType != "m6i.xlarge" || c.Nodes != 2 || c.Until.Before(c.Since) {
		t.Fatalf("cost record = %+v", c)
	}
	b, _ := json.Marshal(led)
	t.Logf("ledger: %s", b)
}

// `lanes host status` of an eks host reports credentials and cost, and never asks the kernel or the root helper.
func TestHostStatusOfAnEksHostReportsCredentialsAndCostNotKernelGates(t *testing.T) {
	root := Root(t.TempDir())
	must(t, os.WriteFile(root.path("hosts.yaml"), []byte(eksHost), 0o644))
	must(t, os.MkdirAll(root.path("eks"), 0o755))
	must(t, os.WriteFile(root.eksLedgerPath("e"), []byte(`{"open":{"eks1":{"since":"2026-11-20T08:00:00Z","instance_type":"m6i.xlarge","nodes":2,"vcpus":8,"usd_per_hour":0.484}},"closed_usd":1.5}`), 0o644))
	var out, errw strings.Builder
	helper := func(context.Context, string, ...string) (string, error) {
		t.Fatal("an eks host asked the root helper")
		return "", nil
	}
	run := func(_ context.Context, _, script string) (string, error) {
		if strings.Contains(script, "sysctl") || strings.Contains(script, "ip ") {
			t.Fatalf("an eks host ran a kernel probe: %s", script)
		}
		return "account 123456789012\n", nil
	}
	if rc := HostCommand(context.Background(), root, []string{"status"}, &out, &errw, helper, run); rc != 0 {
		t.Fatalf("rc %d: %s", rc, errw.String())
	}
	for _, want := range []string{"e: credentials ok: account 123456789012, region us-west-2", "eks: 1/3 lanes, 8/24 vCPU, $0.48/h of $2.00/h", "lane eks1: m6i.xlarge x 2", "no cpuset, ledger, kernel or PSI gates"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("status lacks %q:\n%s", want, out.String())
		}
	}
	out.Reset()
	if rc := HostCommand(context.Background(), root, []string{"reserve", "e"}, &out, &errw, helper, run); rc != 0 || !strings.Contains(out.String(), "nothing to reserve") {
		t.Errorf("reserve all: rc %d: %s %s", rc, out.String(), errw.String())
	}
}

// `exists` is the platform's answer to "is the lane's cluster there": 0 yes, 1 AWS says no, 2 cannot tell. The manager
// forgets a lane it believes gone, so a failed call must never read as "gone".
func TestEksLaneExistsDistinguishesGoneFromUnknown(t *testing.T) {
	s := newEksScript(t)
	code := func(extra ...string) int {
		_, err := s.run(extra, "exists", "eks1")
		if err == nil {
			return 0
		}
		return err.(*exec.ExitError).ExitCode()
	}
	if c := code(); c != 1 {
		t.Fatalf("no cluster: exit %d, want 1", c)
	}
	if out, err := s.run(nil, "up", "eks1", filepath.Join(t.TempDir(), "kc")); err != nil {
		t.Fatalf("up: %v\n%s", err, out)
	}
	if c := code(); c != 0 {
		t.Fatalf("cluster there: exit %d, want 0", c)
	}
	if c := code("FAKE_AWS_DOWN=1"); c != 2 {
		t.Fatalf("aws unreachable: exit %d, want 2 (not gone)", c)
	}
}

// A cluster whose node group was scaled down (by an idle policy outside lanes) gets its nodes back when `up` attaches it.
func TestEksLaneUpRescalesAnAttachedClusterWithNoNodes(t *testing.T) {
	s := newEksScript(t)
	kc := filepath.Join(t.TempDir(), "kc")
	if out, err := s.run(nil, "up", "eks1", kc); err != nil {
		t.Fatalf("up: %v\n%s", err, out)
	}
	if strings.Contains(s.log(), "update-nodegroup-config") {
		t.Fatalf("a cluster at its size was scaled:\n%s", s.log())
	}
	must(t, os.WriteFile(filepath.Join(s.state, "desired"), []byte("0\n"), 0o644))
	if out, err := s.run(nil, "up", "eks1", kc); err != nil || !strings.Contains(s.log(), "--scaling-config minSize=0,maxSize=3,desiredSize=2") {
		t.Fatalf("up of a scaled-down cluster: %v\n%s\n%s", err, out, s.log())
	}
}

// A Down eks lane that may still have a cluster is cleaned (deleted), once per retry interval; a lane known gone, one with
// a step, or a lane that is not Down is not.
func TestEksDownLaneClustersAreDeletedNotLeftToBill(t *testing.T) {
	m := eksManager(t)
	m.lanes["eks1"].State, m.lanes["eks1"].Gone = LaneDown, false // failed or killed cluster-up: a cluster may be there
	m.lanes["eks2"].State, m.lanes["eks2"].Gone = LaneDown, true  // known gone
	m.lanes["eks3"].State, m.lanes["eks3"].Gone = LaneFree, false // free lanes follow the idle timeout
	got := m.eksOrphanLanes()
	if len(got) != 1 || got[0].Lane != "eks1" {
		t.Fatalf("orphans = %v, want only eks1", got)
	}
	m.eksCleanAt = map[string]time.Time{"eks1": m.now()}
	if got := m.eksOrphanLanes(); len(got) != 0 {
		t.Fatalf("asked again at once: %v", got)
	}
	m.advance(eksCleanupRetry)
	if got := m.eksOrphanLanes(); len(got) != 1 {
		t.Fatalf("not asked again after the retry interval: %v", got)
	}
}

// An up that attached a cluster and failed deletes it too: all-or-nothing, never a half-made lane left behind.
func TestEksLaneUpFailureOfAnAttachedClusterDeletesIt(t *testing.T) {
	s := newEksScript(t)
	kc := filepath.Join(t.TempDir(), "kc")
	if out, err := s.run(nil, "up", "eks1", kc); err != nil {
		t.Fatalf("up: %v\n%s", err, out)
	}
	out, err := s.run([]string{"FAKE_NO_NETPOL=1"}, "up", "eks1", kc)
	if err == nil {
		t.Fatalf("attach with no network policy succeeded:\n%s", out)
	}
	if _, statErr := os.Stat(filepath.Join(s.state, "cluster-sdo-lane-eks1")); statErr == nil {
		t.Errorf("the attached cluster was left:\n%s\n%s", out, s.log())
	}
}

// A host's max_builds bounds a kind host's churn; an eks host builds every lane it has at once.
func TestEksBuildLimitIsTheLaneCapNotMaxBuilds(t *testing.T) {
	m := eksManager(t)
	h := m.inv.Hosts["e"]
	if h.MaxBuilds != 2 || h.BuildLimit() != 3 {
		t.Fatalf("max_builds %d, build limit %d, want 2 and the lane cap 3", h.MaxBuilds, h.BuildLimit())
	}
	if (Host{MaxBuilds: 2, Clusters: []string{"a", "b", "c"}}).BuildLimit() != 2 {
		t.Fatal("a kind host's build limit is its max_builds")
	}
}

// A plan with several items starts every lane's cluster-up at once, not one after the other: the cold up (about 20 minutes
// on AWS) is paid once per batch.
func TestBenchEksLanesAreMadeInParallel(t *testing.T) {
	l := newLab(t)
	inv, err := os.ReadFile(l.root.path("hosts.yaml"))
	must(t, err)
	s := strings.Replace(string(inv), "clusters: [c1]}", `clusters: [c1, c2], provider: eks, eks: {region: us-west-2, account: "123456789012", budget_per_hour: 3, vcpu_quota: 32, idle_timeout: 1s, max_load5_per_core: 0}}`, 1)
	must(t, os.WriteFile(l.root.path("hosts.yaml"), []byte(s), 0o644))
	t.Setenv("PATH", testdata("fakeeks")+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("FAKE_EKS_CREATE_SECONDS", "2")
	t.Setenv("HOST_CANARY_PROCSYS", "/nonexistent")
	l.add("p1", l.hooked, "a@x", "[[1]]", 0)
	l.add("p2", l.hooked, "a@x", "[[1]]", 0)
	l.until("both runs done", func() bool { return l.exp("p1").Count(0).Done == 1 && l.exp("p2").Count(0).Done == 1 })
	times := l.read("eks-times.log")
	var starts, ends []float64
	for _, ln := range strings.Split(strings.TrimSpace(times), "\n") {
		f := strings.Fields(ln)
		if len(f) != 3 {
			continue
		}
		var v float64
		fmt.Sscan(f[2], &v)
		if f[0] == "create-start" {
			starts = append(starts, v)
		} else {
			ends = append(ends, v)
		}
	}
	if len(starts) != 2 || len(ends) != 2 {
		t.Fatalf("want 2 clusters made, got:\n%s", times)
	}
	if max(starts[0], starts[1]) >= min(ends[0], ends[1]) {
		t.Fatalf("the second cluster-up began after the first ended (serial):\n%s", times)
	}
}

// The node agent pre-pulls the lane's list through the mirror on every node (and so on a node that joins later); the
// list is written by `prepull`, with unqualified names spelled out the way containerd resolves them.
func TestEksNodeAgentPrepullsTheLanesImageList(t *testing.T) {
	s := newEksScript(t)
	kc := filepath.Join(t.TempDir(), "kc")
	if out, err := s.run(nil, "up", "eks1", kc); err != nil {
		t.Fatalf("up: %v\n%s", err, out)
	}
	applied := s.file("applied.yaml")
	for _, want := range []string{"prepull() {", "ctr -n k8s.io images pull --hosts-dir /etc/containerd/certs.d", "name: sdo-lane-prepull", "optional: true", "{name: host, mountPath: /host, readOnly: true}"} {
		if !strings.Contains(applied, want) {
			t.Errorf("the node agent manifest lacks %q", want)
		}
	}
	// the mirror writer and the prepull share one loop: the list is read only once the mirror headers are in place
	if i, j := strings.Index(applied, "date +%s > /certs/.sdo-mirror-refreshed"), strings.Index(applied, "\n                prepull\n"); i < 0 || j < i {
		t.Errorf("prepull does not follow the mirror refresh in the agent loop")
	}
	out, err := s.run(nil, "prepull", kc, "busybox:1.36", "yinfangchen/hotelreservation:latest", "123456789012.dkr.ecr.us-west-2.amazonaws.com/sdo/sdo-controller@sha256:"+strings.Repeat("a", 64))
	if err != nil || !strings.Contains(out, "EKS_PREPULL 3 images") {
		t.Fatalf("prepull: %v\n%s", err, out)
	}
	log := s.log()
	for _, want := range []string{"docker.io/library/busybox:1.36", "docker.io/yinfangchen/hotelreservation:latest", "sdo-controller@sha256:"} {
		if !strings.Contains(log, want) {
			t.Errorf("the pre-pull list lacks %q:\n%s", want, log)
		}
	}
}

// agentPrepull runs the node agent's prepull function as the manifest `up` applied it, with the agent container's mounts
// mapped under root and fake aws, ctr and chroot on PATH. The agent image's libc is older than the node's, so a `ctr`
// run in the container's own root fails to load (the fake container ctr says so, as the real one did); only a ctr run
// chrooted into the node's root (root/host) can pull, and it records the arguments it got. Returns the agent's output,
// the lines of the node's pulled-image record and the node ctr's recorded invocations.
func agentPrepull(t *testing.T, applied string, images ...string) (out string, pulled, calls []string) {
	t.Helper()
	const open, closing = "              prepull() {\n", "\n              }\n"
	i := strings.Index(applied, open)
	if i < 0 {
		t.Fatal("the node agent manifest has no prepull function")
	}
	body := applied[i:]
	body = body[:strings.Index(body, closing)+len(closing)]
	var fn []string
	for _, ln := range strings.Split(body, "\n") {
		fn = append(fn, strings.TrimPrefix(ln, "              "))
	}
	root := t.TempDir()
	for _, d := range []string{"prepull", "lane", "bin", "host/usr/bin", "usr/bin"} {
		if err := os.MkdirAll(filepath.Join(root, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	write := func(rel, content string, mode os.FileMode) {
		if err := os.WriteFile(filepath.Join(root, rel), []byte(content), mode); err != nil {
			t.Fatal(err)
		}
	}
	write("prepull/images", strings.Join(images, "\n")+"\n", 0o644)
	// the container's mounted /usr/bin/ctr (old manifests mounted the node's binary there) cannot load in its libc
	write("usr/bin/ctr", "#!/bin/sh\necho \"ctr: /lib64/libc.so.6: version 'GLIBC_2.34' not found (required by ctr)\" >&2\nexit 1\n", 0o755)
	write("host/usr/bin/ctr", "#!/bin/sh\necho \"$*\" >> '"+root+"/ctr.calls'\n", 0o755)
	// chroot ROOT CMD...: the node's own binaries (root/host as the node's /) in place of the container's
	write("bin/chroot", "#!/bin/sh\nr=$1; shift; c=$1; shift\nexec \"$r$c\" \"$@\"\n", 0o755)
	write("bin/aws", "#!/bin/sh\necho tok\n", 0o755)
	script := strings.Join(fn, "\n")
	for from, to := range map[string]string{"/prepull/images": root + "/prepull/images", "/lane/": root + "/lane/", "-x /usr/bin/ctr": "-x " + root + "/usr/bin/ctr", "/host": root + "/host"} {
		script = strings.ReplaceAll(script, from, to)
	}
	cmd := exec.Command("bash", "-c", script+"\nprepull\n")
	cmd.Env = append(os.Environ(), "PATH="+filepath.Join(root, "bin")+string(os.PathListSeparator)+filepath.Join(root, "usr/bin")+string(os.PathListSeparator)+os.Getenv("PATH"))
	b, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("prepull: %v\n%s", err, b)
	}
	read := func(f string) []string {
		data, _ := os.ReadFile(filepath.Join(root, f))
		return strings.Split(strings.TrimSpace(string(data)), "\n")
	}
	return string(b), read("lane/prepulled"), read("ctr.calls")
}

// The node agent's image is older than the nodes it runs on: the prepull must run the node's own ctr in the node's root
// (a ctr bind-mounted into the container died with "GLIBC_2.34 not found" on every node, and no image was ever pulled).
func TestEksNodeAgentPrepullRunsTheNodesCtrInTheNodesRoot(t *testing.T) {
	s := newEksScript(t)
	if out, err := s.run(nil, "up", "eks1", filepath.Join(t.TempDir(), "kc")); err != nil {
		t.Fatalf("up: %v\n%s", err, out)
	}
	applied := s.file("applied.yaml")
	ecr := "123456789012.dkr.ecr.us-west-2.amazonaws.com/sdo/sdo-controller@sha256:" + strings.Repeat("a", 64)
	out, pulled, calls := agentPrepull(t, applied, "docker.io/library/busybox:1.36", ecr)
	if strings.Contains(out, "GLIBC") || strings.Contains(out, "failed") {
		t.Errorf("the prepull ran a ctr the agent's libc cannot load:\n%s", out)
	}
	want := []string{"docker.io/library/busybox:1.36", ecr}
	if strings.Join(pulled, " ") != strings.Join(want, " ") {
		t.Errorf("pulled = %v, want %v", pulled, want)
	}
	if len(calls) != 2 || !strings.Contains(calls[0], "-n k8s.io images pull --hosts-dir /etc/containerd/certs.d docker.io/library/busybox:1.36") ||
		!strings.Contains(calls[1], "--user AWS:tok") || !strings.HasSuffix(calls[1], ecr) {
		t.Errorf("the node ctr was called as %q", calls)
	}
}

type startSpy struct {
	HostOps
	started []Step
}

func (s *startSpy) Start(_ context.Context, st Step) error {
	s.started = append(s.started, st)
	return nil
}

// A lane whose first builds failed has no recorded prepared state (the failure came before the workload was recorded):
// the release that deletes its cluster needs none, so the cluster is deleted instead of left billing.
func TestEksDownLaneWithNothingPreparedStillGetsItsClusterDeleted(t *testing.T) {
	m := eksManager(t)
	spy := &startSpy{}
	m.Ops = func(string, Host) HostOps { return spy }
	l := m.lanes["eks1"]
	l.State, l.Gone, l.Prepared = LaneDown, false, nil
	m.eksCleanDown(context.Background(), map[string]bool{"e": true})
	if len(spy.started) != 1 || spy.started[0].Kind != StepRelease || !spy.started[0].DeleteCluster {
		t.Fatalf("started = %+v, want one cluster-deleting release", spy.started)
	}
	if l.Step == nil {
		t.Fatal("the lane has no step after the release started")
	}
}

// A failed build or gate on an eks lane is retried in place; a new cluster (about 20 minutes) is not the first resort.
func TestEksLanesAreNotRecreatedAfterAFailure(t *testing.T) {
	m := eksManager(t)
	if m.inv.Hosts["e"].RecreatesAfterFailure("eks1") {
		t.Fatal("an eks host recreates its cluster after a failed build")
	}
	if !(Host{ClusterUp: "up"}).RecreatesAfterFailure("c") {
		t.Fatal("a kind host with cluster_up no longer recreates after a failure")
	}
	if (Host{}).RecreatesAfterFailure("c") {
		t.Fatal("a host that cannot make clusters recreates one")
	}
}

// The VPC, subnets, roles and node launch template are made once per account and region (host-setup), idempotently, and
// recorded: the second call, and every lane's up, reuses them.
func TestEksHostSetupMakesTheSharedNetworkOnceAndRecordsIt(t *testing.T) {
	s := newEksScript(t)
	st := filepath.Join(t.TempDir(), "eks-state")
	out, err := s.run([]string{"EKS_STATE_DIR=" + st}, "host-setup")
	if err != nil || !strings.Contains(out, "EKS_SHARED vpc=vpc-0aaa subnets=subnet-0a,subnet-0b cluster_role=arn:aws:iam::123456789012:role/shared-cluster node_role=arn:aws:iam::123456789012:role/shared-node") {
		t.Fatalf("host-setup: %v\n%s", err, out)
	}
	if b, _ := os.ReadFile(filepath.Join(st, "shared-us-west-2.env")); !strings.Contains(string(b), "EKS_SHARED_VPC=vpc-0aaa") {
		t.Errorf("the shared ids were not recorded in the host's eks state: %q", b)
	}
	if args := s.file("stack-args"); !strings.Contains(args, "CAPABILITY_IAM") || !strings.Contains(args, "Project") {
		t.Errorf("create-stack args = %q", args)
	}
	if out, err := s.run([]string{"EKS_STATE_DIR=" + st}, "host-setup"); err != nil || strings.Count(s.log(), "cloudformation create-stack") != 1 {
		t.Fatalf("second host-setup: %v\n%s\n%s", err, out, s.log())
	}
}

// Two lanes starting together on a host that has no shared network yet make it once and both wait for it.
func TestEksConcurrentFirstUpsShareOneSetup(t *testing.T) {
	s := newEksScript(t)
	s.env = append(s.env, "FAKE_STACK_SECONDS=1")
	done := make(chan error, 2)
	for _, lane := range []string{"eks1", "eks2"} {
		go func() {
			_, err := s.run(nil, "up", lane, filepath.Join(t.TempDir(), "kc"))
			done <- err
		}()
	}
	for range 2 {
		if err := <-done; err != nil {
			t.Fatalf("up: %v\n%s", err, s.log())
		}
	}
	if n := strings.Count(s.file("stack-args"), "\n"); n != 1 {
		t.Errorf("%d shared stacks created, want 1", n)
	}
}

// Deleting a lane removes its node group and control plane only; the shared network and roles stay for the next lane.
func TestEksLaneDownKeepsTheSharedResources(t *testing.T) {
	s := newEksScript(t)
	kc := filepath.Join(t.TempDir(), "eks1")
	if out, err := s.run(nil, "up", "eks1", kc); err != nil {
		t.Fatalf("up: %v\n%s", err, out)
	}
	if out, err := s.run(nil, "down", "eks1", kc); err != nil {
		t.Fatalf("down: %v\n%s", err, out)
	}
	log := s.log()
	if !strings.Contains(log, "eks delete-nodegroup --cluster-name sdo-lane-eks1 --nodegroup-name workers") || !strings.Contains(log, "eks delete-cluster --name sdo-lane-eks1") {
		t.Errorf("the lane's node group and control plane were not deleted:\n%s", log)
	}
	if strings.Contains(log, "delete-stack") {
		t.Errorf("a lane delete removed the shared stack:\n%s", log)
	}
	if strings.Index(log, "eks delete-nodegroup") > strings.Index(log, "eks delete-cluster") {
		t.Errorf("the control plane was deleted before its node group")
	}
}

// A cluster made by the earlier path (eksctl, its own CloudFormation stacks) is still deleted with eksctl, which owns them.
func TestEksLaneDownOfAnEksctlMadeClusterUsesEksctl(t *testing.T) {
	s := newEksScript(t)
	cfg := filepath.Join(t.TempDir(), "cfg")
	must(t, os.WriteFile(cfg, []byte("metadata:\n  name: sdo-lane-eks1\n"), 0o644))
	fake := exec.Command(filepath.Join(testdata("fakeeks"), "eksctl"), "create", "cluster", "-f", cfg, "--kubeconfig", filepath.Join(t.TempDir(), "k"))
	fake.Env = append(os.Environ(), "FAKE_STATE="+s.state)
	if out, err := fake.CombinedOutput(); err != nil {
		t.Fatalf("%v %s", err, out)
	}
	if out, err := s.run(nil, "down", "eks1"); err != nil || !strings.Contains(s.log(), "eksctl delete cluster --name sdo-lane-eks1") || strings.Contains(s.log(), "eks delete-cluster") {
		t.Fatalf("down: %v\n%s\n%s", err, out, s.log())
	}
}

// host-teardown removes the shared stack, and refuses while a lane cluster still uses it.
func TestEksHostTeardownRemovesTheSharedStackOnlyWhenNoLaneIsLeft(t *testing.T) {
	s := newEksScript(t)
	if out, err := s.run(nil, "up", "eks1", filepath.Join(t.TempDir(), "kc")); err != nil {
		t.Fatalf("up: %v\n%s", err, out)
	}
	out, err := s.run(nil, "host-teardown")
	if err == nil || !strings.Contains(out, "sdo-lane-eks1") || strings.Contains(s.log(), "delete-stack") {
		t.Fatalf("teardown with a lane up: %v\n%s", err, out)
	}
	if out, err := s.run(nil, "down", "eks1"); err != nil {
		t.Fatalf("down: %v\n%s", err, out)
	}
	if out, err := s.run(nil, "host-teardown"); err != nil || !strings.Contains(s.log(), "cloudformation delete-stack --stack-name sdo-lanes-shared") {
		t.Fatalf("teardown: %v\n%s\n%s", err, out, s.log())
	}
	if out, err := s.run(nil, "host-teardown"); err != nil || strings.Count(s.log(), "delete-stack") != 1 {
		t.Fatalf("second teardown: %v\n%s", err, out)
	}
}

// `lanes host adopt` of an eks host sets up its shared network; `lanes host teardown` removes it.
func TestHostAdoptOfAnEksHostRunsHostSetup(t *testing.T) {
	root := Root(t.TempDir())
	must(t, os.WriteFile(root.path("hosts.yaml"), []byte(strings.Replace(eksHost, "staging: /s", "staging: "+t.TempDir(), 1)), 0o644))
	var scripts []string
	run := func(_ context.Context, _, script string) (string, error) {
		scripts = append(scripts, script)
		return "EKS_SHARED vpc=vpc-1\n", nil
	}
	helper := func(context.Context, string, ...string) (string, error) {
		t.Fatal("an eks host asked the root helper")
		return "", nil
	}
	var out, errw strings.Builder
	if rc := HostCommand(context.Background(), root, []string{"adopt", "e"}, &out, &errw, helper, run); rc != 0 {
		t.Fatalf("adopt rc %d: %s", rc, errw.String())
	}
	if len(scripts) != 1 || !strings.Contains(scripts[0], "eks-lane.sh' host-setup") || !strings.Contains(scripts[0], "EKS_REGION='us-west-2'") || !strings.Contains(out.String(), "EKS_SHARED") {
		t.Fatalf("scripts %q out %q", scripts, out.String())
	}
	if rc := HostCommand(context.Background(), root, []string{"teardown", "e"}, &out, &errw, helper, run); rc != 0 || len(scripts) != 2 || !strings.Contains(scripts[1], "eks-lane.sh' host-teardown") {
		t.Fatalf("teardown rc %d scripts %q: %s", rc, scripts, errw.String())
	}
}

// The shared node launch template carries what a node needs that eksctl used to set per lane: a gp3 root volume and an
// instance metadata hop limit of 2 (without it the EBS CSI controller pods cannot reach the node credentials and the add-on
// never becomes healthy).
func TestEksSharedStackTemplateSetsNodeVolumeAndMetadataHops(t *testing.T) {
	b, err := os.ReadFile(filepath.Join(pkgDir, "..", "..", "deploy", "eks", "shared.yaml"))
	must(t, err)
	for _, want := range []string{"HttpPutResponseHopLimit: 2", "VolumeType: gp3", "AmazonEKSClusterPolicy", "AmazonEBSCSIDriverPolicy", "NodeLaunchTemplateVersion"} {
		if !strings.Contains(string(b), want) {
			t.Errorf("shared.yaml lacks %q", want)
		}
	}
}

// An up is ready when the add-on pods run; it does not wait for the add-on API status, which lags them by minutes.
func TestEksLaneUpWaitsForAddonPodsNotAddonStatus(t *testing.T) {
	s := newEksScript(t)
	if out, err := s.run(nil, "up", "eks1", filepath.Join(t.TempDir(), "kc")); err != nil {
		t.Fatalf("up: %v\n%s", err, out)
	}
	for _, d := range []string{"coredns", "metrics-server", "ebs-csi-controller"} {
		if !strings.Contains(s.log(), "rollout status deployment/"+d) {
			t.Errorf("no wait for deployment %s:\n%s", d, s.log())
		}
	}
	if strings.Contains(s.log(), "eks describe-addon --cluster-name sdo-lane-eks1 --addon-name coredns") {
		t.Error("up polls the coredns add-on status")
	}
}

// A lane whose cluster does not exist (its cluster-up failed and removed what it made, or it was never made) has no API to
// read: it is not unhealthy, and the next build makes it. Judging it unhealthy blocked that build for ever (the lane got "no
// new steps", so nothing ever made the cluster the check needs).
func TestEksLaneWithoutAKubeconfigIsNotUnhealthyForItsMissingCluster(t *testing.T) {
	m := eksManager(t)
	m.EksCheck = func(ctx context.Context, lane, kubeconfig string) error { return fmt.Errorf("EKS API not reachable") }
	h := m.inv.Hosts["e"]
	h.Kubeconfigs = t.TempDir()
	m.inv.Hosts["e"] = h
	m.lanes["eks1"].Gone = false // a free lane the manager believes has a cluster, after a failed build removed it
	m.lanes["eks2"].Gone = false // a lane whose cluster exists: its kubeconfig is there
	must(t, os.WriteFile(filepath.Join(h.Kubeconfigs, "eks2"), []byte("x"), 0o600))
	m.observeEksLanes(context.Background(), map[string]bool{"e": true})
	if why := m.laneGate("eks1"); why != "" {
		t.Errorf("a lane with no cluster is gated as unhealthy, so its build can never start: %q", why)
	}
	if why := m.laneGate("eks2"); !strings.Contains(why, "unhealthy") {
		t.Errorf("a lane whose cluster exists and does not answer must stay gated: %q", why)
	}
}
