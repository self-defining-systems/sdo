//go:build integration

package bench

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// --- the script, against fake aws, eksctl, kubectl, helm and vcluster ---

type vcScript struct {
	*eksScript
	host string
	hkc  string
}

func newVcScript(t *testing.T) *vcScript {
	t.Helper()
	s := &vcScript{eksScript: newEksScript(t), host: "lanes-host-v"}
	s.hkc = filepath.Join(t.TempDir(), "kube", ".host", "admin")
	s.env = append(s.env, "EKS_HOST_CLUSTER="+s.host, "EKS_HOST_KUBECONFIG="+s.hkc, "EKS_NODE_VCPUS=4", "EKS_NODES=2")
	return s
}

func (s *vcScript) run(extra []string, args ...string) (string, error) {
	cmd := exec.Command("bash", append([]string{filepath.Join(pkgDir, "..", "..", "deploy", "eks-vcluster.sh")}, args...)...)
	cmd.Env = append(append([]string(nil), s.env...), extra...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func (s *vcScript) hostUp(t *testing.T) string {
	t.Helper()
	out, err := s.run(nil, "host-up")
	if err != nil {
		t.Fatalf("host-up: %v\n%s", err, out)
	}
	return out
}

func TestEksVclusterHostUpMakesTheHostClusterOnce(t *testing.T) {
	s := newVcScript(t)
	out := s.hostUp(t)
	if !strings.Contains(out, "EKS_HOST_READY cluster=lanes-host-v created=1") {
		t.Fatalf("host-up:\n%s", out)
	}
	cfg := s.file("cfg-lanes-host-v")
	for _, want := range []string{"name: lanes-host-v", "karpenter:", `version: "1.6.0"`, "minSize: 0", "labels: {sdo-role: system}", "{key: sdo-role, value: system, effect: NoSchedule}",
		`enableNetworkPolicy":"true"`, "{name: metrics-server", `"tolerations":[{"operator":"Exists"}]`} {
		if !strings.Contains(cfg, want) {
			t.Errorf("host cluster config lacks %q:\n%s", want, cfg)
		}
	}
	log := s.log()
	for _, want := range []string{"helm status karpenter --namespace karpenter", "helm upgrade karpenter oci://public.ecr.aws/karpenter/karpenter --version 1.6.0", "ec2 create-tags --resources subnet-0a sg-0123 --tags Key=karpenter.sh/discovery,Value=lanes-host-v",
		"ec2 authorize-security-group-ingress --group-id sg-0123 --protocol tcp --port 30000-32767 --cidr 203.0.113.7/32", "rollout status daemonset/sdo-lane-node-agent"} {
		if !strings.Contains(log, want) {
			t.Errorf("no %q in\n%s", want, log)
		}
	}
	applied := s.file("applied.yaml")
	for _, want := range []string{"kind: EC2NodeClass", "role: eksctl-node-role", "karpenter.sh/discovery: lanes-host-v", "kind: PriorityClass", "value: -10",
		"- operator: Exists", "kind: DaemonSet", "123456789012.dkr.ecr.us-west-2.amazonaws.com"} {
		if !strings.Contains(applied, want) {
			t.Errorf("applied manifests lack %q", want)
		}
	}
	if strings.Contains(applied, "disk-pressure") {
		t.Error("local-path helper pods must tolerate every lane taint")
	}
	if info, err := os.Stat(s.hkc); err != nil || info.Mode().Perm() != 0o600 {
		t.Errorf("host kubeconfig: %v %v", info, err)
	}
	// once: a second host-up attaches
	if out2, err := s.run(nil, "host-up"); err != nil || !strings.Contains(out2, "created=0") || strings.Count(s.log(), "eksctl create cluster") != 1 {
		t.Fatalf("second host-up: %v\n%s", err, out2)
	}
}

func TestEksVclusterLaneUpOnAWarmHostMakesNoCluster(t *testing.T) {
	s := newVcScript(t)
	s.hostUp(t)
	creates := strings.Count(s.log(), "eksctl create cluster")
	kc := filepath.Join(t.TempDir(), "kube", "vc1")
	out, err := s.run(nil, "up", "vc1", kc, filepath.Join(t.TempDir(), "ev"))
	if err != nil || !strings.Contains(out, "EKS_LANE_READY cluster=vc1 host=lanes-host-v instance=m6i.xlarge nodes=2") {
		t.Fatalf("up: %v\n%s", err, out)
	}
	if strings.Count(s.log(), "eksctl create cluster") != creates {
		t.Error("a lane made a cluster")
	}
	applied := s.file("applied.yaml")
	for _, want := range []string{"metadata: {name: lane-vc1,", "taints: [{key: sdo-lane, value: \"vc1\", effect: NoSchedule}]", "values: [m6i.xlarge]", `limits: {cpu: "8"}`,
		"consolidationPolicy: WhenEmpty", `budgets: [{nodes: "0", reasons: [Drifted]}]`, "replicas: 2", "priorityClassName: sdo-lane-capacity"} {
		if !strings.Contains(applied, want) {
			t.Errorf("lane manifests lack %q", want)
		}
	}
	vals := s.file("values-vc1")
	for _, want := range []string{`enforceTolerations: ["sdo-lane=vc1:NoSchedule"]`, "enabled: true\n      selector:\n        labels: {sdo-lane: \"vc1\"}", "storageClasses:\n      enabled: true", "networkPolicies:\n      enabled: true",
		"nodeSelector: {sdo-role: system}", "resourceQuota:\n    enabled: false", "type: NodePort", "extraSANs: [54.0.0.1]"} {
		if !strings.Contains(vals, want) {
			t.Errorf("vcluster values lack %q:\n%s", want, vals)
		}
	}
	b, err := os.ReadFile(kc)
	if err != nil || !strings.Contains(string(b), "name: kind-vc1") || !strings.Contains(string(b), "server: https://54.0.0.1:31234") {
		t.Fatalf("lane kubeconfig (%v):\n%s", err, b)
	}
	if e, _ := os.ReadFile(kc + ".entry"); strings.TrimSpace(string(e)) != "54.0.0.1" {
		t.Errorf("entry = %q", e)
	}
	log := s.log()
	if !strings.Contains(log, "get pods -l app.kubernetes.io/name=sdo-lane-node-agent") {
		t.Errorf("up did not wait for the node agent of the lane's nodes:\n%s", log)
	}
	for _, want := range []string{"label node ip-vc1-1 node-role.kubernetes.io/control-plane=", "label node ip-vc1-2 node-role.kubernetes.io/worker=", "delete deployment capacity-vc1"} {
		if !strings.Contains(log, want) {
			t.Errorf("no %q in\n%s", want, log)
		}
	}
	for _, want := range []string{"EKS_STAGE host_ready", "EKS_STAGE nodes_requested", "EKS_STAGE vcluster_created", "EKS_STAGE lane_api_ready", "EKS_STAGE nodes_ready", "EKS_STAGE up_total"} {
		if !strings.Contains(out, want) {
			t.Errorf("no %q in\n%s", want, out)
		}
	}
	// idempotent: a second up reuses the vcluster
	n := strings.Count(s.log(), "vcluster create")
	if out, err := s.run(nil, "up", "vc1", kc); err != nil || strings.Count(s.log(), "vcluster create") != n {
		t.Fatalf("second up: %v\n%s", err, out)
	}
}

func TestEksVclusterLaneUpNeedsAnAdoptedHost(t *testing.T) {
	s := newVcScript(t)
	out, err := s.run(nil, "up", "vc1", filepath.Join(t.TempDir(), "vc1"))
	if err == nil || !strings.Contains(out, "run `lanes host adopt` first") {
		t.Fatalf("up on a host that is not there: %v\n%s", err, out)
	}
}

// A lane whose up fails is removed, nodes included: nothing bills.
func TestEksVclusterLaneUpFailureRemovesTheLane(t *testing.T) {
	bad := newVcScript(t)
	bad.hostUp(t)
	out, err := bad.run([]string{"FAKE_VCLUSTER_FAIL=1"}, "up", "vc1", filepath.Join(t.TempDir(), "vc1"))
	if err == nil {
		t.Fatalf("up succeeded:\n%s", out)
	}
	if !strings.Contains(bad.log(), "delete nodepool lane-vc1") || !strings.Contains(bad.log(), "delete deployment capacity-vc1") {
		t.Errorf("the failed lane's nodes were not released:\n%s", bad.log())
	}
}

func TestEksVclusterLaneDownReleasesTheNodesAndKeepsTheHostUnlessItWasTheLastLane(t *testing.T) {
	s := newVcScript(t)
	s.hostUp(t)
	kc := filepath.Join(t.TempDir(), "kube", "vc1")
	if out, err := s.run(nil, "up", "vc1", kc); err != nil {
		t.Fatalf("up: %v\n%s", err, out)
	}
	if out, err := s.run(nil, "up", "vc2", filepath.Join(filepath.Dir(kc), "vc2")); err != nil {
		t.Fatalf("up vc2: %v\n%s", err, out)
	}
	out, err := s.run([]string{"EKS_HOST_LAST=1"}, "down", "vc1", kc)
	if err != nil || !strings.Contains(out, "EKS_LANE_DELETED cluster=vc1") {
		t.Fatalf("down: %v\n%s", err, out)
	}
	for _, p := range []string{kc, kc + ".entry", kc + ".stages", filepath.Join(s.state, "nodepool-vc1"), filepath.Join(s.state, "ns-vc1")} {
		if _, err := os.Stat(p); err == nil {
			t.Errorf("%s still exists", p)
		}
	}
	if !strings.Contains(s.log(), "delete nodepool lane-vc1") || !strings.Contains(s.log(), "vcluster delete vc1") {
		t.Errorf("down did not delete the lane:\n%s", s.log())
	}
	if strings.Contains(s.log(), "scale nodegroup") {
		t.Error("the host was scaled to zero under lane vc2")
	}
	if _, err := os.Stat(filepath.Join(s.state, "nodepool-vc2")); err != nil {
		t.Error("vc2 lost its nodes")
	}
	// the last lane: the system node group goes to zero, the control plane stays
	if out, err := s.run([]string{"EKS_HOST_LAST=1"}, "down", "vc2", filepath.Join(filepath.Dir(kc), "vc2")); err != nil {
		t.Fatalf("down vc2: %v\n%s", err, out)
	}
	if !strings.Contains(s.log(), "scale nodegroup --cluster lanes-host-v --name system --nodes 0") {
		t.Errorf("the host's nodes were not scaled to zero:\n%s", s.log())
	}
	if strings.Contains(s.log(), "eksctl delete cluster") {
		t.Error("the host cluster was deleted")
	}
	// the add-ons' PodDisruptionBudgets would hold the last node in a drain that cannot evict
	if i, j := strings.Index(s.log(), "delete pdb coredns metrics-server"), strings.Index(s.log(), "--name system --nodes 0"); i < 0 || j < i {
		t.Errorf("the budgets were not removed before scaling to zero:\n%s", s.log())
	}
	// the next lane brings the system node back
	if out, err := s.run(nil, "up", "vc3", filepath.Join(filepath.Dir(kc), "vc3")); err != nil || !strings.Contains(s.log(), "scale nodegroup --cluster lanes-host-v --name system --nodes 1") {
		t.Fatalf("up after zero: %v\n%s\n%s", err, out, s.log())
	}
}

func TestEksVclusterExistsReadsTheHostNeverAGuess(t *testing.T) {
	s := newVcScript(t)
	s.hostUp(t)
	if _, err := s.run(nil, "up", "vc1", filepath.Join(t.TempDir(), "vc1")); err != nil {
		t.Fatal(err)
	}
	code := func(extra []string, lane string) int {
		out, err := s.run(extra, "exists", lane)
		if err == nil {
			return 0
		}
		if ee, ok := err.(*exec.ExitError); ok {
			return ee.ExitCode()
		}
		t.Fatalf("exists: %v\n%s", err, out)
		return -1
	}
	if got := code(nil, "vc1"); got != 0 {
		t.Errorf("a lane that exists: %d", got)
	}
	if got := code(nil, "vc9"); got != 1 {
		t.Errorf("a lane that does not: %d", got)
	}
	if got := code([]string{"FAKE_KUBE_DOWN=1"}, "vc9"); got != 2 {
		t.Errorf("an unreachable host cluster is not 'gone': %d", got)
	}
}

func TestEksVclusterPrepullMergesIntoTheHostsList(t *testing.T) {
	s := newVcScript(t)
	s.hostUp(t)
	if out, err := s.run(nil, "prepull", "unused", "busybox:1.36"); err != nil {
		t.Fatalf("prepull: %v\n%s", err, out)
	}
	if out, err := s.run(nil, "prepull", "unused", "yinfangchen/hotelreservation:latest", "busybox:1.36", "123456789012.dkr.ecr.us-west-2.amazonaws.com/sdo/x@sha256:aa"); err != nil {
		t.Fatalf("prepull 2: %v\n%s", err, out)
	}
	got := s.file("prepull-images")
	for _, want := range []string{"docker.io/library/busybox:1.36", "docker.io/yinfangchen/hotelreservation:latest", "123456789012.dkr.ecr.us-west-2.amazonaws.com/sdo/x@sha256:aa"} {
		if strings.Count(got, want) != 1 {
			t.Errorf("list lacks exactly one %q:\n%s", want, got)
		}
	}
}

// A lane that cannot be shown gone is not gone: `down` answers success only after the host says NotFound, never from a
// call that failed (an expired credential, a throttled or unreachable API counted as "nothing to delete" and the lane's
// nodes billed on with the ledger closed).
func TestEksVclusterDownFailsClosedWhenTheHostCannotBeRead(t *testing.T) {
	for _, down := range []string{"FAKE_EKS_DOWN=1", "FAKE_KUBE_DOWN=1", "FAKE_AWS_ACCOUNT=999999999999"} {
		s := newVcScript(t)
		s.hostUp(t)
		kc := filepath.Join(t.TempDir(), "kube", "vc1")
		if out, err := s.run(nil, "up", "vc1", kc); err != nil {
			t.Fatalf("up: %v\n%s", err, out)
		}
		s.env = append(s.env, "EKS_ACCOUNT=123456789012")
		out, err := s.run([]string{down}, "down", "vc1", kc)
		if err == nil || strings.Contains(out, "EKS_LANE_DELETED") {
			t.Errorf("%s: down reported success:\n%s", down, out)
		}
		if _, err := os.Stat(kc); err != nil {
			t.Errorf("%s: down removed the kubeconfig of a lane it did not delete", down)
		}
		if _, err := os.Stat(filepath.Join(s.state, "nodepool-vc1")); err != nil {
			t.Errorf("%s: the fake lane is gone, so the test proves nothing", down)
		}
	}
	// the host cluster itself being gone is a fact, not a failure: nothing is left to bill
	s := newVcScript(t)
	s.hostUp(t)
	must(t, os.Remove(filepath.Join(s.state, "cluster-"+s.host)))
	if out, err := s.run(nil, "down", "vc1", filepath.Join(t.TempDir(), "vc1")); err != nil || !strings.Contains(out, "EKS_LANE_DELETED") {
		t.Errorf("down of a lane whose host cluster is gone: %v\n%s", err, out)
	}
}

// A lane is a namespace, a NodePool and its capacity pods: `exists` says gone only when all are.
func TestEksVclusterExistsCountsTheNodePoolOfALaneWithNoNamespace(t *testing.T) {
	s := newVcScript(t)
	s.hostUp(t)
	must(t, os.WriteFile(filepath.Join(s.state, "nodepool-vc7"), nil, 0o644)) // a build killed after the NodePool, before `vcluster create`
	if out, err := s.run(nil, "exists", "vc7"); err != nil {
		t.Errorf("a lane with a NodePool and no namespace is gone: %v\n%s", err, out)
	}
	// and `down` deletes it
	if out, err := s.run(nil, "down", "vc7", filepath.Join(t.TempDir(), "vc7")); err != nil {
		t.Fatalf("down: %v\n%s", err, out)
	}
	if _, err := os.Stat(filepath.Join(s.state, "nodepool-vc7")); err == nil {
		t.Error("down left the NodePool")
	}
	if out, err := s.run(nil, "exists", "vc7"); err == nil {
		t.Errorf("deleted, yet it exists: %s", out)
	}
}

// Releases that run at the same time each see the others' lanes: the host's nodes go when the host has no lane left,
// counted on the host under its lock, not from a ledger read when the step started.
func TestEksVclusterSimultaneousLastDownsScaleTheHostToZeroOnce(t *testing.T) {
	s := newVcScript(t)
	s.hostUp(t)
	dir := filepath.Join(t.TempDir(), "kube")
	for _, lane := range []string{"vc1", "vc2"} {
		if out, err := s.run(nil, "up", lane, filepath.Join(dir, lane)); err != nil {
			t.Fatalf("up %s: %v\n%s", lane, err, out)
		}
	}
	errs := make(chan error, 2)
	for _, lane := range []string{"vc1", "vc2"} {
		go func() {
			out, err := s.run([]string{"EKS_HOST_LAST=1"}, "down", lane, filepath.Join(dir, lane))
			if err != nil {
				err = fmt.Errorf("%s: %v\n%s", lane, err, out)
			}
			errs <- err
		}()
	}
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if n := strings.Count(s.log(), "--name system --nodes 0"); n != 1 {
		t.Errorf("the system node group was scaled to zero %d times with no lane left:\n%s", n, s.log())
	}
}

// Parallel lanes write the host kubeconfig one after another; a reader never sees a half-written file.
func TestEksVclusterHostKubeconfigIsReplacedAtomically(t *testing.T) {
	b, err := os.ReadFile(filepath.Join(pkgDir, "..", "..", "deploy", "eks-vcluster.sh"))
	must(t, err)
	if strings.Contains(string(b), `--kubeconfig "$HK" >/dev/null 2>&1 || true`) {
		t.Error("`aws eks update-kubeconfig --kubeconfig $HK` rewrites the shared host kubeconfig in place")
	}
}

// A lane's nodes are its own (tainted, one lane each), so a pod's disk is never shared: the vcluster chart's default LimitRange
// request (ephemeral-storage 3Gi per container) would only make the scheduler count 3 GiB per container against a node's
// ~89 GiB allocatable. A 2-node astronomy lane (32 app containers + observability + SDO) then ran out of "ephemeral-storage"
// while the disks were nearly empty, and the SDO claims pinned to one node could not schedule.
func TestEksVclusterLaneRequestsNoEphemeralStorageByDefault(t *testing.T) {
	cmd := exec.Command("bash", "-c", `set -e; source <(sed -n '/^lane_values()/,/^}/p' `+filepath.Join(pkgDir, "..", "..", "deploy", "eks-vcluster.sh")+`); lane_values vc3 192.0.2.1 /dev/stdout`)
	cmd.Env = []string{"PATH=/usr/bin:/bin"}
	out, err := cmd.CombinedOutput()
	must(t, err)
	v := string(out)
	i := strings.Index(v, "defaultRequest:")
	if i < 0 {
		t.Fatalf("the lane's LimitRange has no defaultRequest block:\n%s", v)
	}
	if !strings.Contains(v[i:], "ephemeral-storage: null") {
		t.Errorf("the chart's default ephemeral-storage request (3Gi) is kept; the lane's values must null it:\n%s", v)
	}
}
