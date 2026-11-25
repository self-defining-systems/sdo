//go:build integration

package bench

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Option eks.one_node_lanes: a vcluster lane on one node of the type its two-node layout uses.

func oneNodeInv(t *testing.T) Inventory {
	t.Helper()
	inv, err := loadInv(t, strings.Replace(vcEksHost, "full_lanes: [full1]", "full_lanes: [full1], one_node_lanes: [vc1]", 1))
	if err != nil {
		t.Fatal(err)
	}
	return inv
}

func TestEksOneNodeLaneIsOneNodeOfTheTwoNodeType(t *testing.T) {
	e := oneNodeInv(t).Hosts["v"].EKS
	gitlab := LaneLimit{CPUs: 8, Memory: "30Gi"}
	two, err := e.PlanLaneFor("vc2", gitlab)
	must(t, err)
	one, err := e.PlanLaneFor("vc1", gitlab)
	must(t, err)
	if two.InstanceType != "m6i.2xlarge" || two.Nodes != 2 || two.VCPUs != 16 || two.OneNode {
		t.Fatalf("two-node plan %+v", two)
	}
	if one.InstanceType != two.InstanceType || one.Nodes != 1 || one.VCPUs != 8 || one.USDPerHour != 0.384 || !one.OneNode || !one.Virtual {
		t.Fatalf("one-node plan %+v, want 1 x m6i.2xlarge, 8 vCPUs, $0.384/h", one)
	}
	if env := strings.Join(eksEnv(oneNodeInv(t).Hosts["v"], one, "/st"), " "); !strings.Contains(env, "EKS_NODES=1") || !strings.Contains(env, "EKS_NODE_VCPUS=8") {
		t.Errorf("env %q", env)
	}
	// memory is not compressible: a footprint one node cannot hold is refused, not squeezed
	if _, err := e.PlanLaneFor("vc1", LaneLimit{CPUs: 8, Memory: "40Gi"}); err == nil || !strings.Contains(err.Error(), "one_node_lanes") {
		t.Errorf("a footprint over one node: %v", err)
	}
}

// A one-node lane is taken only by a spec that asks for it, so no row lands on one by accident.
func TestEksOneNodeLaneIsTakenOnlyBySelector(t *testing.T) {
	inv := oneNodeInv(t)
	if got := labelsText(inv.Hosts["v"].LaneLabels["vc1"]); got != "kube-system=virtual,lane-nodes=1,node-access=virtual" {
		t.Errorf("labels %q", got)
	}
	ask := Spec{Selector: map[string]string{LabelLaneNodes: "1"}}
	if inv.LaneAccepts("v", "vc1", Spec{}) || !inv.LaneAccepts("v", "vc1", ask) || inv.LaneAccepts("v", "vc2", ask) || !inv.LaneAccepts("v", "vc2", Spec{}) {
		t.Error("lane selection is wrong")
	}
	// a lane with labels of its own has the label too
	own := strings.Replace(vcEksHost, "full_lanes: [full1]", "full_lanes: [full1], one_node_lanes: [vc1]", 1) + "    lane_labels:\n      vc1: {row: a}\n"
	inv, err := loadInv(t, own)
	must(t, err)
	if got := labelsText(inv.Hosts["v"].LaneLabels["vc1"]); got != "lane-nodes=1,row=a" {
		t.Errorf("labels %q", got)
	}
}

func TestEksOneNodeLanesAreValidated(t *testing.T) {
	with := func(v string) string {
		return strings.Replace(vcEksHost, "full_lanes: [full1]", "full_lanes: [full1], one_node_lanes: ["+v+"]", 1)
	}
	for y, want := range map[string]string{
		with("nope"):  "one_node_lanes names \"nope\", which is not in clusters",
		with("full1"): "one_node_lanes names \"full1\", a full-cluster lane",
		strings.Replace(with("vc1"), "lanes: vcluster, full_lanes: [full1], ", "", 1): "one_node_lanes belongs to a host with lanes: vcluster",
		with("vc1") + "    lane_labels:\n      vc1: {lane-nodes: \"2\"}\n":            "lane-nodes",
		with("vc1") + "    lane_labels:\n      vc2: {lane-nodes: \"1\"}\n":            "lane-nodes",
	} {
		if _, err := loadInv(t, y); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("want %q, got %v", want, err)
		}
	}
}

// What a lane was made with (not what hosts.yaml says now) is its ledger entry, its status and its rows' record; a lane
// whose layout is no longer the one asked for takes no new build and is deleted when free.
func TestEksLaneLayoutIsAccountedRecordedAndKept(t *testing.T) {
	m := vcManager(t)
	m.inv = oneNodeInv(t)
	must(t, os.MkdirAll(filepath.Dir(m.Root.eksHostRecordPath("v")), 0o755))
	must(t, writeJSON(m.Root.eksHostRecordPath("v"), eksHostRecord{Cluster: "lanes-host-v", Since: m.now()}))
	m.eksSettle()
	gl := Spec{Lane: &LaneLimit{CPUs: 8, Memory: "30Gi"}}
	x := &Experiment{Name: "x", Spec: &gl}
	m.exps = map[string]*Experiment{"x": x}
	h := m.inv.Hosts["v"]
	h.EKS.VCPUQuota = 12 // system node 4 + one 8 vCPU lane
	if !m.eksLaneAdmitted(m.lanes["vc1"], gl) || m.eksLaneAdmitted(m.lanes["vc2"], gl) {
		t.Error("a one-node lane counts 8 vCPUs, a two-node lane 16")
	}
	for _, c := range []string{"vc1", "vc2"} {
		m.lanes[c].Gone, m.lanes[c].Experiment = false, "x"
	}
	m.eksSettle()
	if st := m.eksStatus("v"); st.VCPUs != 4+8+16 {
		t.Errorf("vCPUs %d, want 28", st.VCPUs)
	}
	for lane, want := range map[string]string{"vc1": "1 x m6i.2xlarge", "vc2": "2 x m6i.2xlarge"} {
		l := m.lanes[lane]
		l.Seq = &Seq{Q: 1}
		rf := m.runFile(x, l, &RunSpec{ID: "r", Fault: "f", Position: 1}, Result{}, StatusDone, "")
		if l.Layout.String() != want || rf.Layout.String() != want {
			t.Errorf("%s: status %q, row %q, want %q", lane, l.Layout.String(), rf.Layout.String(), want)
		}
	}
	// hosts.yaml changes its mind while vc1 exists
	l := m.lanes["vc1"]
	l.State, l.Since = LaneFree, m.now()
	h.EKS.OneNodeLanes = nil
	if p, _ := m.eksLanePlan(l, gl); p.Nodes != 1 {
		t.Errorf("an existing lane changed its layout: %+v", p)
	}
	if m.eksLaneAdmitted(l, gl) || !m.eksIdle(l) {
		t.Error("a lane of another layout must take no new build and be deleted when free")
	}
	l.Gone = true
	m.eksSettle()
	if l.Layout != nil {
		t.Errorf("a deleted lane has no layout: %v", l.Layout)
	}
	if p, _ := m.eksLanePlan(l, gl); p.Nodes != 2 {
		t.Errorf("the next cluster ignores hosts.yaml: %+v", p)
	}
}

// The script with one node: one pause pod, a NodePool capped at one node, and the node is both infra node and worker.
func TestEksVclusterLaneUpWithOneNode(t *testing.T) {
	s := newVcScript(t)
	s.hostUp(t)
	out, err := s.run([]string{"EKS_NODES=1", "EKS_NODE_VCPUS=8", "EKS_INSTANCE_TYPE=m6i.2xlarge"}, "up", "vc1", filepath.Join(t.TempDir(), "kube", "vc1"))
	if err != nil || !strings.Contains(out, "EKS_LANE_READY cluster=vc1 host=lanes-host-v instance=m6i.2xlarge nodes=1") {
		t.Fatalf("up: %v\n%s", err, out)
	}
	applied := s.file("applied.yaml")
	for _, want := range []string{`limits: {cpu: "8"}`, "replicas: 1", "values: [m6i.2xlarge]"} {
		if !strings.Contains(applied, want) {
			t.Errorf("lane manifests lack %q", want)
		}
	}
}
