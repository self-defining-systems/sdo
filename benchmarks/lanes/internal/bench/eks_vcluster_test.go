//go:build integration

package bench

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// Provider eks with `lanes: vcluster`: one long-lived EKS host cluster, a vcluster and a Karpenter NodePool per lane.
// Everything here runs against fake aws, eksctl, kubectl, helm and vcluster (testdata/fakeeks) and a fake clock.

const vcEksHost = `hosts:
  v:
    local: true
    provider: eks
    eks: {region: us-west-2, account: "123456789012", lanes: vcluster, full_lanes: [full1], budget_per_hour: 3.0, budget_total: 10, vcpu_quota: 40, max_lanes: 4}
    staging: /s
    kubeconfigs: /k
    evidence: /e
    max_builds: 2
    clusters: [vc1, vc2, vc3, full1]
`

func TestEksVclusterHostDefaultsItsCommandsEnvAndLabels(t *testing.T) {
	inv, err := loadInv(t, vcEksHost)
	if err != nil {
		t.Fatal(err)
	}
	v := inv.Hosts["v"]
	if !v.IsEksVirtual() || !strings.HasPrefix(v.ClusterCommand(v.ClusterUp, "vc1"), "$LANES_INSTALL/deploy/eks-vcluster.sh up 'vc1' '/k/vc1' '/e/vc1'") ||
		!strings.HasPrefix(v.ClusterCommand(v.ClusterDown, "vc1"), "$LANES_INSTALL/deploy/eks-vcluster.sh down 'vc1'") {
		t.Fatalf("cluster commands %q %q", v.ClusterUp, v.ClusterDown)
	}
	env := strings.Join(v.EnvList(), " ")
	for _, want := range []string{"BENCH_LANE_PROVIDER=eks", "EKS_HOST_CLUSTER=lanes-host-v", "EKS_HOST_KUBECONFIG=/k/.host/admin"} {
		if !strings.Contains(env, want) {
			t.Errorf("env %q lacks %s", env, want)
		}
	}
	// a full-cluster lane of the same host is made by the cluster script, with a cluster and a control plane of its own
	if got := v.ClusterCommand(v.ClusterUp, "full1"); !strings.HasPrefix(got, "$LANES_INSTALL/deploy/eks-lane.sh up 'full1' '/k/full1'") {
		t.Errorf("full-cluster lane command %q", got)
	}
	if v.EKS.virtualLane("full1") || !v.EKS.virtualLane("vc1") || !v.RecreatesAfterFailure("vc1") || v.RecreatesAfterFailure("full1") {
		t.Error("lane kinds / recreate-after-failure wrong")
	}
	if p, _ := v.EKS.PlanLaneFor("full1", LaneLimit{CPUs: 5, Memory: "12Gi"}); p.Virtual || p.USDPerHour != 0.484 {
		t.Errorf("a full-cluster lane pays for its control plane: %+v", p)
	}
	// lanes say what they have: a vcluster has no real kube-system and no node access
	if got := labelsText(v.LaneLabels["vc2"]); got != "kube-system=virtual,node-access=virtual,shared=true" {
		t.Errorf("vcluster lane labels = %q", got)
	}
	if got := labelsText(v.LaneLabels["full1"]); got != "kube-system=real,node-access=real,shared=true" {
		t.Errorf("full-cluster lane labels = %q", got)
	}
	if _, err := loadInv(t, strings.Replace(vcEksHost, "lanes: vcluster", "lanes: pods", 1)); err == nil || !strings.Contains(err.Error(), `eks.lanes "pods"`) {
		t.Errorf("an unknown eks.lanes: %v", err)
	}
	if _, err := loadInv(t, strings.Replace(vcEksHost, "lanes: vcluster, ", "", 1)); err == nil || !strings.Contains(err.Error(), "eks.full_lanes names the full-cluster lanes of a host with lanes: vcluster") {
		t.Errorf("full_lanes on a cluster host: %v", err)
	}
}

// A lane that needs a real kube-system or node access (the fault matrices) cannot be on a vcluster: it is refused at add
// time with the labels every lane has, or routed to the full-cluster host; a spec with no selector takes either.
func TestEksVclusterGatesSpecsThatNeedTheRealKubeSystemOrNodes(t *testing.T) {
	inv, err := loadInv(t, vcEksHost+"checkouts:\n  fuzzops: {abc: {v: /fz}}\n")
	if err != nil {
		t.Fatal(err)
	}
	need := Spec{Hosts: []string{"v"}, Fuzzops: "abc", Selector: map[string]string{LabelNodeAccess: CapReal}}
	for _, lane := range []string{"vc1", "vc2", "vc3"} {
		if inv.LaneAccepts("v", lane, need) {
			t.Errorf("vcluster lane %s takes a node-access spec", lane)
		}
	}
	if !inv.LaneAccepts("v", "full1", need) || len(inv.SelectorProblems(need)) != 0 {
		t.Error("the full-cluster lane does not take it, or the spec was refused although it can run there")
	}
	plain := Spec{Hosts: []string{"v"}, Fuzzops: "abc"}
	for _, lane := range []string{"vc1", "full1"} {
		if !inv.LaneAccepts("v", lane, plain) {
			t.Errorf("a spec with no selector must take lane %s", lane)
		}
	}
	// without a full-cluster lane, a spec that needs the real ones is refused at add time, with the reason
	only := strings.Replace(strings.Replace(strings.Replace(vcEksHost, "full_lanes: [full1], ", "", 1), ", full1]", "]", 1), "max_lanes: 4", "max_lanes: 3", 1)
	inv2, err := loadInv(t, only+"checkouts:\n  fuzzops: {abc: {v: /fz}}\n")
	if err != nil {
		t.Fatal(err)
	}
	problems := inv2.SelectorProblems(need)
	if len(problems) != 1 || !strings.Contains(problems[0], "host v: no lane has labels satisfying the selector [node-access=real]") ||
		!strings.Contains(problems[0], "vclusters with no real kube-system and no node access") {
		t.Fatalf("a spec for node access on a vcluster-only host: %v", problems)
	}
}

func TestEksVclusterLaneCostsItsNodesOnly(t *testing.T) {
	e := &HostEKS{Region: "r", BudgetPerHour: 9, VCPUQuota: 99, Lanes: EksLanesVcluster}
	p, err := e.PlanLane(LaneLimit{CPUs: 5, Memory: "12Gi"})
	if err != nil || p.InstanceType != "m6i.xlarge" || p.Nodes != 2 || p.USDPerHour != 0.384 {
		t.Fatalf("plan = %+v (%v), want 2 x m6i.xlarge at $0.384/h (no control plane of its own)", p, err)
	}
	if n, usd := e.systemNode(); n != 4 || usd != 0.192 {
		t.Errorf("system node = %d vCPUs $%.3f/h", n, usd)
	}
}

func vcManager(t *testing.T) *Manager {
	t.Helper()
	root := Root(t.TempDir())
	must(t, os.WriteFile(root.path("hosts.yaml"), []byte(vcEksHost+"checkouts:\n  fuzzops: {abc: {v: /fz}}\n"), 0o644))
	now := time.Date(2026, 11, 21, 8, 0, 0, 0, time.UTC)
	m := &Manager{Root: root, Now: func() time.Time { return now }}
	must(t, m.loadInventory())
	m.lanes = map[string]*LaneFile{}
	for _, c := range []string{"vc1", "vc2", "vc3"} {
		m.lanes[c] = &LaneFile{Lane: c, Host: "v", State: LaneFree, Gone: true, Since: now}
	}
	return m
}

// The host cluster is paid for whether or not a lane exists (its control plane), plus its system node while a lane does;
// a lane is held until the host cluster is adopted, and admission counts the host with it.
func TestEksVclusterAdmissionAndSpendIncludeTheHostCluster(t *testing.T) {
	m := vcManager(t)
	h := m.inv.Hosts["v"]
	p, _ := h.EKS.PlanLane(LaneLimit{CPUs: 5, Memory: "12Gi"}) // 8 vCPUs, $0.384/h
	if why := m.eksAdmit("v", p); !strings.Contains(why, "host cluster lanes-host-v is not adopted: run `lanes host adopt v`") {
		t.Fatalf("an unadopted host admitted a lane: %q", why)
	}
	must(t, os.MkdirAll(filepath.Dir(m.Root.eksHostRecordPath("v")), 0o755))
	must(t, writeJSON(m.Root.eksHostRecordPath("v"), eksHostRecord{Cluster: "lanes-host-v", Since: m.now().Add(-10 * time.Hour)}))
	m.eksSettle()
	if why := m.eksAdmit("v", p); why != "" {
		t.Fatalf("an adopted host holds a lane: %s", why)
	}
	if st := m.eksStatus("v"); st.USDPerHour != 0.1 || st.VCPUs != 0 || st.SpendUSD != 1.0 {
		t.Fatalf("idle host status = %+v, want its control plane only: $0.10/h, 0 vCPUs, $1.00 spent in 10 h", st)
	}
	// a lane exists: system node ($0.192/h, 4 vCPUs) + two nodes ($0.384/h, 8 vCPUs)
	m.lanes["vc1"].Gone = false
	m.eksSettle()
	m.advance(2 * time.Hour)
	m.eksSettle()
	st := m.eksStatus("v")
	if st.Lanes != 1 || st.VCPUs != 12 || st.USDPerHour != 0.676 {
		t.Fatalf("status with a lane = %+v, want 1 lane, 12 vCPUs, $0.676/h", st)
	}
	// 12 h of control plane + 2 h of (system node + lane) = 1.2 + 0.384 + 0.768 = 2.352
	if st.SpendUSD != 2.352 {
		t.Errorf("spend = $%.4f, want 2.352", st.SpendUSD)
	}
	if s := st.String(); !strings.Contains(s, "eks (vcluster lanes, host included): 1/4 lanes, 12/40 vCPU") {
		t.Errorf("status line %q", s)
	}
	m.inv.Hosts["v"].EKS.VCPUQuota = 20
	big, _ := h.EKS.PlanLane(LaneLimit{CPUs: 16, Memory: "64Gi"}) // 16 vCPUs
	if why := m.eksAdmit("v", big); !strings.Contains(why, "vCPU quota (12 of 20") {
		t.Errorf("12 + 16 vCPUs over 20: %q", why)
	}
	m.inv.Hosts["v"].EKS.VCPUQuota = 40
	m.inv.Hosts["v"].EKS.BudgetPerHour = 1.0 // lane $0.384 + host $0.292 + a second lane $0.384 > $1.00
	if why := m.eksAdmit("v", p); !strings.Contains(why, "does not fit the budget ($0.68/h of $1.00/h in use)") {
		t.Errorf("$/h budget with the host: %q", why)
	}
	// the last lane going: only the control plane bills again
	m.lanes["vc1"].Gone = true
	m.eksSettle()
	if st := m.eksStatus("v"); st.Lanes != 0 || st.USDPerHour != 0.1 {
		t.Errorf("status after the last lane = %+v", st)
	}
}

// `lanes host status` reads the same ledger and adds the host cluster, as the manager's status line does.
func TestEksVclusterHostStatusIncludesTheHostCluster(t *testing.T) {
	m := vcManager(t)
	must(t, os.MkdirAll(filepath.Dir(m.Root.eksHostRecordPath("v")), 0o755))
	must(t, writeJSON(m.Root.eksHostRecordPath("v"), eksHostRecord{Cluster: "lanes-host-v", Since: time.Now().Add(-time.Hour)}))
	must(t, writeJSON(m.Root.eksLedgerPath("v"), eksLedger{Open: map[string]eksOpen{"vc1": {Since: time.Now().Add(-time.Hour), EksPlan: EksPlan{InstanceType: "m6i.xlarge", Nodes: 2, VCPUs: 8, USDPerHour: 0.384, Virtual: true}}}}))
	var out bytes.Buffer
	statusEksHost(context.Background(), m.Root, m.inv, "v", nil, &out)
	if !strings.Contains(out.String(), "eks (vcluster lanes, host included): 1/4 lanes, 12/40 vCPU, $0.68/h of $3.00/h") {
		t.Errorf("host status:\n%s", out.String())
	}
}

// Every release that deletes a vcluster lane may ask the host to scale to zero: the host decides under its lock, from the
// lanes it holds. A ledger read when each step starts cannot: releases that run together each see the others' lanes, and
// the system node stayed up with no lane while the ledger billed one control plane.
func TestEksVclusterEveryClusterDeletingReleaseMayAskTheHostToScaleToZero(t *testing.T) {
	m := vcManager(t)
	spy := &startSpy{}
	m.Ops = func(string, Host) HostOps { return spy }
	l := m.eksLedger("v")
	l.Open["vc1"] = eksOpen{Since: m.now(), EksPlan: EksPlan{Virtual: true}}
	l.Open["vc2"] = eksOpen{Since: m.now(), EksPlan: EksPlan{Virtual: true}}
	for _, lane := range []string{"vc1", "vc2"} {
		m.lanes[lane].Gone = false
		if !m.start(context.Background(), m.lanes[lane], Step{Kind: StepRelease, DeleteCluster: true}) {
			t.Fatalf("release of %s was refused", lane)
		}
	}
	if len(spy.started) != 2 {
		t.Fatalf("started %d steps", len(spy.started))
	}
	for _, s := range spy.started {
		if !slices.Contains(s.HostEnv, "EKS_HOST_LAST=1") {
			t.Errorf("the release of %s lacks EKS_HOST_LAST=1 while another lane was open: %v", s.Lane, s.HostEnv)
		}
	}
}

// `lanes host adopt` makes the host cluster once through the script and records when it began to bill; adopting again
// keeps that date.
func TestEksVclusterHostAdoptRunsHostUpAndRecordsTheDate(t *testing.T) {
	root := t.TempDir()
	staging := t.TempDir()
	body := strings.Replace(vcEksHost, "staging: /s\n", "staging: "+staging+"\n", 1)
	must(t, os.WriteFile(filepath.Join(root, "hosts.yaml"), []byte(body+"checkouts:\n  fuzzops: {abc: {v: /fz}}\n"), 0o644))
	inv, err := LoadInventory(root)
	must(t, err)
	var scripts []string
	run := func(ctx context.Context, host, script string) (string, error) {
		scripts = append(scripts, script)
		return "EKS_HOST_READY cluster=lanes-host-v created=1\n", nil
	}
	var out, errw bytes.Buffer
	if rc := HostCommand(context.Background(), Root(root), []string{"adopt", "v"}, &out, &errw, nil, run); rc != 0 {
		t.Fatalf("adopt rc=%d\n%s%s", rc, out.String(), errw.String())
	}
	if len(scripts) != 1 || !strings.Contains(scripts[0], "export EKS_HOST_CLUSTER='lanes-host-v'") || !strings.Contains(scripts[0], "eks-vcluster.sh\" host-up") ||
		!strings.Contains(scripts[0], "export LANES_INSTALL=") {
		t.Fatalf("script %v", scripts)
	}
	// the account is checked by host-up like by every lane step: without it the host cluster would be made in whatever
	// account the shell's credentials belong to
	if !strings.Contains(scripts[0], "export EKS_ACCOUNT='123456789012'") {
		t.Errorf("adopt does not carry EKS_ACCOUNT: %s", scripts[0])
	}
	var rec eksHostRecord
	must(t, readJSON(Root(root).eksHostRecordPath("v"), &rec))
	if rec.Cluster != "lanes-host-v" || rec.Since.IsZero() {
		t.Fatalf("record %+v", rec)
	}
	first := rec.Since
	time.Sleep(10 * time.Millisecond)
	if rc := HostCommand(context.Background(), Root(root), []string{"adopt", "v"}, &out, &errw, nil, run); rc != 0 {
		t.Fatalf("second adopt rc=%d", rc)
	}
	must(t, readJSON(Root(root).eksHostRecordPath("v"), &rec))
	if !rec.Since.Equal(first) {
		t.Errorf("adopting again moved the billing date %v -> %v", first, rec.Since)
	}
	_ = inv
}

// Steps of a vcluster host run helm, the vcluster CLI, flock and curl: a host without them fails its first `up` half-made.
func TestEksVclusterHostToolsIncludeWhatTheLaneScriptRuns(t *testing.T) {
	m := vcManager(t)
	got := platformTools(m.inv.Hosts["v"])
	for _, want := range []string{"kubectl", "aws", "eksctl", "helm", "vcluster", "flock", "curl"} {
		if !slices.Contains(got, want) {
			t.Errorf("tools %v lack %s", got, want)
		}
	}
}

// A lane retired after two automatic resets, or one that left hosts.yaml, whose cluster is not shown gone is asked to
// delete it again; its ledger entry closes once its retired file says the cluster is gone.
func TestEksRetiredLaneWithALiveClusterIsStillDeleted(t *testing.T) {
	m := vcManager(t)
	l := m.lanes["vc1"]
	l.State, l.Gone = LaneRetired, false
	if got := m.eksOrphanLanes(); len(got) != 1 || got[0].Lane != "vc1" {
		t.Fatalf("orphans = %v", got)
	}
	l.Gone = true
	if got := m.eksOrphanLanes(); len(got) != 0 {
		t.Fatalf("a deleted lane is an orphan: %v", got)
	}
}

func TestEksLedgerClosesALaneRetiredAfterItsClusterWasDeleted(t *testing.T) {
	m := vcManager(t)
	delete(m.lanes, "vc3")
	must(t, os.MkdirAll(m.Root.path("lanes", "retired"), 0o755))
	must(t, writeJSON(m.Root.path("lanes", "retired", "vc3.json"), LaneFile{Lane: "vc3", Host: "v", State: LaneRetired, Gone: true}))
	l := m.eksLedger("v")
	l.Open["vc3"] = eksOpen{Since: m.now().Add(-time.Hour), EksPlan: EksPlan{InstanceType: "m6i.xlarge", Nodes: 2, USDPerHour: 0.4, Virtual: true}}
	m.eksSettle()
	if _, open := m.eksLedger("v").Open["vc3"]; open {
		t.Error("a retired lane whose cluster is gone still counts against the host's lanes and budget")
	}
	// retired with its cluster left: the entry stays open, and billing
	must(t, writeJSON(m.Root.path("lanes", "retired", "vc4.json"), LaneFile{Lane: "vc4", Host: "v", State: LaneRetired, Gone: false}))
	m.eksLedger("v").Open["vc4"] = eksOpen{Since: m.now().Add(-time.Hour), EksPlan: EksPlan{Virtual: true}}
	m.eksSettle()
	if _, open := m.eksLedger("v").Open["vc4"]; !open {
		t.Error("a lane retired with a live cluster was closed in the ledger: its cost would vanish")
	}
}

// An eks lane that left hosts.yaml with a cluster but no recorded build still gets its cluster deleted.
func TestEksLeftLaneWithNoPreparedStateStillHasItsClusterDeleted(t *testing.T) {
	m := vcManager(t)
	lf := &LaneFile{Lane: "vcX", Host: "v", Gone: false}
	if !m.leftHasCluster(lf) {
		t.Error("a left eks lane with a cluster and no prepared state is retired with its cluster alive")
	}
	lf.Gone = true
	if m.leftHasCluster(lf) {
		t.Error("a deleted cluster is deleted again")
	}
}
