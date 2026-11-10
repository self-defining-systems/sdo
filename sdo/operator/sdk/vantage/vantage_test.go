package vantage_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/podread"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/traffic"
	"sdo.dev/operator/sdk/vantage"
)

var t0 = time.Unix(1_700_000_000, 0).UTC()

var role = podread.Target{Workload: "Deployment/api"}

func spec(id string) sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: id, Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge, Interval: time.Minute,
		Watches: []sdk.WatchKind{{APIVersion: "v1", Kind: "Pod"}}}
}

func TestProbeValidateRefusesOpenEndedInput(t *testing.T) {
	ok := vantage.Probe{ID: "dial", Kind: vantage.TCPConnect, From: role, To: &vantage.Peer{Host: "db", Port: 5432}, Interval: time.Minute}
	if err := ok.Validate(); err != nil {
		t.Fatal(err)
	}
	for name, probe := range map[string]vantage.Probe{
		"unknown kind":     {ID: "x", Kind: "shell", From: role, Interval: time.Minute},
		"no role":          {ID: "x", Kind: vantage.Qdisc, Interval: time.Minute},
		"fast":             {ID: "x", Kind: vantage.Qdisc, From: role, Interval: time.Second},
		"peer on state":    {ID: "x", Kind: vantage.Qdisc, From: role, To: &vantage.Peer{Host: "db", Port: 1}, Interval: time.Minute},
		"shell host":       {ID: "x", Kind: vantage.TCPConnect, From: role, To: &vantage.Peer{Host: "db;rm -rf /", Port: 1}, Interval: time.Minute},
		"bad port":         {ID: "x", Kind: vantage.TCPConnect, From: role, To: &vantage.Peer{Host: "db", Port: 0}, Interval: time.Minute},
		"http path spaces": {ID: "x", Kind: vantage.HTTPGet, From: role, To: &vantage.Peer{Host: "db", Port: 80, Path: "/a b"}, Interval: time.Minute},
	} {
		if err := probe.Validate(); err == nil {
			t.Errorf("%s: accepted %+v", name, probe)
		}
	}
}

func TestCommandPassesThePeerAsArguments(t *testing.T) {
	args, err := vantage.Command(vantage.Probe{ID: "dial", Kind: vantage.TCPConnect, From: role, To: &vantage.Peer{Host: "db", Port: 5432}, Interval: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if args[0] != "sh" || args[1] != "-c" || strings.Contains(args[2], "db") || args[4] != "db" || args[5] != "5432" {
		t.Fatalf("peer must be positional, not spliced: %q", args)
	}
	for _, kind := range vantage.Kinds {
		probe := vantage.Probe{ID: "p", Kind: kind, From: role, Interval: time.Minute}
		if kind.Dials() {
			probe.To = &vantage.Peer{Host: "db", Port: 80, Path: "/"}
		}
		if _, err := vantage.Command(probe); err != nil {
			t.Errorf("%s: %v", kind, err)
		}
	}
}

func TestParseDials(t *testing.T) {
	out := "dial 0 1700000000 0 1700000000 2000 \n" +
		"dial 1 1700000001 0 1700000002 0 \n" +
		"dial 1 1700000003 0 1700000003 3000 nc: bad address 'db'\n"
	_, dials, err := vantage.Parse(vantage.TCPConnect, out, time.Unix(1700000010, 0))
	if err != nil {
		t.Fatal(err)
	}
	if len(dials) != 3 || !dials[0].OK || dials[0].Duration != 2*time.Millisecond {
		t.Fatalf("dials = %+v", dials)
	}
	if dials[1].OK || dials[1].Class != "timeout" || dials[2].Class != "transport_error" {
		t.Fatalf("failure classes = %+v", dials)
	}
	_, dials, err = vantage.Parse(vantage.HTTPGet, "dial 1 1700000000 0 1700000000 5000 wget: server returned error: HTTP/1.1 503 Service Unavailable\n", t0)
	if err != nil || dials[0].OK || dials[0].Status != "503" || dials[0].Class != "not_serving" {
		t.Fatalf("http dials = %+v, %v", dials, err)
	}
	if _, _, err := vantage.Parse(vantage.TCPConnect, "sh: nc: not found\n", t0); err == nil {
		t.Fatal("output without dials must be an error")
	}
}

func TestParseStateKinds(t *testing.T) {
	cases := []struct {
		kind  vantage.Kind
		out   string
		facts map[string]string
	}{
		{vantage.Qdisc, "qdisc noqueue 0: dev lo root refcnt 2\nqdisc prio 1: dev eth0 root refcnt 2 bands 3\nqdisc netem 30: dev eth0 parent 1:3 limit 1000 delay 300ms\n",
			map[string]string{vantage.FactNetemDevs: "eth0", vantage.FactShaping: "eth0", vantage.FactQdiscs: "lo:noqueue eth0:prio eth0:netem"}},
		{vantage.ProcLimits, "pid 1 server\nMax processes             63000                63000                processes\nMax open files            16                   1048576              files\nfds 15\npid 7 worker\nMax open files            1048576              1048576              files\nfds ?\n",
			map[string]string{vantage.FactNofileSoftMin: "16", vantage.FactNofileMinPID: "1", vantage.FactFDsMax: "15", vantage.FactFDUsePctMax: "93.8", vantage.FactFDsUnreadable: "1", vantage.FactNprocSoftMin: "63000"}},
		{vantage.ProcState, "pid 1 T server\npid 9 S worker\npid 12 Z defunct\n",
			map[string]string{vantage.FactStopped: "1", vantage.FactSleeping: "1", vantage.FactZombie: "1", vantage.FactStoppedPIDs: "1:server"}},
		{vantage.CgroupCPU, "== cpu.max\n5000 100000\n== cpu.stat\nusage_usec 9\nnr_periods 200\nnr_throttled 150\nthrottled_usec 9000000\n== cpu.pressure\nsome avg10=72.10 avg60=40.00 avg300=1.00 total=9\nfull avg10=70.00 avg60=1.00 avg300=0.00 total=9\n",
			map[string]string{vantage.FactCPULimitMillis: "50", vantage.FactCPUThrottledPct: "75.0", vantage.FactPressureSome10: "72.10"}},
		{vantage.CgroupMemory, "== memory.max\n104857600\n== memory.high\nmax\n== memory.current\n52428800\n== memory.events\nlow 0\nhigh 0\nmax 3\noom 1\noom_kill 1\n== memory.pressure\nsome avg10=0.00 avg60=0.00 avg300=0.00 total=0\n",
			map[string]string{vantage.FactMemoryMax: "104857600", vantage.FactMemoryUsePct: "50.0", vantage.FactOOMKill: "1"}},
		{vantage.CgroupIO, "== io.max\n8:0 rbps=1048576 wbps=1048576 riops=20 wiops=2\n== io.pressure\nsome avg10=1.00 avg60=0.00 avg300=0.00 total=0\nfull avg10=0.50 avg60=0.00 avg300=0.00 total=0\n",
			map[string]string{vantage.FactIOMax: "8:0 rbps=1048576 wbps=1048576 riops=20 wiops=2", vantage.FactPressureFull10: "0.50"}},
		{vantage.ShmUsage, "Filesystem           1K-blocks      Used Available Use% Mounted on\nshm                      65536     65536         0 100% /dev/shm\n",
			map[string]string{vantage.FactShmUsedPct: "100.0", vantage.FactShmSizeKB: "65536"}},
		{vantage.SocketSummary, "     60 7501F40A:1538 01\n      2 7501F40A:1538 08\n      1 00000000:0050 0A\n",
			map[string]string{"state.ESTABLISHED": "60", "state.LISTEN": "1", "peer.10.244.1.117:5432": "CLOSE_WAIT=2,ESTABLISHED=60", vantage.FactSockets: "63"}},
	}
	for _, c := range cases {
		facts, _, err := vantage.Parse(c.kind, c.out, t0)
		if err != nil {
			t.Errorf("%s: %v", c.kind, err)
			continue
		}
		for key, want := range c.facts {
			if facts[key] != want {
				t.Errorf("%s fact %s = %q, want %q (all %v)", c.kind, key, facts[key], want, facts)
			}
		}
	}
	if _, _, err := vantage.Parse(vantage.CgroupCPU, "== cpu.max\ncat: can't open '/proc/1/root/sys/fs/cgroup/cpu.max': Permission denied\n", t0); err == nil {
		t.Error("an unreadable cgroup must be an error, not an unlimited reading")
	}
}

func reading(kind vantage.Kind, facts map[string]string) vantage.Reading {
	return vantage.Reading{Namespace: "app", Pod: "api-abc-1", UID: "u1", Container: "api", ObservedAt: t0, Kind: kind, OK: true, Facts: facts}
}

func TestJudgeAgainstBaseline(t *testing.T) {
	cases := []struct {
		kind       vantage.Kind
		now, base  map[string]string
		want       vantage.Condition
		noBaseline bool
	}{
		{vantage.Qdisc, map[string]string{vantage.FactNetemDevs: "eth0"}, map[string]string{}, vantage.ConditionNetem, false},
		{vantage.ProcLimits, map[string]string{vantage.FactNofileSoftMin: "256"}, map[string]string{vantage.FactNofileSoftMin: "1048576"}, vantage.ConditionFDLimitLow, false},
		{vantage.ProcLimits, map[string]string{vantage.FactNofileSoftMin: "16"}, nil, vantage.ConditionFDLimitLow, true},
		{vantage.ProcState, map[string]string{vantage.FactStopped: "1"}, nil, vantage.ConditionStopped, true},
		{vantage.CgroupCPU, map[string]string{vantage.FactCPULimitMillis: "50", vantage.FactCPUMax: "5000 100000"}, map[string]string{vantage.FactCPUMax: "max 100000"}, vantage.ConditionCPULimited, false},
		{vantage.CgroupIO, map[string]string{vantage.FactIOMax: "8:0 riops=20"}, map[string]string{}, vantage.ConditionIOLimited, false},
		{vantage.ShmUsage, map[string]string{vantage.FactShmUsedPct: "100.0"}, nil, vantage.ConditionShmFull, true},
		{vantage.SocketSummary, map[string]string{"peer.10.0.0.9:5432": "ESTABLISHED=80"}, map[string]string{"peer.10.0.0.9:5432": "ESTABLISHED=4"}, vantage.ConditionSocketBuildup, false},
	}
	for _, c := range cases {
		var base vantage.Reading
		if !c.noBaseline {
			base = reading(c.kind, c.base)
		}
		got := vantage.Judge(reading(c.kind, c.now), base)
		if len(got) != 1 || got[0].Condition != c.want {
			t.Errorf("%s: judged %+v, want %s", c.kind, got, c.want)
		}
		if !c.noBaseline {
			if same := vantage.Judge(reading(c.kind, c.base), base); len(same) != 0 {
				t.Errorf("%s: the healthy baseline itself judged %+v", c.kind, same)
			}
		}
	}
}

func TestStateDetectorReportsPerPodAndErrsWithoutAnswer(t *testing.T) {
	probe := vantage.Probe{ID: "limits", Kind: vantage.ProcLimits, From: role, Interval: time.Minute}
	d, err := vantage.NewDetector(spec("process-runtime"), probe)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := sdktest.Snapshot{NamespaceName: "app"}
	if _, err := d.Detect(context.Background(), snapshot); err == nil {
		t.Fatal("an unobserved probe must be a detector error")
	}
	snapshot.Vantage = map[string]vantage.Result{"limits": {Probe: probe, Readings: []vantage.Reading{reading(vantage.ProcLimits, map[string]string{vantage.FactNofileSoftMin: "16", vantage.FactNofileMinPID: "1"})}}}
	findings, err := d.Detect(context.Background(), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 1 || findings[0].PrimaryResource.Name != "api-abc-1" || findings[0].RelatedResources[0].Name != "api" {
		t.Fatalf("findings = %+v", findings)
	}
	if _, err := vantage.NewDetector(spec("x"), vantage.Probe{ID: "d", Kind: vantage.TCPConnect, From: role, To: &vantage.Peer{Host: "db", Port: 1}, Interval: time.Minute}); err == nil {
		t.Fatal("a dialing probe belongs to the client link detector")
	}
}

func dials(start time.Time, n int, each time.Duration, ok bool) []vantage.Dial {
	var out []vantage.Dial
	for i := range n {
		out = append(out, vantage.Dial{At: start.Add(time.Duration(i) * time.Second), Duration: each, OK: ok})
	}
	return out
}

func TestClientLinkDetectorSeesClientEgressTheProberMisses(t *testing.T) {
	link := traffic.Link{From: "api", To: "db", Port: 5432}
	d, err := vantage.NewClientLinkDetector(spec("client-links"), "api-db", role, link, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	probe := d.(vantage.Consumer).VantageProbes()[0]
	slow := reading(vantage.TCPConnect, nil)
	// Slow must persist over traffic.LinkSlowWindows probes' worth of dials.
	slow.Dials = dials(t0.Add(-12*time.Second), traffic.LinkSlowWindows*vantage.DialsPerProbe, 300*time.Millisecond, true)
	healthy := reading(vantage.TCPConnect, nil)
	healthy.Dials = dials(t0.Add(-time.Hour), 4, 2*time.Millisecond, true)
	window := traffic.Window{Workload: traffic.Workload{Name: "links", Purpose: traffic.PurposeLinkProbe, Failures: 3}, ObservedAt: t0,
		Links: []traffic.LinkObservations{{Link: link, Qualified: true, BaselineLatency: 2 * time.Millisecond, Samples: []traffic.LinkSample{
			{At: t0.Add(-3 * time.Second), Duration: 2 * time.Millisecond, OK: true}, {At: t0.Add(-2 * time.Second), Duration: 2 * time.Millisecond, OK: true}, {At: t0.Add(-time.Second), Duration: 2 * time.Millisecond, OK: true},
		}}}}
	snapshot := sdktest.Snapshot{NamespaceName: "app", Traffic: map[string]traffic.Window{"links": window},
		Vantage: map[string]vantage.Result{"api-db": {Probe: probe, Readings: []vantage.Reading{slow}, Baseline: []vantage.Reading{healthy}}}}
	findings, err := d.Detect(context.Background(), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 1 {
		t.Fatalf("findings = %+v", findings)
	}
	f := findings[0]
	if f.Metadata["vantage"] != "client" || f.Metadata["condition"] != "slow" || f.Metadata["impairment"] != "client-egress" || f.Metadata["prober_condition"] != "healthy" {
		t.Fatalf("metadata = %v", f.Metadata)
	}
	if f.RuleID != traffic.LinkRuleIDPrefix+link.ID()+".client" {
		t.Fatalf("rule = %s", f.RuleID)
	}
	snapshot.Vantage["api-db"] = vantage.Result{Probe: probe, Readings: []vantage.Reading{healthy}, Baseline: []vantage.Reading{healthy}}
	healthyNow := healthy
	healthyNow.Dials = dials(t0.Add(-4*time.Second), 4, 2*time.Millisecond, true)
	snapshot.Vantage["api-db"] = vantage.Result{Probe: probe, Readings: []vantage.Reading{healthyNow}, Baseline: []vantage.Reading{healthy}}
	if findings, err := d.Detect(context.Background(), snapshot); err != nil || len(findings) != 0 {
		t.Fatalf("healthy client dials reported %+v, %v", findings, err)
	}
}

func TestWithHistoryKeepsALossWindow(t *testing.T) {
	previous := reading(vantage.TCPConnect, nil)
	previous.Dials = dials(t0, vantage.MaxDialHistory, time.Millisecond, true)
	current := reading(vantage.TCPConnect, nil)
	current.Dials = dials(t0.Add(time.Minute), 4, time.Second, false)
	merged := vantage.WithHistory(previous, current)
	if len(merged.Dials) != vantage.MaxDialHistory || merged.Dials[len(merged.Dials)-1].OK {
		t.Fatalf("history = %d dials", len(merged.Dials))
	}
	other := current
	other.UID = "u2"
	if got := vantage.WithHistory(previous, other); len(got.Dials) != 4 {
		t.Fatal("a replaced Pod must not inherit another Pod's dials")
	}
}
