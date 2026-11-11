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
		"no role":          {ID: "x", Kind: vantage.TCPConnect, To: &vantage.Peer{Host: "db", Port: 1}, Interval: time.Minute},
		"fast":             {ID: "x", Kind: vantage.TCPConnect, From: role, To: &vantage.Peer{Host: "db", Port: 1}, Interval: time.Second},
		"no peer":          {ID: "x", Kind: vantage.TCPConnect, From: role, Interval: time.Minute},
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

func reading(kind vantage.Kind, facts map[string]string) vantage.Reading {
	return vantage.Reading{Namespace: "app", Pod: "api-abc-1", UID: "u1", Container: "api", ObservedAt: t0, Kind: kind, OK: true, Facts: facts}
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

// Several edges under one detector: each is dialed from its own client role,
// and a client-side loss on one edge is reported for that edge only.
func TestClientLinksDetectorDialsEachEdgeFromItsClient(t *testing.T) {
	db := traffic.Link{From: "api", To: "db", Port: 5432}
	cache := traffic.Link{From: "api", To: "cache", Port: 6379}
	d, err := vantage.NewClientLinksDetector(spec("client-links"), []vantage.ClientLink{{ID: "api.db.5432", From: role, Link: db}, {ID: "api.cache.6379", From: role, Link: cache}}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	probes := d.(vantage.Consumer).VantageProbes()
	if len(probes) != 2 || probes[0].To.Host != "db" || probes[1].To.Host != "cache" {
		t.Fatalf("probes = %+v", probes)
	}
	healthy := reading(vantage.TCPConnect, nil)
	healthy.Dials = dials(t0.Add(-4*time.Second), 4, 2*time.Millisecond, true)
	lost := reading(vantage.TCPConnect, nil)
	lost.Dials = dials(t0.Add(-4*time.Second), vantage.DialsPerProbe, time.Second, false)
	snapshot := sdktest.Snapshot{NamespaceName: "app", Vantage: map[string]vantage.Result{
		"api.db.5432":    {Probe: probes[0], Readings: []vantage.Reading{lost}, Baseline: []vantage.Reading{healthy}},
		"api.cache.6379": {Probe: probes[1], Readings: []vantage.Reading{healthy}, Baseline: []vantage.Reading{healthy}},
	}}
	findings, err := d.Detect(context.Background(), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 1 || !strings.Contains(findings[0].RuleID, db.ID()) || findings[0].Metadata["vantage"] != "client" {
		t.Fatalf("findings = %+v", findings)
	}
}
