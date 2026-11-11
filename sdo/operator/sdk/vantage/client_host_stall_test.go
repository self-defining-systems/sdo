package vantage_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/traffic"
	"sdo.dev/operator/sdk/vantage"
)

// A client dial records the CPU stall of the node it ran on, as a prober dial
// does, so host load is judged the same way on both vantages.
func TestClientDialsRecordTheirHostsCPUStall(t *testing.T) {
	for _, kind := range []vantage.Kind{vantage.TCPConnect, vantage.HTTPGet} {
		args, err := vantage.Command(vantage.Probe{ID: "dial", Kind: kind, From: role, To: &vantage.Peer{Host: "db", Port: 80, Path: "/"}, Interval: time.Minute})
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(args[2], "/proc/pressure/cpu") {
			t.Fatalf("%s dials do not read the host's CPU pressure: %s", kind, args[2])
		}
	}
	out := "dial 0 1700000000 0 1700000000 2000 \n" +
		"psi 41.50\n" +
		"dial 1 1700000001 0 1700000002 0 \n" +
		"dial 0 1700000003 0 1700000003 3000 \n" +
		"psi 0.00\n"
	_, dials, err := vantage.Parse(vantage.TCPConnect, out, time.Unix(1700000010, 0))
	if err != nil {
		t.Fatal(err)
	}
	if len(dials) != 3 || dials[0].HostCPUStall != 41.5 || dials[1].HostCPUStall != 0 || dials[2].HostCPUStall != 0 {
		t.Fatalf("dials = %+v", dials)
	}
	r := reading(vantage.TCPConnect, nil)
	r.Dials = dials
	links := vantage.ClientLinks(vantage.Result{Readings: []vantage.Reading{r}}, traffic.Link{From: "api", To: "db", Port: 5432})
	if len(links) != 1 || links[0].Samples[0].HostCPUStall != 41.5 {
		t.Fatalf("client samples = %+v", links)
	}
	if _, _, err := vantage.Parse(vantage.TCPConnect, "dial 0 1700000000 0 1700000000 2000 \npsi lots\n", t0); err == nil {
		t.Fatal("a malformed stall line parsed")
	}
}

// clientLinkFindings dials db from api with every dial timing out on a host
// whose CPU stall is stall, while the prober reaches db.
func clientLinkFindings(t *testing.T, stall float64) []sdk.Finding {
	t.Helper()
	link := traffic.Link{From: "api", To: "db", Port: 5432}
	d, err := vantage.NewClientLinkDetector(spec("client-links"), "api-db", role, link, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	probe := d.(vantage.Consumer).VantageProbes()[0]
	healthy := reading(vantage.TCPConnect, nil)
	healthy.Dials = dials(t0.Add(-time.Hour), 4, 2*time.Millisecond, true)
	cut := reading(vantage.TCPConnect, nil)
	for i := range vantage.DialsPerProbe {
		cut.Dials = append(cut.Dials, vantage.Dial{At: t0.Add(time.Duration(i-vantage.DialsPerProbe) * time.Second), Duration: vantage.DialTimeout, Class: string(traffic.LinkFailureTimeout), Error: "no connection", HostCPUStall: stall})
	}
	window := traffic.Window{Workload: traffic.Workload{Name: "links", Purpose: traffic.PurposeLinkProbe, Failures: 3}, ObservedAt: t0,
		Links: []traffic.LinkObservations{{Link: link, Qualified: true, BaselineLatency: 2 * time.Millisecond, Samples: []traffic.LinkSample{
			{At: t0.Add(-3 * time.Second), Duration: 2 * time.Millisecond, OK: true}, {At: t0.Add(-2 * time.Second), Duration: 2 * time.Millisecond, OK: true}, {At: t0.Add(-time.Second), Duration: 2 * time.Millisecond, OK: true},
		}}}}
	snapshot := sdktest.Snapshot{NamespaceName: "app", Traffic: map[string]traffic.Window{"links": window},
		Vantage: map[string]vantage.Result{"api-db": {Probe: probe, Readings: []vantage.Reading{cut}, Baseline: []vantage.Reading{healthy}}}}
	findings, err := d.Detect(context.Background(), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	return findings
}

// A client-side partition (every dial from the client times out while the
// prober reaches the callee) is reported when the client's host is calm.
func TestClientSidePartitionIsReportedOnACalmHost(t *testing.T) {
	findings := clientLinkFindings(t, 0)
	if len(findings) != 1 || findings[0].Metadata["condition"] != string(traffic.LinkUnreachable) || findings[0].Metadata["impairment"] != "client-egress" {
		t.Fatalf("client-side partition on a calm host: %+v", findings)
	}
}

// A short run of client dial timeouts on a stalled host is the host's load,
// not the link: host load is one input on both vantages.
func TestStalledClientHostExplainsAShortRunOfTimeouts(t *testing.T) {
	if findings := clientLinkFindings(t, traffic.HostStallBound+10); len(findings) != 0 {
		t.Fatalf("timeouts on a stalled client host reported: %+v", findings)
	}
}
