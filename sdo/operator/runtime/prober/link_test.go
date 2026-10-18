package prober_test

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"sdo.dev/operator/runtime/prober"
	"sdo.dev/operator/sdk/traffic"
)

func linkDocuments() map[string][]byte {
	return map[string][]byte{
		"links": []byte(`{"apiVersion": "sdo.dev/v1alpha1", "kind": "TrafficWorkload", "name": "links", "purpose": "link-probe",
			"interval": "100ms", "timeout": "50ms", "failures": 3,
			"links": [{"from": "web", "to": "backend", "port": 9001}, {"from": "web", "to": "cache", "port": 9002}]}`),
	}
}

func newLinkProber(t *testing.T, dial func(ctx context.Context, network string, address string) (net.Conn, error)) *prober.Prober {
	t.Helper()
	workloads, err := prober.ParseWorkloads(linkDocuments())
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	p, err := prober.New(prober.Config{Namespace: "shop", Catalog: catalog(), Workloads: workloads, DialContext: dial,
		LinkAddress: func(link traffic.Link) string { return link.To }})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	return p
}

func linkObservations(t *testing.T, p *prober.Prober) map[string]traffic.LinkObservations {
	t.Helper()
	window, ok := p.Windows()["links"]
	if !ok {
		t.Fatalf("no links window: %v", p.Windows())
	}
	byTarget := map[string]traffic.LinkObservations{}
	for _, observed := range window.Links {
		byTarget[observed.Link.To] = observed
	}
	return byTarget
}

func TestLinkProbeDialsFreshAndRecordsHealthyLinks(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	var accepted atomic.Int64
	go func() {
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			accepted.Add(1)
			_ = connection.Close()
		}
	}()
	p := newLinkProber(t, func(ctx context.Context, network string, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, listener.Addr().String())
	})
	probeFor(t, p, 450*time.Millisecond)
	for target, observed := range linkObservations(t, p) {
		if !observed.Qualified || len(observed.Samples) < 3 {
			t.Fatalf("%s: qualified %v samples %d", target, observed.Qualified, len(observed.Samples))
		}
		for _, sample := range observed.Samples {
			if !sample.OK {
				t.Fatalf("%s: healthy listener failed: %s", target, sample.Error)
			}
		}
	}
	if accepted.Load() < 6 {
		t.Fatalf("every sample must be a fresh connection, listener saw %d", accepted.Load())
	}
}

func TestLinkProbeRecordsABlockedLinkWhileOthersStayHealthy(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			_ = connection.Close()
		}
	}()
	var blocked atomic.Bool
	p := newLinkProber(t, func(ctx context.Context, network string, address string) (net.Conn, error) {
		if address == "backend" && blocked.Load() {
			<-ctx.Done() // a NetworkPolicy drop looks like a dial that never completes
			return nil, &net.OpError{Op: "dial", Net: network, Err: context.DeadlineExceeded}
		}
		return (&net.Dialer{}).DialContext(ctx, network, listener.Addr().String())
	})
	probeFor(t, p, 250*time.Millisecond)
	blocked.Store(true)
	probeFor(t, p, 900*time.Millisecond)
	observations := linkObservations(t, p)
	backend := observations["backend"]
	if !backend.Qualified {
		t.Fatalf("the link was reachable first, so it must be qualified")
	}
	last := backend.Samples[len(backend.Samples)-3:]
	for _, sample := range last {
		if sample.OK || sample.Error == "" {
			t.Fatalf("blocked link sample: %+v", sample)
		}
	}
	for _, sample := range observations["cache"].Samples {
		if !sample.OK {
			t.Fatalf("an unrelated link failed: %+v", sample)
		}
	}
}

func TestLinkProbeResetKeepsQualification(t *testing.T) {
	p := newLinkProber(t, func(ctx context.Context, network string, _ string) (net.Conn, error) {
		client, server := net.Pipe()
		_ = server.Close()
		return client, nil
	})
	probeFor(t, p, 100*time.Millisecond)
	p.Reset()
	for target, observed := range linkObservations(t, p) {
		if len(observed.Samples) != 0 || !observed.Qualified {
			t.Fatalf("%s after reset: %+v", target, observed)
		}
	}
}

func TestLinkProbeSamplesABlockedLinkOnTheIntervalNotIntervalPlusTimeout(t *testing.T) {
	documents := map[string][]byte{
		"links": []byte(`{"apiVersion": "sdo.dev/v1alpha1", "kind": "TrafficWorkload", "name": "links", "purpose": "link-probe",
			"interval": "100ms", "timeout": "80ms", "failures": 3, "links": [{"from": "web", "to": "backend", "port": 9001}]}`),
	}
	workloads, err := prober.ParseWorkloads(documents)
	if err != nil {
		t.Fatal(err)
	}
	p, err := prober.New(prober.Config{Namespace: "shop", Catalog: catalog(), Workloads: workloads,
		LinkAddress: func(link traffic.Link) string { return link.To },
		DialContext: func(ctx context.Context, network string, _ string) (net.Conn, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}})
	if err != nil {
		t.Fatal(err)
	}
	probeFor(t, p, time.Second)
	got := len(linkObservations(t, p)["backend"].Samples)
	// A fixed cadence gives about 10 samples; interval-plus-timeout would give about 5.
	if got < 8 {
		t.Fatalf("a blocked link was sampled only %d times in 1s", got)
	}
}
