package runtime

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"sdo.dev/operator/runtime/prober"
	"sdo.dev/operator/sdk/traffic"
)

// TestObserverWakesOnAQualifiedLinkThatStopsConnecting covers the
// controller side of link reachability: the prober's link window is warm once
// every link has a sample, healthy links stay quiet, and a link that
// connected before and now fails wakes the controller.
func TestObserverWakesOnAQualifiedLinkThatStopsConnecting(t *testing.T) {
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
	workloads, err := prober.ParseWorkloads(map[string][]byte{
		"links": []byte(`{"apiVersion": "sdo.dev/v1alpha1", "kind": "TrafficWorkload", "name": "links", "purpose": "link-probe",
			"interval": "100ms", "timeout": "50ms", "failures": 2, "links": [{"from": "web", "to": "backend", "port": 9001}]}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	p, err := prober.New(prober.Config{
		Namespace: "shop", Catalog: frontendCatalog(), Workloads: workloads,
		LinkAddress: func(link traffic.Link) string { return link.To },
		DialContext: func(ctx context.Context, network string, _ string) (net.Conn, error) {
			if blocked.Load() {
				<-ctx.Done()
				return nil, ctx.Err()
			}
			return (&net.Dialer{}).DialContext(ctx, network, listener.Addr().String())
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	running, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Run(running)

	observer := NewTrafficObserver(proberDirect{p}, []string{"links"}, nil, 0)
	if !observer.WaitWarm(running, 5*time.Second) {
		t.Fatalf("link window never warmed: %+v", observer.Windows())
	}
	time.Sleep(300 * time.Millisecond)
	if observer.Poll(running) {
		t.Fatalf("healthy links must not wake the controller")
	}
	blocked.Store(true)
	time.Sleep(500 * time.Millisecond)
	if !observer.Poll(running) {
		t.Fatalf("a link that stopped connecting must wake the controller")
	}
}

// proberDirect adapts an in-process prober to the controller's ProberAPI.
type proberDirect struct{ p *prober.Prober }

func (d proberDirect) Windows(context.Context) (map[string]traffic.Window, error) {
	return d.p.Windows(), nil
}

func (d proberDirect) Burst(ctx context.Context, request prober.BurstRequest) (prober.BurstResult, error) {
	return d.p.Burst(ctx, request)
}

func (d proberDirect) Reset(context.Context) error { d.p.Reset(); return nil }
