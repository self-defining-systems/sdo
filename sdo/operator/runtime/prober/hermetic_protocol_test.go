package prober

import (
	"context"
	"net"
	"testing"
	"time"

	"sdo.dev/operator/runtime/internal/testprotocol"
	"sdo.dev/operator/sdk/traffic"
)

func TestHermeticProtocolDialer(t *testing.T) {
	for _, protocol := range []traffic.LinkProtocol{traffic.LinkProtocolHTTP, traffic.LinkProtocolGRPC, traffic.LinkProtocolRedis, traffic.LinkProtocolMemcached, traffic.LinkProtocolMongoDB, traffic.LinkProtocolPostgres, traffic.LinkProtocolMySQL} {
		for _, mode := range []string{"healthy", "refused", "protocol_error"} {
			t.Run(string(protocol)+"/"+mode, func(t *testing.T) {
				p, err := New(Config{Namespace: "app", Catalog: traffic.Catalog{{ID: "dummy", Target: traffic.Target{Service: "web", Port: 80}, SideEffect: traffic.SideEffectRead, Steps: []traffic.Step{{Name: "dummy", Endpoint: traffic.GET("/", nil)}}}}, DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					return testprotocol.Dial(ctx, string(protocol), mode)
				}})
				if err != nil {
					t.Fatal(err)
				}
				state := &linkState{link: traffic.Link{From: "web", To: "db", Port: 1234, Protocol: protocol}}
				p.dialLink(context.Background(), traffic.Workload{Timeout: traffic.Duration(time.Second)}, state)
				sample := state.samples[0]
				want := traffic.LinkFailureClass("")
				if mode == "refused" {
					want = traffic.LinkFailureRefused
				}
				if mode == "protocol_error" {
					want = traffic.LinkFailureProtocol
				}
				if sample.OK != (mode == "healthy") || sample.Class != want {
					t.Fatalf("sample: %+v", sample)
				}
			})
		}
	}
}
