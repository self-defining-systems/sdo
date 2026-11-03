//go:build integration

package prober

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sdo.dev/operator/sdk/traffic"
	"testing"
	"time"
)

func TestLinkProtocolsSocketReset(t *testing.T) {
	for _, protocol := range []string{"http", "grpc", "redis", "memcached", "mongodb", "postgres", "mysql"} {
		t.Run(protocol, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			go func() {
				c, err := listener.Accept()
				if err != nil {
					return
				}
				c.(*net.TCPConn).SetLinger(0)
				c.Close()
			}()
			checkLinkProtocol(t, protocol, "reset", func(ctx context.Context, network, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, network, listener.Addr().String())
			})
		})
	}
}
func TestLinkRealRedis(t *testing.T) {
	address := os.Getenv("SDO_LINK_TEST_REDIS")
	if address == "" {
		t.Skip("set SDO_LINK_TEST_REDIS for the real Redis smoke")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c, err := (&net.Dialer{}).DialContext(ctx, "tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	deadline, _ := ctx.Deadline()
	c.SetDeadline(deadline)
	status, excerpt, class, err := probeProtocol(ctx, c, traffic.Link{Protocol: traffic.LinkProtocolRedis})
	if err != nil || class != "" || status != "+PONG" {
		t.Fatalf("status=%s excerpt=%s class=%s err=%v", status, excerpt, class, err)
	}
}

// tlsHangGuard bounds a handshake that never finishes; it is far above any
// loaded-host handshake time so the outcome never depends on it.
const tlsHangGuard = time.Minute

func TestLinkGRPCTLSVerifiesCertificates(t *testing.T) {
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("untrusted TLS connection reached handler") }))
	server.EnableHTTP2 = true
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.StartTLS()
	defer server.Close()
	// The class must come from the certificate check, never from the clock: a
	// loaded host can stretch a real handshake past any short budget and turn
	// the failure into a timeout. The deadline only bounds a genuine hang.
	ctx, cancel := context.WithTimeout(context.Background(), tlsHangGuard)
	defer cancel()
	c, err := (&net.Dialer{}).DialContext(ctx, "tcp", server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	deadline, _ := ctx.Deadline()
	c.SetDeadline(deadline)
	_, _, class, err := probeProtocol(ctx, c, traffic.Link{To: "backend", Protocol: traffic.LinkProtocolGRPC, GRPC: &traffic.LinkGRPCOptions{TLS: true}})
	if err == nil || class != traffic.LinkFailureTransport {
		t.Fatalf("untrusted TLS: class=%s err=%v", class, err)
	}
}

// checkLinkProtocol dials one link through dial and checks the probe classified it as mode says.
func checkLinkProtocol(t *testing.T, protocol, mode string, dial func(context.Context, string, string) (net.Conn, error)) {
	address := "backend:1234"
	docs := map[string][]byte{"links": []byte(fmt.Sprintf(`{"apiVersion":"sdo.dev/v1alpha1","kind":"TrafficWorkload","name":"links","purpose":"link-probe","timeout":"50ms","links":[{"from":"web","to":"backend","port":1234,"protocol":%q}]}`, protocol))}
	ws, e := ParseWorkloads(docs)
	if e != nil {
		t.Fatal(e)
	}
	p, e := New(Config{Namespace: "test", Catalog: traffic.Catalog{{ID: "dummy", Target: traffic.Target{Service: "web", Port: 80}, SideEffect: traffic.SideEffectRead, Steps: []traffic.Step{{Name: "dummy", Endpoint: traffic.GET("/", nil)}}}}, Workloads: ws, LinkAddress: func(traffic.Link) string { return address }, DialContext: dial})
	if e != nil {
		t.Fatal(e)
	}
	state := &linkState{link: ws[0].Links[0]}
	p.dialLink(context.Background(), ws[0], state)
	if len(state.samples) != 1 {
		t.Fatal(state.samples)
	}
	s := state.samples[0]
	want := ""
	switch mode {
	case "protocol":
		want = "protocol_error"
	case "refused":
		want = "refused"
	case "timeout":
		want = "timeout"
	case "not-serving", "unknown-health":
		want = "not_serving"
	case "reset":
		want = "reset"
	}
	if s.OK != (mode == "reachable" || mode == "auth" || mode == "unimplemented" || mode == "other-status") || string(s.Class) != want || s.Duration <= 0 {
		t.Fatalf("sample %+v; want class %q", s, want)
	}
}
