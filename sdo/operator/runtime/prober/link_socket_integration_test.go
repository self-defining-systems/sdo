//go:build integration

package prober

import (
	"context"
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

func TestLinkGRPCTLSVerifiesCertificates(t *testing.T) {
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("untrusted TLS connection reached handler") }))
	server.EnableHTTP2 = true
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.StartTLS()
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
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
