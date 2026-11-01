package runtime

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sdo.dev/operator/runtime/internal/testprober"
	"sdo.dev/operator/runtime/prober"
	"strings"
	"testing"
)

// The real prober HTTP API has no generators or workloads. Only its network
// edge is replaced: our hand-written app listens on 8080 and refuses other ports.
func noTrafficProberHandler(t *testing.T) http.Handler {
	t.Helper()
	p, err := prober.New(prober.Config{Namespace: "app", DialContext: func(_ context.Context, _, address string) (net.Conn, error) {
		if !strings.HasSuffix(address, ":8080") {
			return nil, errors.New("connection refused")
		}
		client, server := net.Pipe()
		_ = server.Close()
		return client, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	return testprober.Handler(p)
}

// noTrafficRoutingProber serves that API on a real loopback socket.
func noTrafficRoutingProber(t *testing.T) HTTPProberClient {
	t.Helper()
	api := httptest.NewServer(noTrafficProberHandler(t))
	t.Cleanup(api.Close)
	return HTTPProberClient{BaseURL: StaticProberURL(api.URL)}
}

type routingCaptureDispatcher struct{ requests chan IncidentRequest }

func (d routingCaptureDispatcher) Dispatch(ctx context.Context, request IncidentRequest) (IncidentResult, error) {
	d.requests <- request
	<-ctx.Done()
	return IncidentResult{}, ctx.Err()
}
