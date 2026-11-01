package prober_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"sdo.dev/operator/runtime/internal/testprober"

	"sdo.dev/operator/runtime/prober"
	"sdo.dev/operator/sdk/servicehealth"
	"sdo.dev/operator/sdk/traffic"
)

// reachabilityProber maps Service names to local addresses: "open" accepts,
// "closed" refuses, "blackhole" never completes, "missing" fails to resolve.
func reachabilityProber(t *testing.T) (*prober.Prober, func() int) {
	t.Helper()
	accepted := make(chan struct{}, 64)
	p, err := prober.New(prober.Config{
		Namespace:   "shop",
		LinkAddress: func(link traffic.Link) string { return link.To + ":1" },
		DialContext: func(ctx context.Context, network string, address string) (net.Conn, error) {
			switch address {
			case "blackhole:1":
				<-ctx.Done()
				return nil, &net.OpError{Op: "dial", Net: network, Err: ctx.Err()}
			case "missing:1":
				return nil, &net.OpError{Op: "dial", Net: network, Err: &net.DNSError{Err: "no such host", Name: "missing", IsNotFound: true}}
			}
			if address == "closed:1" {
				return nil, &net.OpError{Op: "dial", Net: network, Err: syscall.ECONNREFUSED}
			}
			client, peer := net.Pipe()
			accepted <- struct{}{}
			_ = peer.Close()
			return client, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return p, func() int { return len(accepted) }
}

func TestDialTargetsClassifiesEachTargetInRequestOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, accepted := reachabilityProber(t)
		request := prober.DialRequest{Timeout: 100 * time.Millisecond, Targets: []servicehealth.ReachabilityTarget{
			{Service: "open", Port: 8080}, {Service: "closed", Port: 8081}, {Service: "blackhole", Port: 8082}, {Service: "missing", Port: 8083},
		}}
		started := time.Now()
		response, err := p.DialTargets(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		if elapsed := time.Since(started); elapsed > time.Second {
			t.Fatalf("dials run concurrently within the timeout, took %s", elapsed)
		}
		want := []servicehealth.DialErrorClass{"", servicehealth.DialRefused, servicehealth.DialTimeout, servicehealth.DialDNS}
		if len(response.Results) != len(want) {
			t.Fatalf("results: %+v", response.Results)
		}
		for index, result := range response.Results {
			if result.Target != request.Targets[index] {
				t.Fatalf("result %d is for %v, want %v", index, result.Target, request.Targets[index])
			}
			if result.Sample.OK != (want[index] == "") || result.Sample.Class != want[index] || result.Sample.At.IsZero() {
				t.Fatalf("%s: got %+v want class %q", result.Target.Service, result.Sample, want[index])
			}
			if !result.Sample.OK && result.Sample.Error == "" {
				t.Fatalf("%s: a failed dial must carry its error", result.Target.Service)
			}
		}
		if accepted() != 1 {
			t.Fatalf("the open target must see exactly one fresh connection, saw %d", accepted())
		}
	})
}

func TestClassifyDialError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for want, err := range map[servicehealth.DialErrorClass]error{
			servicehealth.DialRefused:     &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED},
			servicehealth.DialReset:       &net.OpError{Op: "dial", Err: syscall.ECONNRESET},
			servicehealth.DialUnreachable: &net.OpError{Op: "dial", Err: syscall.EHOSTUNREACH},
			servicehealth.DialTimeout:     context.DeadlineExceeded,
			servicehealth.DialDNS:         &net.DNSError{Err: "server misbehaving", Name: "geo"},
			servicehealth.DialOther:       net.ErrClosed,
		} {
			if got := prober.ClassifyDialError(err); got != want {
				t.Fatalf("%v: got %q want %q", err, got, want)
			}
		}
	})
}

func TestDialTargetsEndpointValidatesRequests(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, _ := reachabilityProber(t)
		server := handlerDoer{testprober.Handler(p)}
		post := func(request prober.DialRequest) *http.Response {
			body, _ := json.Marshal(request)
			response, err := handlerCall(t, server, http.MethodPost, testprober.PathDials, bytes.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			return response
		}
		response := post(prober.DialRequest{Targets: []servicehealth.ReachabilityTarget{{Service: "open", Port: 8080}}})
		var decoded prober.DialResponse
		if err := json.NewDecoder(response.Body).Decode(&decoded); err != nil || response.StatusCode != http.StatusOK ||
			len(decoded.Results) != 1 || !decoded.Results[0].Sample.OK {
			t.Fatalf("status %d results %+v err %v", response.StatusCode, decoded.Results, err)
		}
		response.Body.Close()
		tooMany := make([]servicehealth.ReachabilityTarget, servicehealth.MaxReachabilityTargets+1)
		for index := range tooMany {
			tooMany[index] = servicehealth.ReachabilityTarget{Service: "open", Port: int32(1000 + index)}
		}
		for name, request := range map[string]prober.DialRequest{
			"too many targets": {Targets: tooMany},
			"bad service name": {Targets: []servicehealth.ReachabilityTarget{{Service: "Open.evil", Port: 80}}},
			"bad port":         {Targets: []servicehealth.ReachabilityTarget{{Service: "open", Port: 0}}},
			"long timeout":     {Targets: []servicehealth.ReachabilityTarget{{Service: "open", Port: 80}}, Timeout: time.Minute},
		} {
			response := post(request)
			response.Body.Close()
			if response.StatusCode != http.StatusBadRequest {
				t.Fatalf("%s: status %d", name, response.StatusCode)
			}
		}
	})
}
