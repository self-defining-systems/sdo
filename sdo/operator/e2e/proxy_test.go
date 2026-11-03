//go:build integration && controller_e2e

package e2e

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Faults is what the proxy injects in front of the API server. Watches are
// never failed by rate (a blackout cuts them); everything else is.
type Faults struct {
	ErrorRate float64       // fraction of requests answered 500
	Latency   time.Duration // added to every request
	Blackout  bool          // refuse every request with 503 and cut open watches
}

// Proxy is a small TLS reverse proxy in front of the API server. The controller's
// in-cluster config points at it; the harness, kubelet and workers go direct.
type Proxy struct {
	Port     int
	Requests atomic.Int64
	Injected atomic.Int64

	mu     sync.Mutex
	faults Faults
	rng    *rand.Rand
	conns  map[net.Conn]struct{}
}

// StartProxy starts the proxy for a cluster; the seed makes the injected error pattern repeat.
func StartProxy(t testing.TB, cluster *Cluster, seed int64) *Proxy {
	t.Helper()
	target, _ := url.Parse(fmt.Sprintf("https://127.0.0.1:%d", cluster.Port))
	ca, err := os.ReadFile(filepath.Join(cluster.Dir, "ca.crt"))
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(ca)
	reverse := httputil.NewSingleHostReverseProxy(target)
	reverse.Transport = &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}, MaxIdleConnsPerHost: 64}
	reverse.FlushInterval = -1
	p := &Proxy{rng: rand.New(rand.NewSource(seed)), conns: map[net.Conn]struct{}{}}
	cert, err := tls.LoadX509KeyPair(filepath.Join(cluster.Dir, "tls.crt"), filepath.Join(cluster.Dir, "tls.key"))
	if err != nil {
		t.Fatal(err)
	}
	p.Port = freePort(t)
	listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", p.Port))
	if err != nil {
		t.Fatal(err)
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p.Requests.Add(1)
		p.mu.Lock()
		faults := p.faults
		fail := faults.ErrorRate > 0 && p.rng.Float64() < faults.ErrorRate && r.URL.Query().Get("watch") != "true" && r.URL.Query().Get("watch") != "1"
		p.mu.Unlock()
		if faults.Blackout || fail {
			p.Injected.Add(1)
			status := http.StatusInternalServerError
			if faults.Blackout {
				status = http.StatusServiceUnavailable
			}
			http.Error(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","message":"injected fault","reason":"InternalError","code":`+fmt.Sprint(status)+`}`, status)
			return
		}
		if faults.Latency > 0 {
			select {
			case <-time.After(faults.Latency):
			case <-r.Context().Done():
				return
			}
		}
		reverse.ServeHTTP(w, r)
	})
	server := &http.Server{Handler: handler, TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}},
		ConnState: func(c net.Conn, s http.ConnState) {
			p.mu.Lock()
			defer p.mu.Unlock()
			if s == http.StateClosed || s == http.StateHijacked {
				delete(p.conns, c)
			} else {
				p.conns[c] = struct{}{}
			}
		}}
	go func() { _ = server.ServeTLS(listener, "", "") }()
	t.Cleanup(func() { _ = server.Close() })
	return p
}

// Set replaces the injected faults. A blackout also cuts every open connection so long watches notice.
func (p *Proxy) Set(f Faults) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.faults = f
	if f.Blackout {
		for c := range p.conns {
			_ = c.Close()
		}
	}
}

// Clear stops all injected faults.
func (p *Proxy) Clear() { p.Set(Faults{}) }

func isControllerPod(name string) bool { return strings.HasPrefix(name, "sdo-controller-run") }
