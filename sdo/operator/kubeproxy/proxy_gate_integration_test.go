//go:build integration

package kubeproxy_test

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/apihistory"
	"sdo.dev/operator/kubeproxy"
)

// abandonThroughGate sends one request through a gated proxy whose upstream
// never answers, closes the client connection before any response, and
// returns the gate's observation once the proxy has finished the request.
func abandonThroughGate(t *testing.T, requestLine, body string, headers ...string) *apihistory.MutationObservation {
	t.Helper()
	arrived, finished := make(chan struct{}, 1), make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Reading the body to its end lets the server notice the proxy
		// hanging up.
		_, _ = io.Copy(io.Discard, r.Body)
		arrived <- struct{}{}
		select {
		case <-r.Context().Done():
		case <-finished:
		}
	}))
	t.Cleanup(upstream.Close)
	t.Cleanup(func() { close(finished) })
	address, _ := url.Parse(upstream.URL)
	gate, err := apihistory.PrepareMutationGate(filepath.Join(t.TempDir(), "gate"))
	if err != nil {
		t.Fatal(err)
	}
	proxy := httptest.NewServer(kubeproxy.NewRecording(address, http.DefaultTransport, http.DefaultTransport, kubeproxy.Config{Gate: gate}))
	t.Cleanup(proxy.Close)

	conn, err := net.Dial("tcp", strings.TrimPrefix(proxy.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	if body != "" {
		headers = append(headers, fmt.Sprintf("Content-Length: %d", len(body)))
	}
	request := requestLine + "\r\nHost: x\r\n" + strings.Join(append(headers, ""), "\r\n") + "\r\n" + body
	if _, err := conn.Write([]byte(request)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-arrived:
	case <-time.After(5 * time.Second):
		t.Fatal("the request never reached the upstream")
	}
	// The client gives up before the upstream answers (a `kubectl port-forward`
	// killed while it is still connecting).
	conn.Close()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		observation, err := gate.TryObserve()
		if err != nil {
			t.Fatal(err)
		}
		if observation != nil {
			t.Cleanup(func() { observation.Close() })
			return observation
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("the proxy still holds the request's mutation lease")
	return nil
}

// A request that cannot have changed anything leaves no uncertain mutation
// behind when its client gives up before the upstream answers: an uncertain
// record blocks the incident's closure for good.
func TestAbandonedRequestThatCannotWriteLeavesNoUncertainMutation(t *testing.T) {
	cases := map[string]struct {
		line, body string
		headers    []string
	}{
		"port-forward before the upgrade": {
			line:    "POST /api/v1/namespaces/app/pods/web-0/portforward?ports=8080 HTTP/1.1",
			headers: []string{"Connection: Upgrade", "Upgrade: SPDY/3.1"},
		},
		"access review": {
			line: "POST /apis/authorization.k8s.io/v1/selfsubjectaccessreviews HTTP/1.1", body: "{}",
			headers: []string{"Content-Type: application/json"},
		},
		"server-side dry run": {
			line: "PATCH /apis/apps/v1/namespaces/app/deployments/web?dryRun=All HTTP/1.1", body: "{}",
			headers: []string{"Content-Type: application/merge-patch+json"},
		},
	}
	for name, request := range cases {
		t.Run(name, func(t *testing.T) {
			observation := abandonThroughGate(t, request.line, request.body, request.headers...)
			if err := observation.Fence(time.Unix(1000, 0)); err != nil {
				t.Fatalf("abandoned %s blocked closure: %v", name, err)
			}
		})
	}
}

// A real write the client abandoned may still be applied by the API server:
// it stays uncertain.
func TestAbandonedWriteStaysUncertain(t *testing.T) {
	for _, request := range []struct {
		name, line, body string
		headers          []string
	}{
		{
			name:    "ordinary patch",
			line:    "PATCH /apis/apps/v1/namespaces/app/deployments/web HTTP/1.1",
			body:    "{}",
			headers: []string{"Content-Type: application/merge-patch+json"},
		},
		{
			name:    "connection request ignores dry run query",
			line:    "POST /api/v1/namespaces/app/pods/web-0/exec?dryRun=All HTTP/1.1",
			headers: []string{"Connection: Upgrade", "Upgrade: SPDY/3.1"},
		},
		{
			name:    "delete options body overrides dry run query",
			line:    "DELETE /api/v1/namespaces/app/pods/web-0?dryRun=All HTTP/1.1",
			body:    "{}",
			headers: []string{"Content-Type: application/json"},
		},
	} {
		t.Run(request.name, func(t *testing.T) {
			observation := abandonThroughGate(t, request.line, request.body, request.headers...)
			if err := observation.Fence(time.Unix(1000, 0)); err == nil {
				t.Fatal("an abandoned write must block closure")
			}
		})
	}
}

// kubectl prints a Warning header: reads carry the notice once, writes always,
// and a fence that outlived its recovery never says the incident is verified.
func TestAWithdrawnRecoveryIsToldToKubectl(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, "{}") }))
	t.Cleanup(upstream.Close)
	address, _ := url.Parse(upstream.URL)
	gate, err := apihistory.PrepareMutationGate(filepath.Join(t.TempDir(), "gate"))
	if err != nil {
		t.Fatal(err)
	}
	proxy := httptest.NewServer(kubeproxy.NewRecording(address, http.DefaultTransport, http.DefaultTransport, kubeproxy.Config{Gate: gate}))
	t.Cleanup(proxy.Close)
	send := func(method string) (string, int, string) {
		t.Helper()
		request, _ := http.NewRequest(method, proxy.URL+"/api/v1/namespaces/app/pods/a", nil)
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		body, _ := io.ReadAll(response.Body)
		return response.Header.Get("Warning"), response.StatusCode, string(body)
	}
	fence := func() {
		t.Helper()
		observation, err := gate.TryObserve()
		if err != nil || observation == nil {
			t.Fatal(err)
		}
		defer observation.Close()
		if err := observation.Fence(time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	fence()
	if warning, status, body := send("DELETE"); warning != "" || status != 409 || !strings.Contains(body, "retry") || strings.Contains(body, "finish") {
		t.Fatalf("%q %d %s", warning, status, body)
	}
	if err := gate.Withdraw("recovery withdrawn: write-item fires", "alert"); err != nil {
		t.Fatal(err)
	}
	if _, status, body := send("DELETE"); status != 409 || !strings.Contains(body, "recovery withdrawn: write-item fires") || strings.Contains(body, "Recovery judged") {
		t.Fatalf("%d %s", status, body)
	}
	if err := gate.Reopen(); err != nil {
		t.Fatal(err)
	}
	for _, method := range []string{"DELETE", "DELETE"} {
		if warning, status, _ := send(method); status != 200 || !strings.Contains(warning, "write-item") {
			t.Fatalf("%d %q", status, warning)
		}
	}
	if warning, _, _ := send("GET"); warning != "" {
		t.Fatalf("a read repeated the notice: %q", warning)
	}
	if err := gate.Withdraw("recovery withdrawn: read-item fires", "alert"); err != nil {
		t.Fatal(err)
	}
	if warning, _, _ := send("GET"); !strings.Contains(warning, "read-item") {
		t.Fatalf("a read missed the notice: %q", warning)
	}
	fence()
	if warning, _, _ := send("GET"); warning != "" {
		t.Fatalf("a verified incident repeats the old notice: %q", warning)
	}
}

// openUpgradedStream opens one upgraded stream through a gated proxy to an
// upstream that answers 101 and echoes, and returns the client side.
func openUpgradedStream(t *testing.T, gate *apihistory.MutationGate, path string) (net.Conn, *bufio.Reader, string) {
	t.Helper()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, buffered, err := http.NewResponseController(w).Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = buffered.WriteString("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: SPDY/3.1\r\n\r\n")
		_ = buffered.Flush()
		_, _ = io.Copy(conn, buffered)
	}))
	t.Cleanup(upstream.Close)
	address, _ := url.Parse(upstream.URL)
	proxy := httptest.NewServer(kubeproxy.NewRecording(address, http.DefaultTransport, http.DefaultTransport, kubeproxy.Config{Gate: gate}))
	t.Cleanup(proxy.Close)
	conn, err := net.Dial("tcp", strings.TrimPrefix(proxy.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	if _, err := fmt.Fprintf(conn, "POST %s HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: SPDY/3.1\r\n\r\n", path); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	response, err := http.ReadResponse(reader, nil)
	if err != nil || response.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("upgrade: %v %v", response, err)
	}
	echo(t, conn, reader, "ping")
	return conn, reader, proxy.URL
}

func echo(t *testing.T, conn net.Conn, reader *bufio.Reader, line string) {
	t.Helper()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := fmt.Fprintln(conn, line); err != nil {
		t.Fatal(err)
	}
	if got, err := reader.ReadString('\n'); err != nil || got != line+"\n" {
		t.Fatalf("echo %q: %q %v", line, got, err)
	}
}

// observe retries a busy gate, as the controller does: a peer can read forwarded
// bytes before the proxy releases that write's shared lock. A stream that holds
// the lock still fails at the deadline.
func observe(t *testing.T, gate *apihistory.MutationGate) *apihistory.MutationObservation {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		observation, err := gate.TryObserve()
		if err != nil {
			t.Fatal(err)
		}
		if observation != nil {
			return observation
		}
		if time.Now().After(deadline) {
			t.Fatalf("an open stream blocks observation: %+v", gate.InFlight())
		}
		time.Sleep(time.Millisecond)
	}
}

// upgrade sends an upgrade request through the proxy and returns its status.
func upgrade(t *testing.T, proxyURL, path string) int {
	t.Helper()
	conn, err := net.Dial("tcp", strings.TrimPrefix(proxyURL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := fmt.Fprintf(conn, "POST %s HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: SPDY/3.1\r\n\r\n", path); err != nil {
		t.Fatal(err)
	}
	response, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	return response.StatusCode
}

func fence(t *testing.T, gate *apihistory.MutationGate) {
	t.Helper()
	observation := observe(t, gate)
	defer observation.Close()
	if err := observation.Fence(time.Unix(1000, 0)); err != nil {
		t.Fatal(err)
	}
}

const portForwardPath = "/api/v1/namespaces/app/pods/web-0/portforward?ports=8080"

// A port-forward left open (a backgrounded `kubectl port-forward`) is a
// network tunnel, not a write: it never blocks Recovered and the fence leaves
// it alone. A new one is refused once the incident is fenced, like a new exec.
func TestAnOpenPortForwardNeverBlocksRecovered(t *testing.T) {
	gate, err := apihistory.PrepareMutationGate(filepath.Join(t.TempDir(), "gate"))
	if err != nil {
		t.Fatal(err)
	}
	conn, reader, proxyURL := openUpgradedStream(t, gate, portForwardPath)
	observation := observe(t, gate)
	if observation.Pending || len(observation.OpenSessions) != 0 {
		t.Fatalf("a port-forward took a lease: %+v", observation)
	}
	observation.Close()
	fence(t, gate)
	echo(t, conn, reader, "after the fence")
	if status := upgrade(t, proxyURL, portForwardPath); status != http.StatusConflict {
		t.Fatalf("a new port-forward after the fence got %d", status)
	}
}

// An exec stream in use is journalled with its last activity in either
// direction. A verification fence holds its client bytes rather than closing
// it, because a withdrawn recovery reopens writes; the seal closes it.
func TestAnOpenExecStreamIsHeldByTheFenceAndClosedByTheSeal(t *testing.T) {
	gate, err := apihistory.PrepareMutationGate(filepath.Join(t.TempDir(), "gate"))
	if err != nil {
		t.Fatal(err)
	}
	conn, reader, _ := openUpgradedStream(t, gate, "/api/v1/namespaces/app/pods/web-0/exec?command=sh&stdin=true")
	observation := observe(t, gate)
	if observation.Pending || len(observation.OpenSessions) != 1 || observation.OpenSessions[0].Subject.String() != "exec app/pods/web-0" {
		t.Fatalf("observation: %+v", observation)
	}
	if active, _, ok := observation.LastSessionActivity(); !ok || active.IsZero() {
		t.Fatalf("no session activity: %+v", observation.OpenSessions)
	}
	observation.Close()
	fence(t, gate)
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	_, _ = fmt.Fprintln(conn, "under the fence")
	got := make(chan string, 1)
	go func() { line, _ := reader.ReadString('\n'); got <- line }()
	select {
	case line := <-got:
		t.Fatalf("the fenced session forwarded %q", line)
	case <-time.After(time.Second):
	}
	if err := gate.Reopen(); err != nil {
		t.Fatal(err)
	}
	if line := <-got; line != "under the fence\n" {
		t.Fatalf("the withdrawn fence did not release the held bytes: %q", line)
	}
	fence(t, gate)
	sealing := observe(t, gate)
	if err := sealing.Seal(); err != nil {
		t.Fatal(err)
	}
	sealing.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if line, err := reader.ReadString('\n'); err == nil {
		t.Fatalf("the sealed session forwarded %q", line)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && len(gate.InFlight()) > 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if entries := gate.InFlight(); len(entries) != 0 {
		t.Fatalf("the closed session is still journalled: %+v", entries)
	}
}
