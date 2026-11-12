//go:build integration

package kubeproxy_test

import (
	"bufio"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"sdo.dev/operator/apihistory"
	"sdo.dev/operator/kubeproxy"
)

type upstreamLog struct {
	mu       sync.Mutex
	requests []*http.Request
}

func (l *upstreamLog) add(r *http.Request) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.requests = append(l.requests, r)
}
func (l *upstreamLog) count() int { l.mu.Lock(); defer l.mu.Unlock(); return len(l.requests) }
func (l *upstreamLog) last() *http.Request {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.requests[len(l.requests)-1]
}

func newUpstream(t *testing.T) (*url.URL, *upstreamLog) {
	t.Helper()
	log := &upstreamLog{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.add(r)
		if strings.Contains(r.Header.Get("Connection"), "Upgrade") {
			conn, rw, err := w.(http.Hijacker).Hijack()
			if err != nil {
				return
			}
			defer conn.Close()
			_, _ = rw.WriteString("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: " + r.Header.Get("Upgrade") + "\r\n\r\n")
			_ = rw.Flush()
			line, _ := bufio.NewReader(conn).ReadString('\n')
			_, _ = conn.Write([]byte(line))
			return
		}
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"api","namespace":"app","resourceVersion":"42"}}`))
	}))
	t.Cleanup(server.Close)
	address, _ := url.Parse(server.URL)
	return address, log
}

func openStore(t *testing.T) (*apihistory.Store, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "history")
	if err := apihistory.BeginGeneration(dir); err != nil {
		t.Fatal(err)
	}
	store, err := apihistory.Open(dir, "incident", apihistory.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return store, dir
}

func serve(t *testing.T, config kubeproxy.Config) (*httptest.Server, *upstreamLog) {
	t.Helper()
	address, log := newUpstream(t)
	handler := kubeproxy.NewRecording(address, http.DefaultTransport, http.DefaultTransport, config)
	proxy := httptest.NewServer(handler)
	t.Cleanup(proxy.Close)
	return proxy, log
}

func do(t *testing.T, method, target, contentType, body string) *http.Response {
	t.Helper()
	request, _ := http.NewRequest(method, target, strings.NewReader(body))
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	response.Body.Close()
	return response
}

func history(t *testing.T, store *apihistory.Store, dir string) apihistory.History {
	t.Helper()
	store.Close()
	h, err := apihistory.Read(dir)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

type refusingRecorder struct{}

func (refusingRecorder) Record(apihistory.Event, []byte) {}
func (refusingRecorder) Gap()                            {}
func (refusingRecorder) Admit() error                    { return errors.New("store failed to open") }

func TestWriteIsRefusedWhenItCannotBeRecordedAndNeverForwarded(t *testing.T) {
	proxy, upstream := serve(t, kubeproxy.Config{Recorder: refusingRecorder{}})

	for _, write := range []struct{ method, path string }{
		{"POST", "/api/v1/namespaces/app/configmaps"},
		{"PATCH", "/apis/apps/v1/namespaces/app/deployments/api"},
		{"DELETE", "/api/v1/namespaces/app/pods/api-0"},
		{"GET", "/api/v1/namespaces/app/pods/api-0/exec?command=ls&container=api"},
	} {
		response := do(t, write.method, proxy.URL+write.path, "application/json", `{}`)
		if response.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("%s %s = %d, want 503", write.method, write.path, response.StatusCode)
		}
	}
	if upstream.count() != 0 {
		t.Fatalf("upstream saw %d requests; an unrecordable write must never be forwarded", upstream.count())
	}
	if response := do(t, "GET", proxy.URL+"/api/v1/namespaces/app/pods", "", ""); response.StatusCode != http.StatusOK || upstream.count() != 1 {
		t.Fatalf("a read must still be forwarded: %d, upstream %d", response.StatusCode, upstream.count())
	}
}

func TestWriteRecordsFieldPathsForcesTheManagerAndStoresNoBody(t *testing.T) {
	store, dir := openStore(t)
	proxy, upstream := serve(t, kubeproxy.Config{Recorder: store, FieldManager: "sdo-responder"})
	const canary = "bodycanaryvalue"

	do(t, "PATCH", proxy.URL+"/apis/apps/v1/namespaces/app/deployments/api?fieldManager=kubectl-edit&dryRun=", "application/strategic-merge-patch+json",
		`{"spec":{"template":{"spec":{"containers":[{"name":"api","image":"registry.invalid/`+canary+`"}]}}}}`)

	if got := upstream.last().URL.Query()["fieldManager"]; len(got) != 1 || got[0] != "sdo-responder" {
		t.Fatalf("upstream fieldManager = %v, want exactly sdo-responder", got)
	}
	h := history(t, store, dir)
	var end apihistory.Event
	for _, e := range h.Events {
		if e.Phase == "end" {
			end = e
		}
	}
	if end.Verb != "patch" || end.Name != "api" || end.Status != 200 || end.ResourceVersion == "" {
		t.Fatalf("end event = %+v", end)
	}
	if end.FieldManager != "sdo-responder" {
		t.Fatalf("recorded field manager = %q", end.FieldManager)
	}
	if len(end.ChangedPaths) != 1 || end.ChangedPaths[0] != "spec.template.spec.containers[api].image" {
		t.Fatalf("changed paths = %v", end.ChangedPaths)
	}
	events, _ := os.ReadFile(filepath.Join(dir, "events.jsonl"))
	if strings.Contains(string(events), canary) {
		t.Fatal("request body value reached the recording")
	}
}

func TestManagerIsNotForcedWithoutAConfiguredManager(t *testing.T) {
	proxy, upstream := serve(t, kubeproxy.Config{})
	do(t, "PATCH", proxy.URL+"/api/v1/namespaces/app/configmaps/x?fieldManager=kubectl", "application/merge-patch+json", `{"data":{"a":"b"}}`)
	if got := upstream.last().URL.Query().Get("fieldManager"); got != "kubectl" {
		t.Fatalf("unconfigured proxy rewrote fieldManager to %q", got)
	}
}

func TestReadOnlyScopeRefusesWritesAndSessionsButForwardsReads(t *testing.T) {
	store, dir := openStore(t)
	proxy, upstream := serve(t, kubeproxy.Config{Recorder: store, ReadOnly: true})

	for _, write := range []struct{ method, path string }{
		{"POST", "/api/v1/namespaces/app/pods"},
		{"PUT", "/apis/apps/v1/namespaces/app/deployments/api"},
		{"PATCH", "/apis/apps/v1/namespaces/app/deployments/api"},
		{"DELETE", "/api/v1/namespaces/app/pods/api-0"},
		{"GET", "/api/v1/namespaces/app/pods/api-0/exec?command=ls"},
		{"GET", "/api/v1/namespaces/app/pods/api-0/attach"},
		{"GET", "/api/v1/namespaces/app/pods/api-0/portforward"},
	} {
		if response := do(t, write.method, proxy.URL+write.path, "application/json", `{}`); response.StatusCode != http.StatusForbidden {
			t.Fatalf("%s %s = %d, want 403", write.method, write.path, response.StatusCode)
		}
	}
	if upstream.count() != 0 {
		t.Fatalf("read-only scope forwarded %d writes", upstream.count())
	}
	if response := do(t, "GET", proxy.URL+"/api/v1/namespaces/app/pods", "", ""); response.StatusCode != http.StatusOK {
		t.Fatalf("read = %d", response.StatusCode)
	}
	if response := do(t, "POST", proxy.URL+"/apis/authorization.k8s.io/v1/selfsubjectaccessreviews", "application/json", `{}`); response.StatusCode != http.StatusOK {
		t.Fatalf("non-persisting review create = %d, want it forwarded", response.StatusCode)
	}
	refused := 0
	for _, e := range history(t, store, dir).Events {
		if e.Phase == "end" && e.Status == http.StatusForbidden && e.Omission == "scope_read_only" {
			refused++
		}
	}
	if refused != 7 {
		t.Fatalf("recorded %d refusals, want 7", refused)
	}
}

func TestExecSessionRecordsTargetContainerRedactedCommandAndSpan(t *testing.T) {
	store, dir := openStore(t)
	proxy, _ := serve(t, kubeproxy.Config{Recorder: store, FieldManager: "sdo-responder"})
	const canary = "SECRETPASSWORD"

	address := strings.TrimPrefix(proxy.URL, "http://")
	conn, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = conn.Write([]byte("GET /api/v1/namespaces/app/pods/db-0/exec?container=db&command=mysql&command=-p" + canary + "&command=mydb HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: SPDY/3.1\r\n\r\n"))
	reader := bufio.NewReader(conn)
	status, _ := reader.ReadString('\n')
	if !strings.Contains(status, "101") {
		t.Fatalf("exec upgrade = %q", status)
	}
	for {
		line, _ := reader.ReadString('\n')
		if line == "\r\n" || line == "" {
			break
		}
	}
	started := time.Now()
	_, _ = conn.Write([]byte("ping\n"))
	echo, _ := reader.ReadString('\n')
	if echo != "ping\n" {
		t.Fatalf("stream bytes were altered: %q", echo)
	}
	time.Sleep(50 * time.Millisecond)
	conn.Close()
	deadline := time.Now().Add(2 * time.Second)
	var end apihistory.Event
	for time.Now().Before(deadline) {
		h := historySnapshot(t, store, dir)
		for _, e := range h.Events {
			if e.Phase == "end" && e.Subresource == "exec" {
				end = e
			}
		}
		if end.Subresource != "" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if end.Subresource != "exec" || end.Name != "db-0" || end.Container != "db" {
		t.Fatalf("exec end = %+v", end)
	}
	if len(end.Command) != 3 || end.Command[0] != "mysql" || end.Command[1] != apihistory.Withheld {
		t.Fatalf("command = %q", end.Command)
	}
	if end.SessionEnd.IsZero() || end.SessionEnd.Before(started) {
		t.Fatalf("session end %v not after session start %v", end.SessionEnd, started)
	}
	store.Close()
	events, _ := os.ReadFile(filepath.Join(dir, "events.jsonl"))
	if strings.Contains(string(events), canary) {
		t.Fatal("credential-shaped argument reached the recording")
	}
}

func historySnapshot(t *testing.T, store *apihistory.Store, dir string) apihistory.History {
	t.Helper()
	// Checkpoints publish a drained prefix every 100ms; a partial read is enough for polling.
	h, err := apihistory.Read(dir)
	if err != nil {
		return apihistory.History{}
	}
	return h
}
