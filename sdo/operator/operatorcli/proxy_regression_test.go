package operatorcli_test

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/apihistory"
	"sdo.dev/operator/kubeproxy"
	"sdo.dev/operator/operatorcli"
)

const canary = "RV2_SYNTHETIC_CREDENTIAL_CANARY"

func storedCanary(t *testing.T, dir string) bool {
	t.Helper()
	found := false
	err := filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			b, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			found = found || bytes.Contains(b, []byte(canary))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return found
}

// Every value is a constructed fake. This never loads in-cluster config.
func TestCredentialPaths(t *testing.T) {
	for _, tc := range []struct {
		name, method, path, body string
		status                   int
	}{
		{"authorization_header", "GET", "/api/v1/namespaces/app/pods/p", `{"apiVersion":"v1","kind":"Pod","metadata":{"name":"p","namespace":"app"}}`, 200},
		{"secret_list", "GET", "/api/v1/namespaces/app/secrets", `{"apiVersion":"v1","kind":"SecretList","items":[{"metadata":{"name":"s"},"data":{"x":"` + canary + `"},"stringData":{"x":"` + canary + `"}}]}`, 200},
		{"secret_patch", "PATCH", "/api/v1/namespaces/app/secrets/s", `{"apiVersion":"v1","kind":"Secret","metadata":{"name":"s"},"data":{"x":"` + canary + `"},"stringData":{"x":"` + canary + `"}}`, 200},
		{"secret_watch", "GET", "/api/v1/namespaces/app/secrets?watch=true", `{"type":"ADDED","object":{"kind":"Secret","data":{"x":"` + canary + `"}}}`, 200},
		{"secret_legacy_watch", "GET", "/api/v1/watch/namespaces/app/secrets", `{"type":"ADDED","object":{"kind":"Secret","data":{"x":"` + canary + `"}}}`, 200},
		{"secret_table", "GET", "/api/v1/namespaces/app/secrets", `{"kind":"Table","apiVersion":"meta.k8s.io/v1","rows":[{"cells":["` + canary + `"],"object":{"kind":"Secret","data":{"x":"` + canary + `"}}}]}`, 200},
		{"protobuf", "GET", "/api/v1/namespaces/app/secrets", "k8s\x00" + canary, 200},
		{"status_error", "PATCH", "/api/v1/namespaces/app/secrets/s", `{"kind":"Status","apiVersion":"v1","message":"` + canary + `","details":{"causes":[{"message":"` + canary + `"}]}}`, 422},
		{"logs_capped_tail", "GET", "/api/v1/namespaces/app/pods/p/log", strings.Repeat("x", 100<<10) + canary, 200},
		{"logs_error", "GET", "/api/v1/namespaces/app/pods/p/log", `{"kind":"Status","message":"` + canary + `"}`, 403},
		{"logs_nonfollow", "GET", "/api/v1/namespaces/app/pods/p/log", "Authorization: Bearer " + canary, 200},
		{"logs_follow", "GET", "/api/v1/namespaces/app/pods/p/log?follow=true", canary, 200},
		{"exec_no_upgrade_error", "POST", "/api/v1/namespaces/app/pods/p/exec?command=" + canary, canary, 400},
		{"attach_no_upgrade_error", "POST", "/api/v1/namespaces/app/pods/p/attach", canary, 400},
		{"portforward_no_upgrade_error", "POST", "/api/v1/namespaces/app/pods/p/portforward", canary, 400},
		{"token_request", "POST", "/api/v1/namespaces/app/serviceaccounts/sa/token", `{"kind":"TokenRequest","apiVersion":"authentication.k8s.io/v1","status":{"token":"` + canary + `"}}`, 201},
		{"configmap_certificate", "GET", "/api/v1/namespaces/app/configmaps/kube-root-ca.crt", `{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"kube-root-ca.crt"},"data":{"ca.crt":"-----BEGIN CERTIFICATE-----\n` + canary + `\n-----END CERTIFICATE-----"}}`, 200},
		{"pod_command_and_env", "GET", "/api/v1/namespaces/app/pods/p", `{"apiVersion":"v1","kind":"Pod","metadata":{"name":"p","labels":{"free":"` + canary + `"}},"spec":{"containers":[{"command":["` + canary + `"],"env":[{"name":"MODE","value":"` + canary + `"}]}]}}`, 200},
		{"pod_args", "GET", "/api/v1/namespaces/app/pods/p", `{"apiVersion":"v1","kind":"Pod","metadata":{"name":"p"},"spec":{"containers":[{"name":"c","args":["--token=` + canary + `"]}]}}`, 200},
		{"secret_annotation", "GET", "/api/v1/namespaces/app/secrets/s", `{"apiVersion":"v1","kind":"Secret","metadata":{"name":"s","annotations":{"auth":"Bearer ` + canary + `"}},"data":{"x":"` + canary + `"}}`, 200},
		{"configmap_patch", "PATCH", "/api/v1/namespaces/app/configmaps/c", `{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"c"},"data":{"token":"` + canary + `"}}`, 200},
		{"event_error_message", "GET", "/api/v1/namespaces/app/events/e", `{"apiVersion":"v1","kind":"Event","metadata":{"name":"e"},"message":"request failed Authorization: Bearer ` + canary + `"}`, 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			store, err := apihistory.Open(dir, "synthetic", apihistory.Limits{})
			if err != nil {
				t.Fatal(err)
			}
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("Authorization", "Bearer "+canary)
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer up.Close()
			u, _ := url.Parse(up.URL)
			proxy := httptest.NewServer(kubeproxy.New(u, http.DefaultTransport, http.DefaultTransport, store))
			req, _ := http.NewRequest(tc.method, proxy.URL+tc.path, strings.NewReader(canary))
			req.Header.Set("Authorization", "Bearer "+canary)
			res, err := proxy.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(res.Body)
			res.Body.Close()
			proxy.Close()
			store.Close()
			if err != nil || string(body) != tc.body || res.StatusCode != tc.status {
				t.Fatal("pass-through changed")
			}
			if storedCanary(t, dir) {
				t.Error("synthetic credential reached storage")
			}
		})
	}
}

func TestUpgradePayloadNeverStored(t *testing.T) {
	for _, protocol := range []string{"websocket", "SPDY/3.1"} {
		t.Run(protocol, func(t *testing.T) {
			dir := t.TempDir()
			store, err := apihistory.Open(dir, "synthetic", apihistory.Limits{})
			if err != nil {
				t.Fatal(err)
			}
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, rw, err := w.(http.Hijacker).Hijack()
				if err != nil {
					t.Error(err)
					return
				}
				defer conn.Close()
				fmt.Fprintf(rw, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: %s\r\n\r\n", protocol)
				rw.Flush()
				b := make([]byte, len(canary))
				if _, err := io.ReadFull(rw, b); err == nil {
					rw.Write(b)
					rw.Flush()
				}
			}))
			defer up.Close()
			u, _ := url.Parse(up.URL)
			proxy := httptest.NewServer(kubeproxy.New(u, http.DefaultTransport, http.DefaultTransport, store))
			p, _ := url.Parse(proxy.URL)
			conn, err := net.DialTimeout("tcp", p.Host, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			conn.SetDeadline(time.Now().Add(3 * time.Second))
			fmt.Fprintf(conn, "GET /api/v1/namespaces/app/pods/p/exec?command=%s HTTP/1.1\r\nHost: %s\r\nConnection: Upgrade\r\nUpgrade: %s\r\n\r\n", canary, p.Host, protocol)
			rw := bufio.NewReader(conn)
			res, err := http.ReadResponse(rw, nil)
			if err != nil || res.StatusCode != 101 {
				t.Fatalf("upgrade: %v", err)
			}
			io.WriteString(conn, canary)
			b := make([]byte, len(canary))
			if _, err := io.ReadFull(rw, b); err != nil || string(b) != canary {
				t.Fatal("duplex changed")
			}
			conn.Close()
			proxy.Close()
			store.Close()
			if storedCanary(t, dir) {
				t.Error("upgrade credential stored")
			}
		})
	}
}

func TestProjectionTailLatency(t *testing.T) {
	// 3 MiB, below the supported 4 MiB cap. No actual credentials.
	item := `{"metadata":{"name":"p","namespace":"app"},"spec":{"containers":[{"name":"c","env":[{"name":"MODE","value":"` + strings.Repeat("x", 3072) + `"}]}]}}`
	payload := []byte(`{"apiVersion":"v1","kind":"PodList","items":[` + strings.TrimSuffix(strings.Repeat(item+",", 950), ",") + `]}`)
	dir := t.TempDir()
	store, _ := apihistory.Open(dir, "synthetic", apihistory.Limits{Queue: 1})
	defer store.Close()
	var total time.Duration
	for range 5 {
		start := time.Now()
		store.Record(apihistory.Event{Phase: "end", Verb: "list"}, payload)
		total += time.Since(start)
	}
	t.Logf("nonblocking Record mean for %d-byte PodList: %s", len(payload), total/5)
	if total/5 > 10*time.Millisecond {
		t.Fatal("large response projection blocked admission")
	}
}

func TestClusterWideListObservation(t *testing.T) {
	dir := t.TempDir()
	store, _ := apihistory.Open(dir, "synthetic", apihistory.Limits{})
	now := time.Now().UTC()
	store.Record(apihistory.Event{Phase: "end", Verb: "list", Resource: "pods", Status: 200, Started: now, Finished: now}, []byte(`{"apiVersion":"v1","kind":"PodList","items":[{"metadata":{"name":"p","namespace":"app"}}]}`))
	store.Close()
	var out bytes.Buffer
	err := operatorcli.RunEvidence([]string{"state", "Pod/app/p", "--at", now.Add(time.Second).Format(time.RFC3339Nano)}, func(string) string { return dir }, &out)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(out.Bytes(), []byte(`"last_observed"`)) {
		t.Errorf("compatible cluster-wide observation ignored: %s", &out)
	}
}

func TestMalformedTrailerDoesNotLogCredential(t *testing.T) {
	var stderr bytes.Buffer
	old := log.Writer()
	log.SetOutput(&stderr)
	defer log.SetOutput(old)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, rw, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		defer c.Close()
		fmt.Fprintf(rw, "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n1\r\nx\r\n0\r\nInvalid Trailer %s\r\n\r\n", canary)
		rw.Flush()
	}))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	dir := t.TempDir()
	store, err := apihistory.Open(dir, "synthetic", apihistory.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	proxy := httptest.NewServer(kubeproxy.New(u, http.DefaultTransport, http.DefaultTransport, store))
	defer proxy.Close()
	res, err := proxy.Client().Get(proxy.URL + "/api/v1/namespaces/app/pods")
	if err == nil {
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
	}
	proxy.Close()
	store.Close()
	if storedCanary(t, dir) {
		t.Error("malformed response credential reached storage")
	}
	h, err := apihistory.Read(dir)
	if err != nil {
		t.Fatal(err)
	}
	if h.Manifest.Dropped == 0 || h.Manifest.Complete {
		t.Fatal("body copy failure missing gap")
	}
	if bytes.Contains(stderr.Bytes(), []byte(canary)) {
		t.Errorf("upstream credential reached default stderr logger: %s", &stderr)
	}
}

func TestFailedRecordingReopenMarksGap(t *testing.T) {
	dir := t.TempDir()
	store, err := apihistory.Open(dir, "synthetic", apihistory.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	store.Close()
	// Simulate broken recorder storage while leaving its last manifest readable.
	if err := os.Remove(filepath.Join(dir, "blobs")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "blobs"), []byte("broken"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := apihistory.Open(dir, "synthetic", apihistory.Limits{}); err == nil {
		t.Fatal("expected storage failure")
	}
	// This is RunAPIProxy's storeErr != nil branch: forwarding with nil recorder.
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(201) }))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	proxy := httptest.NewServer(kubeproxy.New(u, http.DefaultTransport, http.DefaultTransport, nil))
	defer proxy.Close()
	res, err := proxy.Client().Post(proxy.URL+"/api/v1/namespaces/app/configmaps", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	h, err := apihistory.Read(dir)
	if err != nil {
		t.Fatal(err)
	}
	if h.Manifest.Complete || h.Manifest.Dropped == 0 {
		t.Errorf("unrecorded write not marked: %+v", h.Manifest)
	}
}

func TestMetrics404IsNotPodAbsence(t *testing.T) {
	dir := t.TempDir()
	store, _ := apihistory.Open(dir, "synthetic", apihistory.Limits{})
	now := time.Now().UTC()
	store.Record(apihistory.Event{Phase: "end", Started: now, Finished: now, Verb: "get", Group: "metrics.k8s.io", Version: "v1beta1", Resource: "pods", Namespace: "app", Name: "p", Status: 404}, nil)
	store.Close()
	var out bytes.Buffer
	if err := operatorcli.RunEvidence([]string{"state", "Pod/app/p", "--at", now.Add(time.Second).Format(time.RFC3339Nano)}, func(string) string { return dir }, &out); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(out.Bytes(), []byte(`"observed_absent":true`)) {
		t.Errorf("metrics absence became core Pod absence: %s", &out)
	}
}

func TestExecDryRunCannotHideFirstMutation(t *testing.T) {
	for _, subresource := range []string{"exec", "attach", "portforward"} {
		t.Run(subresource, func(t *testing.T) {
			dir := t.TempDir()
			store, _ := apihistory.Open(dir, "synthetic", apihistory.Limits{})
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/"+subresource) {
					w.WriteHeader(200)
					return
				}
				io.WriteString(w, `{"apiVersion":"v1","kind":"Pod","metadata":{"name":"p","namespace":"app"},"status":{"phase":"Running"}}`)
			}))
			defer up.Close()
			u, _ := url.Parse(up.URL)
			proxy := httptest.NewServer(kubeproxy.New(u, http.DefaultTransport, http.DefaultTransport, store))
			defer proxy.Close()
			call := func(method, path string) {
				req, _ := http.NewRequest(method, proxy.URL+path, nil)
				res, err := proxy.Client().Do(req)
				if err != nil {
					t.Fatal(err)
				}
				io.Copy(io.Discard, res.Body)
				res.Body.Close()
			}
			call("POST", "/api/v1/namespaces/app/pods/p/"+subresource+"?dryRun=All&command=true")
			call("GET", "/api/v1/namespaces/app/pods/p")
			call("PATCH", "/api/v1/namespaces/app/pods/p")
			store.Close()
			var out bytes.Buffer
			if err := operatorcli.RunEvidence([]string{"state", "Pod/app/p", "--at", "before-first-mutation"}, func(string) string { return dir }, &out); err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(out.Bytes(), []byte(`"last_observed"`)) {
				t.Errorf("post-exec read presented before first mutation: %s", &out)
			}
		})
	}
}

func TestSaturatedRecorderDoesNotDelayLargeForwarding(t *testing.T) {
	payload := strings.Repeat("x", 3<<20)
	dir := t.TempDir()
	store, err := apihistory.Open(dir, "synthetic", apihistory.Limits{Queue: 1, Bytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for range 500 {
		store.Record(apihistory.Event{Phase: "end", Verb: "get"}, nil)
	}
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, payload) }))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	proxy := httptest.NewServer(kubeproxy.New(u, http.DefaultTransport, http.DefaultTransport, store))
	defer proxy.Close()
	start := time.Now()
	res, err := proxy.Client().Get(proxy.URL + "/api/v1/pods")
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(res.Body)
	res.Body.Close()
	if err != nil || string(b) != payload {
		t.Fatal("saturation changed forwarding")
	}
	if time.Since(start) > time.Second {
		t.Fatal("saturated recorder blocked forwarding")
	}
	proxy.Close()
	store.Close()
	h, err := apihistory.Read(dir)
	if err != nil {
		t.Fatal(err)
	}
	if h.Manifest.Complete || h.Manifest.Dropped == 0 {
		t.Fatal("saturation missing gap")
	}
}

func TestStateMatchesExplicitGroupResourceAndListScope(t *testing.T) {
	for _, tc := range []struct {
		name, group, resource, selector, body string
		partial, positive, absent             bool
	}{
		{name: "foreign_positive", group: "metrics.k8s.io", resource: "pods", selector: "Pod/app/p", body: `{"apiVersion":"v1","kind":"Pod","metadata":{"name":"p","namespace":"app"}}`},
		{name: "qualified_apps", group: "apps", resource: "deployments", selector: "Deployment.apps/app/p", body: `{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"p","namespace":"app"}}`, positive: true},
		{name: "unsupported_group", group: "apps", resource: "deployments", selector: "Deployment.example.com/app/p", body: `{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"p","namespace":"app"}}`},
		{name: "partial_cluster_list_positive", resource: "pods", selector: "Pod/app/p", body: `{"apiVersion":"v1","kind":"PodList","metadata":{"continue":"redacted"},"items":[{"metadata":{"name":"p","namespace":"app"}}]}`, partial: true, positive: true},
		{name: "empty_cluster_list", resource: "pods", selector: "Pod/app/p", body: `{"apiVersion":"v1","kind":"PodList","items":[]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			store, err := apihistory.Open(dir, "synthetic", apihistory.Limits{})
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC()
			verb := "get"
			if strings.Contains(tc.body, "List") {
				verb = "list"
			}
			store.Record(apihistory.Event{Phase: "end", Started: now, Finished: now, Verb: verb, Group: tc.group, Resource: tc.resource, Status: 200, Partial: tc.partial}, []byte(tc.body))
			store.Close()
			var out bytes.Buffer
			if err := operatorcli.RunEvidence([]string{"state", tc.selector, "--at", now.Add(time.Second).Format(time.RFC3339Nano)}, func(string) string { return dir }, &out); err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(out.Bytes(), []byte(`"last_observed"`)) != tc.positive || bytes.Contains(out.Bytes(), []byte(`"observed_absent":true`)) != tc.absent {
				t.Fatalf("incorrect identity or scope: %s", &out)
			}
		})
	}
}
