package operatorcli_test

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
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
				w.Header().Set("Content-Type", "application/json")
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

func TestLargeResponseAdmissionMarksGap(t *testing.T) {
	// 3 MiB, below the supported 4 MiB cap. No actual credentials.
	item := `{"metadata":{"name":"p","namespace":"app"},"spec":{"containers":[{"name":"c","env":[{"name":"MODE","value":"` + strings.Repeat("x", 3072) + `"}]}]}}`
	payload := []byte(`{"apiVersion":"v1","kind":"PodList","items":[` + strings.TrimSuffix(strings.Repeat(item+",", 950), ",") + `]}`)
	dir := t.TempDir()
	store, _ := apihistory.Open(dir, "synthetic", apihistory.Limits{Queue: 1})
	defer store.Close()
	for range 5 {
		store.Record(apihistory.Event{Phase: "end", Verb: "list"}, payload)
	}
	store.Close()
	history, err := apihistory.Read(dir)
	if err != nil {
		t.Fatal(err)
	}
	if history.Manifest.Complete || history.Manifest.Dropped == 0 || len(history.Events) == 0 {
		t.Fatalf("missing bounded-admission gap: %+v", history.Manifest)
	}
	for _, event := range history.Events {
		if event.BodyHash != "" || !event.Partial || event.Omission != "redacted_projection_budget" {
			t.Fatalf("oversized body admitted: %+v", event)
		}
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
				w.Header().Set("Content-Type", "application/json")
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
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, payload)
	}))
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

// Reviewed projections are exercised through the HTTP proxy and persisted store,
// for both objects and lists. Everything here is minimal synthetic test data.
func TestOperationalFieldAllowlist(t *testing.T) {
	container := `{"name":"web","image":"example.invalid/web:v1","ports":[{"name":"http","containerPort":8080,"protocol":"TCP"}],"readinessProbe":{"httpGet":{"port":"http","scheme":"HTTP"},"initialDelaySeconds":2,"periodSeconds":3,"timeoutSeconds":1,"successThreshold":1,"failureThreshold":2},"livenessProbe":{"exec":{}},"startupProbe":{"tcpSocket":{"port":8080}},"resources":{"requests":{"cpu":"100m","memory":"32Mi"},"limits":{"cpu":"1","memory":"64Mi"}},"env":[{"name":"MODE","valueFrom":{"secretKeyRef":{"name":"settings","key":"mode","optional":false}}}],"envFrom":[{"configMapRef":{"name":"settings","optional":true}}],"volumeMounts":[{"name":"config"}]}`
	podSpec := `{"nodeName":"node-a","containers":[` + container + `],"initContainers":[` + container + `],"volumes":[{"name":"config"}]}`
	conditions := `[{"type":"Ready","status":"False","reason":"ContainersNotReady"}]`
	workload := `{"spec":{"replicas":2,"selector":{"matchLabels":{"app":""},"matchExpressions":[{"key":"tier","operator":"In"}]},"template":{"metadata":{"labels":{"app":""}},"spec":` + podSpec + `}},"status":{"replicas":2,"readyReplicas":1,"availableReplicas":1,"updatedReplicas":2,"conditions":` + conditions + `}}`
	for _, tc := range []struct{ kind, version, resource, want string }{
		{"Service", "v1", "services", `{"spec":{"type":"ClusterIP","selector":{"app":""},"ports":[{"name":"http","port":80,"targetPort":"http","protocol":"TCP","nodePort":30080}],"clusterIPPresent":true}}`},
		{"Deployment", "apps/v1", "deployments", workload},
		{"StatefulSet", "apps/v1", "statefulsets", workload},
		{"ReplicaSet", "apps/v1", "replicasets", workload},
		{"Pod", "v1", "pods", `{"spec":` + podSpec + `,"status":{"phase":"Running","podIPPresent":true,"conditions":` + conditions + `,"containerStatuses":[{"name":"web","image":"example.invalid/web:v1","ready":false,"restartCount":3,"state":{"waiting":{"reason":"CrashLoopBackOff"}},"lastState":{"terminated":{"reason":"Error","exitCode":1}}}],"initContainerStatuses":[{"name":"init","image":"example.invalid/init:v1","ready":true,"restartCount":0,"state":{"terminated":{"reason":"Completed","exitCode":0}}}]}}`},
		{"Endpoints", "v1", "endpoints", `{"subsets":[{"readyAddressCount":2,"notReadyAddressCount":1,"ports":[{"name":"http","port":8080,"protocol":"TCP"}]}]}`},
		{"EndpointSlice", "discovery.k8s.io/v1", "endpointslices", `{"readyAddressCount":2,"notReadyAddressCount":1,"ports":[{"name":"http","port":8080,"protocol":"TCP"}]}`},
		{"Event", "v1", "events", `{"type":"Warning","reason":"BackOff","involvedObject":{"kind":"Pod","namespace":"app","name":"p","apiVersion":"v1"},"count":3,"firstTimestamp":"2026-01-01T00:00:00Z","lastTimestamp":"2026-01-01T00:01:00Z","eventTime":"2026-01-01T00:01:00Z","series":{"count":2,"lastObservedTime":"2026-01-01T00:01:00Z"}}`},
		{"HorizontalPodAutoscaler", "autoscaling/v2", "horizontalpodautoscalers", `{"spec":{"scaleTargetRef":{"apiVersion":"apps/v1","kind":"Deployment","name":"web"},"minReplicas":1,"maxReplicas":4,"metrics":[{"type":"Resource","resource":{"name":"cpu","target":{"type":"Utilization","averageUtilization":70}}}],"behavior":{"scaleUp":{"stabilizationWindowSeconds":30,"selectPolicy":"Max","policies":[{"type":"Pods","value":2,"periodSeconds":60}]}}}}`},
		{"PodDisruptionBudget", "policy/v1", "poddisruptionbudgets", `{"spec":{"minAvailable":"50%","maxUnavailable":1,"unhealthyPodEvictionPolicy":"IfHealthyBudget","selector":{"matchLabels":{"app":""}}}}`},
		{"NetworkPolicy", "networking.k8s.io/v1", "networkpolicies", `{"spec":{"podSelector":{"matchLabels":{"app":""}},"policyTypes":["Ingress","Egress"],"ingress":[{"from":[{"namespaceSelector":{"matchLabels":{"team":""}},"podSelector":{"matchLabels":{"app":""}},"ipBlock":{"cidr":"192.0.2.0/24","except":["192.0.2.1/32"]}}],"ports":[{"protocol":"TCP","port":"http","endPort":8081}]}],"egress":[{"to":[{"ipBlock":{"cidr":"198.51.100.0/24"}}],"ports":[{"port":443}]}]}}`},
		{"Secret", "v1", "secrets", `{"data":{"token":""},"stringData":{"token":""},"binaryData":{"token":""}}`},
		{"ConfigMap", "v1", "configmaps", `{"data":{"token":""},"stringData":{"token":""},"binaryData":{"token":""}}`},
	} {
		for _, list := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/list=%t", tc.kind, list), func(t *testing.T) {
				var want map[string]any
				if err := json.Unmarshal([]byte(tc.want), &want); err != nil {
					t.Fatal(err)
				}
				if tc.kind == "Deployment" {
					want["spec"].(map[string]any)["strategy"] = map[string]any{"type": "RollingUpdate"}
				}
				if tc.kind == "StatefulSet" {
					want["spec"].(map[string]any)["updateStrategy"] = map[string]any{"type": "RollingUpdate"}
				}
				want["apiVersion"], want["kind"], want["redacted"] = tc.version, tc.kind, true
				meta := map[string]any{"name": "p", "namespace": "app", "resourceVersion": "1"}
				if tc.kind != "Secret" && tc.kind != "ConfigMap" {
					meta["labels"] = map[string]any{"app": ""}
				}
				if tc.kind == "Pod" {
					meta["ownerReferences"] = []any{map[string]any{"kind": "ReplicaSet", "name": "web"}}
				}
				want["metadata"] = meta
				b, _ := json.Marshal(want)
				var input map[string]any
				json.Unmarshal(b, &input)
				// Every kind, including unsupported placements, gets every excluded
				// channel. Recursing also protects templates, init containers,
				// conditions, previous termination state, and probe exec handlers.
				poisonExcludedFields(input)
				// Also exercise all credential channels on kinds whose real API
				// has no pod spec: an unreviewed spec must stay wholly opaque.
				spec, ok := input["spec"].(map[string]any)
				if !ok {
					spec = map[string]any{}
					input["spec"] = spec
				}
				if _, exists := spec["containers"]; !exists {
					spec["containers"] = []any{map[string]any{
						"name": "excluded", "env": []any{map[string]any{"name": "TOKEN", "value": canary}},
						"command": []any{canary}, "args": []any{canary},
						"readinessProbe": map[string]any{"exec": map[string]any{"command": []any{canary}}},
					}}
				}
				switch tc.kind {
				case "Service":
					input["spec"].(map[string]any)["clusterIP"] = "192.0.2.10"
				case "Pod":
					input["status"].(map[string]any)["podIP"] = "192.0.2.20"
				case "Endpoints":
					subset := input["subsets"].([]any)[0].(map[string]any)
					subset["addresses"] = []any{map[string]any{"ip": "192.0.2.1"}, map[string]any{"ip": "192.0.2.2"}}
					subset["notReadyAddresses"] = []any{map[string]any{"ip": "192.0.2.3"}}
				case "EndpointSlice":
					input["endpoints"] = []any{map[string]any{"addresses": []any{"192.0.2.1", "192.0.2.2"}, "conditions": map[string]any{"ready": true}}, map[string]any{"addresses": []any{"192.0.2.3"}, "conditions": map[string]any{"ready": false}}}
				}
				// Derived presence/count fields must come from the real fields.
				delete(input, "readyAddressCount")
				delete(input, "notReadyAddressCount")
				if spec, ok := input["spec"].(map[string]any); ok {
					delete(spec, "clusterIPPresent")
				}
				if status, ok := input["status"].(map[string]any); ok {
					delete(status, "podIPPresent")
				}
				if subsets, ok := input["subsets"].([]any); ok {
					for _, s := range subsets {
						delete(s.(map[string]any), "readyAddressCount")
						delete(s.(map[string]any), "notReadyAddressCount")
					}
				}
				var payload any = input
				if list {
					payload = map[string]any{"apiVersion": tc.version, "kind": tc.kind + "List", "items": []any{input}}
				}
				raw, _ := json.Marshal(payload)
				dir := t.TempDir()
				store, err := apihistory.Open(dir, "synthetic", apihistory.Limits{})
				if err != nil {
					t.Fatal(err)
				}
				up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					w.Write(raw)
				}))
				defer up.Close()
				u, _ := url.Parse(up.URL)
				proxy := httptest.NewServer(kubeproxy.New(u, http.DefaultTransport, http.DefaultTransport, store))
				path := "/api/" + tc.version + "/namespaces/app/" + tc.resource
				if strings.Contains(tc.version, "/") {
					path = "/apis/" + tc.version + "/namespaces/app/" + tc.resource
				}
				if !list {
					path += "/p"
				}
				res, err := proxy.Client().Get(proxy.URL + path)
				if err != nil {
					t.Fatal(err)
				}
				body, err := io.ReadAll(res.Body)
				res.Body.Close()
				proxy.Close()
				store.Close()
				if err != nil || !bytes.Equal(body, raw) {
					t.Fatal("pass-through changed")
				}
				if storedCanary(t, dir) {
					t.Error("excluded field canary reached persisted files")
				}
				h, err := apihistory.Read(dir)
				if err != nil {
					t.Fatal(err)
				}
				var got map[string]any
				for _, e := range h.Events {
					if e.BodyHash != "" {
						if err := json.Unmarshal(h.Body(e), &got); err != nil {
							t.Fatal(err)
						}
					}
				}
				if list && got != nil {
					got = got["items"].([]any)[0].(map[string]any)
				}
				if !reflect.DeepEqual(got, want) {
					t.Errorf("projection mismatch\ngot: %v\nwant: %v", got, want)
				}
			})
		}
	}
}

func poisonExcludedFields(v any) {
	switch x := v.(type) {
	case []any:
		for _, child := range x {
			poisonExcludedFields(child)
		}
	case map[string]any:
		for key, child := range x {
			switch key {
			case "labels", "matchLabels", "data", "stringData", "binaryData":
				for k := range child.(map[string]any) {
					child.(map[string]any)[k] = canary
				}
			case "selector":
				values := child.(map[string]any)
				if _, structured := values["matchLabels"]; structured {
					poisonExcludedFields(child)
				} else {
					for k := range values {
						values[k] = canary
					}
				}
			default:
				poisonExcludedFields(child)
			}
		}
		for _, key := range []string{"annotation", "message", "unknownFutureField", "host", "mountPath", "subPath", "prefix", "containerID", "imageID"} {
			x[key] = canary
		}
		x["annotations"] = map[string]any{"auth": canary}
		x["command"], x["args"] = []any{canary}, []any{canary}
		if _, exists := x["operator"]; exists {
			x["values"] = []any{canary}
		}
		if _, exists := x["status"]; !exists {
			x["status"] = map[string]any{"message": canary}
		}
		x["httpHeaders"] = []any{map[string]any{"name": "Authorization", "value": canary}}
		if _, exists := x["valueFrom"]; exists {
			x["value"] = canary
		}
		if _, exists := x["env"]; !exists {
			x["env"] = []any{map[string]any{"name": "EXCLUDED", "value": canary}}
		}
	}
}

func TestEvidenceRewindsOperationalFieldsBeforeFirstMutation(t *testing.T) {
	for _, tc := range []struct{ kind, path, before, after string }{
		{"Service", "/api/v1/namespaces/app/p", `"spec":{"ports":[{"port":80,"targetPort":8080}]}`, `"spec":{"ports":[{"port":80,"targetPort":9090}]}`},
		{"Deployment", "/apis/apps/v1/namespaces/app/p", `"spec":{"template":{"spec":{"containers":[{"name":"web","image":"example.invalid/web:old"}]}}}`, `"spec":{"template":{"spec":{"containers":[{"name":"web","image":"example.invalid/web:new"}]}}}`},
		{"Pod", "/api/v1/namespaces/app/p", `"spec":{"containers":[{"name":"web","image":"example.invalid/web:old","ports":[{"containerPort":8080}],"readinessProbe":{"httpGet":{"port":8080,"scheme":"HTTP"},"periodSeconds":5}}],"initContainers":[{"name":"init","image":"example.invalid/init:old","ports":[{"containerPort":9090}],"startupProbe":{"tcpSocket":{"port":9090},"timeoutSeconds":2}}]},"status":{"conditions":[{"type":"Ready","status":"False","reason":"ReadinessGatesNotReady"}],"initContainerStatuses":[{"name":"init","image":"example.invalid/init:old","ready":false,"restartCount":2,"state":{"waiting":{"reason":"CrashLoopBackOff"}},"lastState":{"terminated":{"reason":"Error","exitCode":1}}}]}`, `"spec":{"containers":[{"name":"web","image":"example.invalid/web:new","ports":[{"containerPort":8181}],"readinessProbe":{"httpGet":{"port":8181,"scheme":"HTTPS"},"periodSeconds":10}}],"initContainers":[{"name":"init","image":"example.invalid/init:new","ports":[{"containerPort":9191}],"startupProbe":{"tcpSocket":{"port":9191},"timeoutSeconds":3}}]},"status":{"conditions":[{"type":"Ready","status":"True","reason":"ContainersReady"}],"initContainerStatuses":[{"name":"init","image":"example.invalid/init:new","ready":true,"restartCount":2,"state":{"terminated":{"reason":"Completed","exitCode":0}}}]}`},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			dir := t.TempDir()
			store, err := apihistory.Open(dir, "synthetic", apihistory.Limits{})
			if err != nil {
				t.Fatal(err)
			}
			version, resource := "v1", "services"
			if tc.kind == "Deployment" {
				version, resource = "apps/v1", "deployments"
			} else if tc.kind == "Pod" {
				version, resource = "v1", "pods"
			}
			path := strings.TrimSuffix(tc.path, "p") + resource + "/p"
			var changed atomic.Bool
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.Method == "PATCH" {
					changed.Store(true)
				}
				fields := tc.before
				if changed.Load() {
					fields = tc.after
				}
				fmt.Fprintf(w, `{"apiVersion":%q,"kind":%q,"metadata":{"name":"p","namespace":"app"},%s}`, version, tc.kind, fields)
			}))
			defer up.Close()
			u, _ := url.Parse(up.URL)
			proxy := httptest.NewServer(kubeproxy.New(u, http.DefaultTransport, http.DefaultTransport, store))
			for _, method := range []string{"GET", "PATCH", "GET"} {
				req, _ := http.NewRequest(method, proxy.URL+path, nil)
				res, err := proxy.Client().Do(req)
				if err != nil {
					t.Fatal(err)
				}
				io.Copy(io.Discard, res.Body)
				res.Body.Close()
			}
			proxy.Close()
			store.Close()
			for _, cut := range []string{"before-first-mutation", time.Now().UTC().Add(time.Second).Format(time.RFC3339Nano)} {
				var out bytes.Buffer
				if err := operatorcli.RunEvidence([]string{"state", tc.kind + "/app/p", "--at", cut}, func(string) string { return dir }, &out); err != nil {
					t.Fatal(err)
				}
				want := tc.before
				if cut != "before-first-mutation" {
					want = tc.after
				}
				var result struct {
					LastObserved map[string]any `json:"last_observed"`
					Unknown      bool           `json:"unknown"`
				}
				if err := json.Unmarshal(out.Bytes(), &result); err != nil {
					t.Fatal(err)
				}
				var expected map[string]any
				if err := json.Unmarshal([]byte("{"+want+"}"), &expected); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(result.LastObserved["spec"], expected["spec"]) || !reflect.DeepEqual(result.LastObserved["status"], expected["status"]) || !result.Unknown || result.LastObserved["redacted"] != true {
					t.Errorf("wrong historical operational fields: %s", &out)
				}
			}
		})
	}
}
