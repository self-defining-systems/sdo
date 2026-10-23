package operatorcli_test

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sdo.dev/operator/operatorcli"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer/protobuf"

	"sdo.dev/operator/apihistory"
	"sdo.dev/operator/kubeproxy"
)

const retainedLeafCanary = "rv3-synthetic-credential-canary"

// Run actual forwarding and wait for all projection workers before examining
// every stored file. The fixtures contain synthetic credentials only.
func recordRetainedLeaf(t *testing.T, path, method, body, contentType, encoding string) []byte {
	t.Helper()
	dir := t.TempDir()
	store, err := apihistory.Open(dir, "synthetic", apihistory.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", contentType)
		if encoding != "" {
			w.Header().Set("Content-Encoding", encoding)
		}
		w.Header().Set("Authorization", "Bearer "+retainedLeafCanary)
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, body)
	}))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	transport := &http.Transport{DisableCompression: true}
	defer transport.CloseIdleConnections()
	proxy := httptest.NewServer(kubeproxy.New(u, transport, transport, store))
	defer proxy.Close()
	req, err := http.NewRequest(method, proxy.URL+path, strings.NewReader(retainedLeafCanary))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+retainedLeafCanary)
	res, err := (&http.Client{Transport: transport}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(res.Body)
	res.Body.Close()
	proxy.Close()
	store.Close()
	if err != nil || !bytes.Equal(b, []byte(body)) || res.StatusCode != http.StatusOK || res.Header.Get("Content-Type") != contentType || res.Header.Get("Content-Encoding") != encoding {
		t.Fatal("pass-through changed", err)
	}
	if err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		b, err := os.ReadFile(path)
		if err == nil && bytes.Contains(b, []byte(retainedLeafCanary)) {
			t.Error("retained leaf credential reached persisted file")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	h, err := apihistory.Read(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, cmd := range [][]string{{"history"}, {"state", "Pod/app/p", "--at", time.Now().UTC().Add(time.Second).Format(time.RFC3339Nano)}} {
		var out bytes.Buffer
		if err := operatorcli.RunEvidence(cmd, func(string) string { return dir }, &out); err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(out.Bytes(), []byte(retainedLeafCanary)) {
			t.Error("credential reached offline CLI")
		}
	}
	var projected []byte
	for _, e := range h.Events {
		if e.BodyHash != "" {
			projected = h.Body(e)
			var out bytes.Buffer
			if err := operatorcli.RunEvidence([]string{"show", "--event", fmt.Sprint(e.Sequence)}, func(string) string { return dir }, &out); err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(out.Bytes(), []byte(retainedLeafCanary)) {
				t.Error("credential reached evidence show")
			}
		}
	}
	if bytes.Contains(projected, []byte(retainedLeafCanary)) {
		t.Error("retained leaf credential persisted")
	}
	return projected
}

// RV3's seven credential assertions and two representation assertions must
// remain permanent; a positive field allowlist alone does not sanitize leaves.
func TestRetainedLeafCredentialInvariant(t *testing.T) {
	for _, tc := range []struct{ name, path, body string }{
		{"containers_readinessProbe_query", "/api/v1/namespaces/app/pods/p", `{"apiVersion":"v1","kind":"Pod","metadata":{"name":"p","namespace":"app"},"spec":{"containers":[{"name":"web","image":"example.invalid/web:v1","readinessProbe":{"httpGet":{"path":"/health?access_token=rv3-synthetic-credential-canary","port":8080,"scheme":"HTTP"}}}]}}`},
		{"containers_image_userinfo", "/api/v1/namespaces/app/pods/p", `{"apiVersion":"v1","kind":"Pod","metadata":{"name":"p","namespace":"app"},"spec":{"containers":[{"name":"web","image":"https://user:rv3-synthetic-credential-canary@registry.invalid/web:v1"}]}}`},
		{"initContainers_image_userinfo", "/api/v1/namespaces/app/pods/p", `{"apiVersion":"v1","kind":"Pod","metadata":{"name":"p","namespace":"app"},"spec":{"initContainers":[{"name":"web","image":"https://user:rv3-synthetic-credential-canary@registry.invalid/web:v1"}]}}`},
		{"containerStatuses_state_waiting_reason", "/api/v1/namespaces/app/pods/p", `{"apiVersion":"v1","kind":"Pod","metadata":{"name":"p","namespace":"app"},"status":{"containerStatuses":[{"name":"web","state":{"waiting":{"reason":"Authorization: Bearer rv3-synthetic-credential-canary"}}}]}}`},
		{"Pod_condition_reason", "/api/v1/namespaces/app/pods/p", `{"apiVersion":"v1","kind":"Pod","metadata":{"name":"p","namespace":"app"},"status":{"conditions":[{"type":"Ready","status":"False","reason":"Authorization: Bearer rv3-synthetic-credential-canary"}]}}`},
		{"Event_reason", "/api/v1/namespaces/app/events/p", `{"apiVersion":"v1","kind":"Event","metadata":{"name":"p","namespace":"app"},"reason":"Authorization: Bearer rv3-synthetic-credential-canary"}`},
		{"Deployment_template_image", "/apis/apps/v1/namespaces/app/deployments/p", `{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"p","namespace":"app"},"spec":{"template":{"spec":{"containers":[{"name":"web","image":"https://u:rv3-synthetic-credential-canary@registry.invalid/web"}],"initContainers":[{"name":"init","image":"https://u:rv3-synthetic-credential-canary@registry.invalid/init"}]}}}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Scan every file after recorder closure, as well as the projection.
			recordRetainedLeaf(t, tc.path, "GET", tc.body, "application/json", "")
		})
	}
}

func TestRepresentationSafetyInvariant(t *testing.T) {
	for _, tc := range []struct{ name, contentType, encoding string }{
		{"unknown_encoding", "application/json", "rv3-unknown"},
		{"unknown_mime", "application/x-rv3-unknown", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := `{"apiVersion":"v1","kind":"Pod","metadata":{"name":"p"},"spec":{"containers":[{"image":"` + retainedLeafCanary + `"}]}}`
			got := recordRetainedLeaf(t, "/api/v1/namespaces/app/pods/p", "GET", body, tc.contentType, tc.encoding)
			if len(got) != 0 {
				t.Error("unreviewed representation persisted")
			}
		})
	}
}

// Keep a small JSON constructor for the expanded leaf/context matrix below.
func leafJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestCredentialLeavesAcrossSharedShapes(t *testing.T) {
	for _, kind := range []string{"Pod", "Deployment", "StatefulSet", "ReplicaSet"} {
		for _, group := range []string{"containers", "initContainers"} {
			for _, leaf := range []string{"image", "readinessProbe", "livenessProbe", "startupProbe"} {
				for _, list := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/%s/%s/list=%t", kind, group, leaf, list), func(t *testing.T) {
						container := map[string]any{"name": "web", "image": "example.invalid/web:v1"}
						if leaf == "image" {
							container[leaf] = "https://user:" + retainedLeafCanary + "@registry.invalid/web:v1"
						} else {
							container[leaf] = map[string]any{"httpGet": map[string]any{"path": "/health/" + retainedLeafCanary + "?access_token=" + retainedLeafCanary + "#" + retainedLeafCanary, "port": 8080, "scheme": "HTTP"}, "periodSeconds": 5}
						}
						version, resource := "v1", "pods"
						spec := map[string]any{group: []any{container}}
						if kind != "Pod" {
							version, resource = "apps/v1", strings.ToLower(kind)+"s"
							spec = map[string]any{"template": map[string]any{"spec": spec}}
						}
						obj := map[string]any{"apiVersion": version, "kind": kind, "metadata": map[string]any{"name": "p"}, "spec": spec}
						path := "/api/" + version + "/namespaces/app/" + resource
						if kind != "Pod" {
							path = "/apis/" + version + "/namespaces/app/" + resource
						}
						if list {
							obj = map[string]any{"apiVersion": version, "kind": kind + "List", "items": []any{obj}}
						} else {
							path += "/p"
						}
						got := recordRetainedLeaf(t, path, "GET", leafJSON(t, obj), "application/json", "")
						if len(got) == 0 {
							t.Fatal("supported object lost")
						}
						if leaf != "image" && bytes.Contains(got, []byte(`"path":`)) {
							t.Error("HTTP probe path must remain opaque")
						}
						if leaf != "image" && (!bytes.Contains(got, []byte(`"port":8080`)) || !bytes.Contains(got, []byte(`"periodSeconds":5`)) || !bytes.Contains(got, []byte(`"scheme":"HTTP"`))) {
							t.Error("probe structure lost")
						}
					})
				}
			}
		}
	}
	for _, group := range []string{"containerStatuses", "initContainerStatuses"} {
		for _, state := range []string{"state", "lastState"} {
			for _, handler := range []string{"waiting", "terminated"} {
				t.Run(group+"/"+state+"/"+handler, func(t *testing.T) {
					obj := map[string]any{"apiVersion": "v1", "kind": "Pod", "metadata": map[string]any{"name": "p"}, "status": map[string]any{group: []any{map[string]any{
						"name": "web", "image": "user:" + retainedLeafCanary + "@registry.invalid/web:v1",
						state: map[string]any{handler: map[string]any{"reason": retainedLeafCanary, "exitCode": 1}},
					}}}}
					for _, method := range []string{"GET", "PATCH"} {
						got := recordRetainedLeaf(t, "/api/v1/namespaces/app/pods/p/status", method, leafJSON(t, obj), "application/json", "")
						if !bytes.Contains(got, []byte(`"reason":"redacted"`)) {
							t.Error("missing opaque reason")
						}
					}
				})
			}
		}
	}
	for _, kind := range []string{"Pod", "Deployment", "StatefulSet", "ReplicaSet", "Event"} {
		t.Run(kind+"/token_shaped_reason", func(t *testing.T) {
			version, path := "v1", "/api/v1/namespaces/app/"+strings.ToLower(kind)+"s/p"
			if kind != "Pod" && kind != "Event" {
				version, path = "apps/v1", "/apis/apps/v1/namespaces/app/"+strings.ToLower(kind)+"s/p"
			}
			obj := map[string]any{"apiVersion": version, "kind": kind, "metadata": map[string]any{"name": "p"}, "status": map[string]any{"conditions": []any{map[string]any{"type": "Ready", "status": "False", "reason": retainedLeafCanary}}}}
			if kind == "Event" {
				obj["reason"] = retainedLeafCanary
			}
			got := recordRetainedLeaf(t, path, "GET", leafJSON(t, obj), "application/json", "")
			if !bytes.Contains(got, []byte(`"reason":"redacted"`)) {
				t.Error("missing opaque reason")
			}
		})
	}
}

func TestConstrainedLeavesRejectArbitraryText(t *testing.T) {
	for _, tc := range []struct{ name, path, body string }{
		{"secretRef_invented_key", "/api/v1/namespaces/app/pods/p", `{"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "p", "namespace": "app"}, "spec": {"containers": [{"name": "web", "image": "example.invalid/web:v1", "envFrom": [{"secretRef": {"name": "settings", "key": "` + retainedLeafCanary + `"}}]}]}}`},
		{"configMapRef_invented_key", "/api/v1/namespaces/app/pods/p", `{"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "p", "namespace": "app"}, "spec": {"containers": [{"name": "web", "image": "example.invalid/web:v1", "envFrom": [{"configMapRef": {"name": "settings", "key": "` + retainedLeafCanary + `"}}]}]}}`},
		{"fieldRef_apiVersion", "/api/v1/namespaces/app/pods/p", `{"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "p", "namespace": "app"}, "spec": {"containers": [{"name": "web", "image": "example.invalid/web:v1", "env": [{"name": "MODE", "valueFrom": {"fieldRef": {"apiVersion": "` + retainedLeafCanary + `", "fieldPath": "metadata.name"}}}]}]}}`},
		{"resourceFieldRef_resource", "/api/v1/namespaces/app/pods/p", `{"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "p", "namespace": "app"}, "spec": {"containers": [{"name": "web", "image": "example.invalid/web:v1", "env": [{"name": "LIMIT", "valueFrom": {"resourceFieldRef": {"containerName": "web", "resource": "` + retainedLeafCanary + `", "divisor": "1"}}}]}]}}`},
		{"resourceFieldRef_divisor", "/api/v1/namespaces/app/pods/p", `{"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "p", "namespace": "app"}, "spec": {"containers": [{"name": "web", "image": "example.invalid/web:v1", "env": [{"name": "LIMIT", "valueFrom": {"resourceFieldRef": {"containerName": "web", "resource": "limits.cpu", "divisor": "` + retainedLeafCanary + `"}}}]}]}}`},
		{"hpa_invalid_quantity_value", "/apis/autoscaling/v2/namespaces/app/horizontalpodautoscalers/p", `{"apiVersion": "autoscaling/v2", "kind": "HorizontalPodAutoscaler", "metadata": {"name": "p", "namespace": "app"}, "spec": {"metrics": [{"type": "External", "external": {"metric": {"name": "q"}, "target": {"type": "Value", "value": "` + retainedLeafCanary + `"}}}]}}`},
		{"hpa_invalid_quantity_averageValue", "/apis/autoscaling/v2/namespaces/app/horizontalpodautoscalers/p", `{"apiVersion": "autoscaling/v2", "kind": "HorizontalPodAutoscaler", "metadata": {"name": "p", "namespace": "app"}, "spec": {"metrics": [{"type": "External", "external": {"metric": {"name": "q"}, "target": {"type": "Value", "averageValue": "` + retainedLeafCanary + `"}}}]}}`},
		{"invalid_np_cidr", "/apis/networking.k8s.io/v1/namespaces/app/networkpolicies/p", `{"apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy", "metadata": {"name": "p", "namespace": "app"}, "spec": {"ingress": [{"from": [{"ipBlock": {"cidr": "` + retainedLeafCanary + `", "except": ["` + retainedLeafCanary + `"]}}]}]}}`},
		{"invalid_np_policy_type", "/apis/networking.k8s.io/v1/namespaces/app/networkpolicies/p", `{"apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy", "metadata": {"name": "p", "namespace": "app"}, "spec": {"policyTypes": ["` + retainedLeafCanary + `"]}}`},
		{"invalid_np_port", "/apis/networking.k8s.io/v1/namespaces/app/networkpolicies/p", `{"apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy", "metadata": {"name": "p", "namespace": "app"}, "spec": {"egress": [{"ports": [{"port": "` + retainedLeafCanary + `", "protocol": "` + retainedLeafCanary + `"}]}]}}`},
		{"invalid_pdb_percent", "/apis/policy/v1/namespaces/app/poddisruptionbudgets/p", `{"apiVersion": "policy/v1", "kind": "PodDisruptionBudget", "metadata": {"name": "p", "namespace": "app"}, "spec": {"minAvailable": "` + retainedLeafCanary + `"}}`},
		{"invalid_pdb_policy", "/apis/policy/v1/namespaces/app/poddisruptionbudgets/p", `{"apiVersion": "policy/v1", "kind": "PodDisruptionBudget", "metadata": {"name": "p", "namespace": "app"}, "spec": {"unhealthyPodEvictionPolicy": "` + retainedLeafCanary + `"}}`},
		{"invalid_service_type", "/api/v1/namespaces/app/services/p", `{"apiVersion": "v1", "kind": "Service", "metadata": {"name": "p", "namespace": "app"}, "spec": {"type": "` + retainedLeafCanary + `"}}`},
		{"invalid_service_targetPort", "/api/v1/namespaces/app/services/p", `{"apiVersion": "v1", "kind": "Service", "metadata": {"name": "p", "namespace": "app"}, "spec": {"ports": [{"port": 80, "targetPort": "` + retainedLeafCanary + `"}]}}`},
		{"invalid_pod_phase", "/api/v1/namespaces/app/pods/p", `{"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "p", "namespace": "app"}, "status": {"phase": "` + retainedLeafCanary + `"}}`},
		{"invalid_pod_condition_status", "/api/v1/namespaces/app/pods/p", `{"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "p", "namespace": "app"}, "status": {"conditions": [{"type": "Ready", "status": "` + retainedLeafCanary + `"}]}}`},
		{"invalid_pod_resource", "/api/v1/namespaces/app/pods/p", `{"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "p", "namespace": "app"}, "spec": {"containers": [{"name": "web", "resources": {"requests": {"cpu": "` + retainedLeafCanary + `"}}}]}}`},
		{"invalid_event_timestamp", "/api/v1/namespaces/app/events/p", `{"apiVersion": "v1", "kind": "Event", "metadata": {"name": "p", "namespace": "app"}, "firstTimestamp": "` + retainedLeafCanary + `"}`},
		{"invalid_hpa_behavior", "/apis/autoscaling/v2/namespaces/app/horizontalpodautoscalers/p", `{"apiVersion": "autoscaling/v2", "kind": "HorizontalPodAutoscaler", "metadata": {"name": "p", "namespace": "app"}, "spec": {"behavior": {"scaleUp": {"selectPolicy": "` + retainedLeafCanary + `", "policies": [{"type": "` + retainedLeafCanary + `", "value": 1, "periodSeconds": 60}]}}}}`},
		{"selector_expression_operator", "/apis/apps/v1/namespaces/app/deployments/p", `{"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": "p", "namespace": "app"}, "spec": {"selector": {"matchExpressions": [{"key": "app", "operator": "` + retainedLeafCanary + `"}]}}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			recordRetainedLeaf(t, tc.path, "GET", tc.body, "application/json", "")
		})
	}
}

func TestRepresentationGatePreservesWireBytes(t *testing.T) {
	body := `{"apiVersion":"v1","kind":"Pod","metadata":{"name":"p"},"spec":{"containers":[{"image":"example.invalid/web:v1"}]}}`
	for _, tc := range []struct {
		contentType, encoding string
		available             bool
	}{
		{"application/json", "", true},
		{"application/json; charset=utf-8", "identity", true},
		{"application/json; charset=UTF-8", "Identity", true},
		{"", "", false},
		{"text/plain", "", false},
		{"application/vnd.kubernetes.protobuf", "", false},
		{"application/json; as=Table; g=meta.k8s.io; v=v1", "", false},
		{"application/json; charset=synthetic-unknown", "", false},
		{"application/json", "gzip", false},
		{"application/json", "identity, identity", false},
	} {
		t.Run(tc.contentType+"/"+tc.encoding, func(t *testing.T) {
			got := recordRetainedLeaf(t, "/api/v1/namespaces/app/pods/p", "GET", body, tc.contentType, tc.encoding)
			if (len(got) != 0) != tc.available {
				t.Errorf("body available=%t want=%t", len(got) != 0, tc.available)
			}
		})
	}
	var zipped bytes.Buffer
	z := gzip.NewWriter(&zipped)
	if _, err := z.Write([]byte(body)); err != nil {
		t.Fatal(err)
	}
	if err := z.Close(); err != nil {
		t.Fatal(err)
	}
	if got := recordRetainedLeaf(t, "/api/v1/namespaces/app/pods/p", "GET", zipped.String(), "application/json", "gzip"); len(got) != 0 {
		t.Error("compressed representation recorded")
	}
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"}, ObjectMeta: metav1.ObjectMeta{Name: "p"}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "web", Image: retainedLeafCanary}}}}
	var protobufBody bytes.Buffer
	if err := protobuf.NewSerializer(scheme, scheme).Encode(pod, &protobufBody); err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(protobufBody.Bytes(), []byte{'k', '8', 's', 0}) {
		t.Fatal("invalid protobuf fixture")
	}
	if got := recordRetainedLeaf(t, "/api/v1/namespaces/app/pods/p", "GET", protobufBody.String(), "application/vnd.kubernetes.protobuf", ""); len(got) != 0 {
		t.Error("protobuf representation recorded")
	}
}
