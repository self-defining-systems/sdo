package runtime

import (
	"context"
	"encoding/json"
	"k8s.io/client-go/rest"
	"net/http"
	"net/http/httptest"
	"sdo.dev/operator/sdk/program"
	"strings"
	"sync/atomic"
	"testing"
)

func TestGeneralAPIProductionPathAndSecretBoundary(t *testing.T) {
	var calls atomic.Int32
	const canary = "synthetic-controller-token"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("Authorization") != "Bearer "+canary {
			t.Error("API authentication missing")
		}
		if r.Method != "PATCH" || r.URL.Path != "/api/v1/namespaces/app/configmaps/flags" || r.URL.Query().Get("fieldManager") != "fixture" || r.Header.Get("Content-Type") != "application/apply-patch+yaml" {
			t.Error("API options lost")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"kind":"ConfigMap","metadata":{"name":"flags"}}`))
	}))
	defer server.Close()
	c := TypedPlanClient{ExecConfig: &rest.Config{Host: server.URL, BearerToken: canary}}
	q := program.KubernetesRequest{Method: "PATCH", Path: "/api/v1/namespaces/app/configmaps/flags", Query: map[string]string{"fieldManager": "fixture"}, ContentType: "application/apply-patch+yaml", Body: json.RawMessage(`{"kind":"ConfigMap"}`)}
	if body, err := c.KubernetesRequest(context.Background(), q); err != nil || !json.Valid(body) {
		t.Fatalf("API request failed: %v", err)
	}
	for _, path := range []string{"/api/v1/namespaces/app/secrets/a", "/api/v1/secrets", "/api/v1/watch/secrets", "/api/v1/watch/namespaces/app/secrets", "/api/v1/namespaces/app/%73ecrets/a"} {
		q.Method = "GET"
		q.Path = path
		if _, err := c.KubernetesRequest(context.Background(), q); err == nil {
			t.Errorf("Secret endpoint accepted: %s", path)
		}
	}
	if calls.Load() != 1 {
		t.Fatal("Secret request crossed API boundary")
	}
}
func TestGeneralAPIRedirectAndFailureDiscardCredentials(t *testing.T) {
	const canary = "synthetic-private-body"
	var redirected atomic.Int32
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { redirected.Add(1); _, _ = w.Write([]byte(`{}`)) }))
	defer other.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "redirect") {
			http.Redirect(w, r, other.URL, http.StatusTemporaryRedirect)
		} else {
			http.Error(w, canary, http.StatusForbidden)
		}
	}))
	defer server.Close()
	c := TypedPlanClient{ExecConfig: &rest.Config{Host: server.URL, BearerToken: canary}}
	for _, name := range []string{"redirect", "forbidden"} {
		_, err := c.KubernetesRequest(context.Background(), program.KubernetesRequest{Method: "GET", Path: "/api/v1/namespaces/app/configmaps/" + name})
		if err == nil || strings.Contains(err.Error(), canary) {
			t.Fatal("HTTP failure leaked body or became success")
		}
	}
	if redirected.Load() != 0 {
		t.Fatal("forwarded API credentials through redirect")
	}
}
func TestGeneralTokenRequestDoesNotReadCredentialResponse(t *testing.T) {
	const canary = "synthetic-created-token"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"kind":"TokenRequest","status":{"token":"` + canary + `"}}`))
	}))
	defer server.Close()
	c := TypedPlanClient{ExecConfig: &rest.Config{Host: server.URL}}
	q := program.KubernetesRequest{Method: "POST", Path: "/api/v1/namespaces/app/serviceaccounts/default/token", Body: json.RawMessage(`{"apiVersion":"authentication.k8s.io/v1","kind":"TokenRequest","spec":{"audiences":["fixture"]}}`)}
	result, err := c.KubernetesRequest(context.Background(), q)
	if err != nil || strings.Contains(string(result), canary) {
		t.Fatal("generated credential crossed API boundary")
	}
}
