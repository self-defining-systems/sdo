package kubeproxy

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestProxySecretReadsNeverReachUpstream(t *testing.T) {
	var calls atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Write([]byte(`{"data":{"value":"canary"}}`))
	}))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	handler := New(u, http.DefaultTransport, http.DefaultTransport, nil)
	for _, path := range []string{"/api/v1/secrets", "/api/v1/namespaces/app/secrets", "/api/v1/namespaces/app/secrets/name", "/api/v1/watch/namespaces/app/secrets"} {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		var status metav1.Status
		json.Unmarshal(w.Body.Bytes(), &status)
		if !apierrors.IsForbidden(&apierrors.StatusError{ErrStatus: status}) || strings.Contains(w.Body.String(), "canary") {
			t.Fatalf("Secret read escaped: %s", w.Body.String())
		}
	}
	if calls.Load() != 0 {
		t.Fatal("Secret reads reached upstream")
	}
	_ = context.Background()
}
