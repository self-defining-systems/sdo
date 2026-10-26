package operatorcli

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func TestEvidenceExecUsesClientGoIdentityAndContext(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/api/v1/namespaces/sdo/pods/controller-1/exec" || r.Method != "POST" {
			t.Errorf("wrong exec request: %s %s", r.Method, r.URL)
		}
		if r.Header.Get("Authorization") != "Bearer synthetic" {
			t.Error("exec lost shared service-account identity")
		}
		q := r.URL.Query()
		if q.Get("stdout") != "true" || q.Get("stderr") != "true" || len(q["command"]) != 2 || q["command"][0] != "cat" {
			t.Errorf("exec options lost: %v", q)
		}
		http.Error(w, "upgrade unavailable", http.StatusBadRequest)
	}))
	defer server.Close()
	config := &rest.Config{Host: server.URL, BearerToken: "synthetic"}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	execute, err := newPodExecutor(config, client)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := execute(ctx, "sdo", "controller-1", []string{"cat", "/ledger"}, io.Discard, io.Discard); err == nil {
		t.Fatal("missing SPDY upgrade treated as success")
	}
	if calls.Load() != 1 {
		t.Fatalf("exec attempts %d", calls.Load())
	}
	canceled, stop := context.WithCancel(context.Background())
	stop()
	if err := execute(canceled, "sdo", "controller-1", []string{"cat", "/ledger"}, io.Discard, io.Discard); err == nil {
		t.Fatal("canceled exec succeeded")
	}
	if calls.Load() != 1 {
		t.Fatal("canceled exec reached API")
	}
}
