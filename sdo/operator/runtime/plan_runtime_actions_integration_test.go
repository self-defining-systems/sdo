//go:build integration

package runtime

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/httpstream"
	"k8s.io/apimachinery/pkg/util/httpstream/spdy"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sdo.dev/operator/sdk"
)

func TestXV2TypedExecUsesRBACAndRequestsNoOutput(t *testing.T) {
	called := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		if r.URL.Path != "/api/v1/namespaces/app/pods/one/exec" {
			t.Errorf("unexpected exec path %s", r.URL.Path)
		}
		for _, stream := range []string{"stdout", "stderr", "tty"} {
			if r.URL.Query().Get(stream) == "true" {
				t.Errorf("Exec enabled %s", stream)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden","message":"no controller grant","code":403}`))
	}))
	defer server.Close()
	config := &rest.Config{Host: server.URL}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	code, err := (TypedPlanClient{Client: client, ExecConfig: config}).Exec(context.Background(), sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "one"}, "app", []string{"/bin/true"})
	if !called || !apierrors.IsForbidden(err) || code != 0 {
		t.Fatalf("exec RBAC boundary: called=%t code=%d err=%v", called, code, err)
	}
}

func TestXV2TypedExecCompletesWithOnlyExitStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("stdin") != "true" || r.URL.Query().Get("stdout") == "true" || r.URL.Query().Get("stderr") == "true" || r.URL.Query().Get("tty") == "true" {
			http.Error(w, "exec requires a closed input stream and no output", http.StatusBadRequest)
			return
		}
		if _, err := httpstream.Handshake(r, w, []string{"v4.channel.k8s.io"}); err != nil {
			t.Error(err)
			return
		}
		type readyStream struct {
			stream  httpstream.Stream
			replied <-chan struct{}
		}
		streams := make(chan readyStream, 2)
		conn := spdy.NewResponseUpgrader().UpgradeResponse(w, r, func(stream httpstream.Stream, replySent <-chan struct{}) error {
			streams <- readyStream{stream, replySent}
			return nil
		})
		if conn == nil {
			return
		}
		defer conn.Close()
		var status, input httpstream.Stream
		for i := 0; i < 2; i++ {
			select {
			case next := <-streams:
				<-next.replied
				switch next.stream.Headers().Get(corev1.StreamType) {
				case corev1.StreamTypeError:
					status = next.stream
				case corev1.StreamTypeStdin:
					input = next.stream
				default:
					t.Errorf("unexpected output stream")
				}
			case <-time.After(3 * time.Second):
				t.Error("streams not opened")
				return
			}
		}
		if status == nil || input == nil {
			t.Error("status or input stream absent")
			return
		}
		if count, err := io.Copy(io.Discard, input); err != nil || count != 0 {
			t.Errorf("stdin must be EOF: count=%d err=%v", count, err)
			return
		}
		_ = json.NewEncoder(status).Encode(metav1.Status{Status: metav1.StatusFailure, Reason: "NonZeroExitCode", Details: &metav1.StatusDetails{Causes: []metav1.StatusCause{{Type: "ExitCode", Message: "7"}}}})
		_ = status.Close()
		select {
		case <-conn.CloseChan():
		case <-time.After(3 * time.Second):
		}
	}))
	defer server.Close()
	config := &rest.Config{Host: server.URL}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	code, err := (TypedPlanClient{Client: client, ExecConfig: config}).Exec(ctx, sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "one"}, "app", []string{"true"})
	if err != nil || code != 7 {
		t.Fatalf("output-free upgrade: code=%d err=%v", code, err)
	}
}
