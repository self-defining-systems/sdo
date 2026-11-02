//go:build integration

package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/httpstream"
	"k8s.io/apimachinery/pkg/util/httpstream/spdy"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"net/http"
	"net/http/httptest"
	"sdo.dev/operator/sdk/program"
	"strings"
	"testing"
	"time"
)

func TestDatabaseCatalogTransportProjectionAndCanaries(t *testing.T) {
	const canary = "fake-password-MUST-NOT-LEAK"
	t.Setenv("APP_PASSWORD", canary)
	t.Setenv("ADMIN_PASSWORD", canary)
	t.Setenv("ADMIN_USERNAME", canary)
	for _, mode := range []string{"read", "ensure", "malformed", "denied", "missing-env", "authentication", "catalog-read", "client-binary", "duplicate-users"} {
		t.Run(mode, func(t *testing.T) {
			a := databaseAction()
			a.Target.Auth = &program.DatabaseAuth{Database: "admin", Username: program.CredentialRef{Env: "ADMIN_USERNAME"}, Password: program.CredentialRef{Env: "ADMIN_PASSWORD"}}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "GET" {
					w.Header().Set("Content-Type", "application/json")
					_ = json.NewEncoder(w).Encode(corev1.Pod{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"}, ObjectMeta: metav1.ObjectMeta{Name: "database", Namespace: "app", UID: "one", ResourceVersion: "1"}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "mongo"}}}})
					return
				}
				if mode == "denied" {
					http.Error(w, canary, http.StatusForbidden)
					return
				}
				q := r.URL.Query()
				command := q["command"]
				joined := strings.Join(command, " ")
				if strings.Contains(joined, canary) || !strings.Contains(joined, "--host 127.0.0.1 --port 27017 --eval") || strings.Contains(joined, "ADMIN_PASSWORD") {
					t.Errorf("command did not preserve reference-only noninteractive execution")
				}
				if q.Get("stdin") != "true" || q.Get("stderr") == "true" || q.Get("tty") == "true" || (q.Get("stdout") == "true") != (mode != "ensure") {
					http.Error(w, "incorrect stream contract", 400)
					return
				}
				if _, err := httpstream.Handshake(r, w, []string{"v4.channel.k8s.io"}); err != nil {
					t.Error(err)
					return
				}
				type ready struct {
					s       httpstream.Stream
					replied <-chan struct{}
				}
				streams := make(chan ready, 3)
				conn := spdy.NewResponseUpgrader().UpgradeResponse(w, r, func(s httpstream.Stream, replied <-chan struct{}) error { streams <- ready{s, replied}; return nil })
				if conn == nil {
					return
				}
				defer conn.Close()
				var input, output, status httpstream.Stream
				count := 3
				if mode == "ensure" {
					count = 2
				}
				for i := 0; i < count; i++ {
					select {
					case next := <-streams:
						<-next.replied
						switch next.s.Headers().Get(corev1.StreamType) {
						case corev1.StreamTypeStdin:
							input = next.s
						case corev1.StreamTypeStdout:
							output = next.s
						case corev1.StreamTypeError:
							status = next.s
						}
					case <-time.After(3 * time.Second):
						t.Error("stream timeout")
						return
					}
				}
				script, err := io.ReadAll(input)
				if err != nil || !strings.Contains(string(script), "ADMIN_PASSWORD") || strings.Contains(string(script), canary) {
					t.Errorf("invalid reference-only stdin script: %v", err)
				}
				if output != nil {
					body := `[{"name":"app","roles":[{"database":"app","name":"readWrite"}]}]`
					if mode == "duplicate-users" {
						body = `[{"name":"app","roles":[]},{"name":"app","roles":[]}]`
					}
					if mode == "malformed" {
						body = `[{"name":"app","roles":[],"password":"` + canary + `"}]`
					}
					_, _ = io.WriteString(output, body)
					_ = output.Close()
				}
				exitStatus := metav1.Status{Status: metav1.StatusSuccess}
				if mode == "missing-env" {
					exitStatus = metav1.Status{Status: metav1.StatusFailure, Reason: "NonZeroExitCode", Message: canary, Details: &metav1.StatusDetails{Causes: []metav1.StatusCause{{Type: "ExitCode", Message: "43"}}}}
				}
				for step, code := range map[string]string{"authentication": "60", "catalog-read": "61", "client-binary": "127"} {
					if mode == step {
						exitStatus = metav1.Status{Status: metav1.StatusFailure, Reason: "NonZeroExitCode", Message: canary, Details: &metav1.StatusDetails{Causes: []metav1.StatusCause{{Type: "ExitCode", Message: code}}}}
					}
				}
				_ = json.NewEncoder(status).Encode(exitStatus)
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
			c := TypedPlanClient{Client: client, ExecConfig: config}
			if mode == "ensure" {
				err = c.EnsureDatabaseUser(ctx, a)
			} else {
				var users []program.DatabaseUser
				users, err = c.DatabaseUsers(ctx, a.Target)
				if mode == "read" && (len(users) != 1 || users[0].Name != "app") {
					t.Fatalf("%+v", users)
				}
			}
			if (err != nil) != (mode != "read" && mode != "ensure") {
				t.Fatalf("%v", err)
			}
			if mode == "missing-env" {
				var refErr *CredentialReferenceError
				if !errors.As(err, &refErr) || refErr.Reference.Env != "ADMIN_PASSWORD" {
					t.Fatalf("missing env lacks typed reference: %v", err)
				}
			}
			if mode == "authentication" || mode == "catalog-read" || mode == "client-binary" {
				safe := databaseSafeError(err, "database catalog observation unavailable")
				if !strings.Contains(safe.Error(), mode) {
					t.Fatalf("failure step lost: %v", safe)
				}
			}
			if (mode == "malformed" || mode == "duplicate-users") && !strings.Contains(databaseSafeError(err, "unavailable").Error(), "catalog-parse") {
				t.Fatal("parse failure step lost")
			}
			if strings.Contains(fmt.Sprint(err), canary) {
				t.Fatal("transport leaked excluded field or credential")
			}
		})
	}
}

