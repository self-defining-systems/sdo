package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/quick"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/httpstream"
	"k8s.io/apimachinery/pkg/util/httpstream/spdy"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func TestGeneralCommandTransportStreamsOnlyInput(t *testing.T) {
	const input = "synthetic script or tar input\x00fixture"
	command := program.Command{Pod: sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "one"}, Container: "app", Args: []string{"sh", "-s"}, Stdin: input}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if r.Method != "POST" || r.URL.Path != "/api/v1/namespaces/app/pods/one/exec" || q.Get("stdin") != "true" || q.Get("stdout") == "true" || q.Get("stderr") == "true" || q.Get("tty") == "true" {
			t.Error("invalid command stream request")
			http.Error(w, "invalid", 400)
			return
		}
		if got := q["command"]; len(got) != 2 || got[0] != "sh" || got[1] != "-s" {
			t.Error("command args changed")
		}
		if _, err := httpstream.Handshake(r, w, []string{"v4.channel.k8s.io"}); err != nil {
			t.Error(err)
			return
		}
		type ready struct {
			s       httpstream.Stream
			replied <-chan struct{}
		}
		streams := make(chan ready, 2)
		conn := spdy.NewResponseUpgrader().UpgradeResponse(w, r, func(s httpstream.Stream, replied <-chan struct{}) error { streams <- ready{s, replied}; return nil })
		if conn == nil {
			return
		}
		defer conn.Close()
		var stdin, status httpstream.Stream
		for i := 0; i < 2; i++ {
			select {
			case next := <-streams:
				<-next.replied
				switch next.s.Headers().Get(corev1.StreamType) {
				case corev1.StreamTypeStdin:
					stdin = next.s
				case corev1.StreamTypeError:
					status = next.s
				default:
					t.Error("unexpected output stream")
				}
			case <-time.After(3 * time.Second):
				t.Error("stream timeout")
				return
			}
		}
		raw, err := io.ReadAll(stdin)
		if err != nil || string(raw) != input {
			t.Error("stdin was not preserved")
		}
		_ = json.NewEncoder(status).Encode(map[string]any{"status": "Success"})
		_ = status.Close()
	}))
	defer server.Close()
	config := &rest.Config{Host: server.URL}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	c := TypedPlanClient{Client: client, ExecConfig: config}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if code, err := c.RunOperation(ctx, program.ExternalOperation{Exec: &command}); err != nil || code != 0 {
		t.Fatalf("exec transport: code=%d err=%v", code, err)
	}
}
func TestGeneralHTTPOnlySerializesCredentialReference(t *testing.T) {
	h := program.HTTPRequest{Pod: sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "api"}, Container: "app", Method: "POST", URL: "http://api.app.svc/admin", Body: "fixture", Bearer: &program.CredentialRef{Env: "ADMIN_TOKEN"}}
	property := func(secret uint64) bool {
		canary := fmt.Sprintf("synthetic-secret-%016x", secret)
		t.Setenv("ADMIN_TOKEN", canary)
		command := httpCommand(h)
		raw, err := json.Marshal(command)
		return err == nil && !strings.Contains(string(raw), canary) && strings.Contains(string(raw), "ADMIN_TOKEN") && command.Stdin == "fixture" && strings.Contains(strings.Join(command.Args, " "), "--output /dev/null")
	}
	if err := quick.Check(property, &quick.Config{MaxCount: 128}); err != nil {
		t.Fatal(err)
	}
}
func TestGeneralCommandRejectsCredentialRedirect(t *testing.T) {
	const canary = "synthetic-exec-token"
	var leaked bool
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		leaked = r.Header.Get("Authorization") == "Bearer "+canary
		http.Error(w, "stop", 400)
	}))
	defer target.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, http.StatusFound) }))
	defer server.Close()
	config := &rest.Config{Host: server.URL, BearerToken: canary}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	c := TypedPlanClient{Client: client, ExecConfig: config}
	_, err = c.RunOperation(context.Background(), program.ExternalOperation{Exec: &program.Command{Pod: sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "api"}, Container: "app", Args: []string{"true"}}})
	if err == nil || leaked {
		t.Fatal("exec followed redirect with controller credentials")
	}
}
