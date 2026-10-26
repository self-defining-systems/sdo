package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/httpstream"
	"k8s.io/apimachinery/pkg/util/httpstream/spdy"
	"k8s.io/client-go/kubernetes"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	"net/http"
	"net/http/httptest"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"strings"
	"testing"
	"testing/quick"
	"time"
)

func TestDatabaseCatalogTransportProjectionAndCanaries(t *testing.T) {
	const canary = "fake-password-MUST-NOT-LEAK"
	t.Setenv("APP_PASSWORD", canary)
	t.Setenv("ADMIN_PASSWORD", canary)
	t.Setenv("ADMIN_USERNAME", canary)
	for _, mode := range []string{"read", "ensure", "malformed", "denied", "missing-env", "authentication", "catalog-read", "client-binary"} {
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
			if mode == "malformed" && !strings.Contains(databaseSafeError(err, "unavailable").Error(), "catalog-parse") {
				t.Fatal("parse failure step lost")
			}
			if strings.Contains(fmt.Sprint(err), canary) {
				t.Fatal("transport leaked excluded field or credential")
			}
		})
	}
}

func TestDatabaseReferencesNeverResolveHostCredentials(t *testing.T) {
	a := databaseAction()
	const canary = "fake-host-secret-canary"
	t.Setenv("APP_PASSWORD", canary)
	if err := quick.Check(func(suffix uint16) bool {
		copy := a
		copy.Password.Env = fmt.Sprintf("PASSWORD_%d", suffix)
		t.Setenv(copy.Password.Env, canary)
		raw, err := json.Marshal(copy)
		script := mongoScript(copy.Target, &copy)
		return err == nil && !strings.Contains(string(raw)+script, canary) && strings.Contains(script, copy.Password.Env)
	}, &quick.Config{MaxCount: 100}); err != nil {
		t.Fatal(err)
	}
}
func TestDatabaseMalformedContractDoesNotPanic(t *testing.T) {
	a := databaseActionState(&contracts.EnsureDatabaseUser{})
	if err := (program.Plan{Actions: []program.Action{a}}).Validate(); err == nil {
		t.Fatal("accepted absent target and password")
	}
}
func TestDatabaseCatalogBatchProjectsEarlierUsers(t *testing.T) {
	a := databaseAction()
	ref := a.Target.Pod
	e, c, oldRef := runtimeActionFixture()
	pod := c.objects[oldRef].DeepCopy()
	pod.SetName(ref.Name)
	pod.SetNamespace(ref.Namespace)
	_ = unstructured.SetNestedSlice(pod.Object, []any{map[string]any{"name": "mongo"}}, "spec", "containers")
	db := &databaseFake{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: pod}}}
	e.Client = db
	b := a
	b.User = "second"
	r, err := e.Apply(context.Background(), IncidentRequest{Namespace: "app", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{a, b}}, false)
	if err != nil || !r.VerificationPassed || len(db.users) != 2 {
		t.Fatalf("%+v %v", r, err)
	}
}

func TestNewActionsAppearInDescriptorMenuAndRoundTrip(t *testing.T) {
	entries := rawMenuActions()
	for _, name := range []string{"RemoveServicePort", "EnsureDatabaseUser"} {
		if _, ok := entries[name]; !ok {
			t.Fatalf("missing menu action %s", name)
		}
	}
	p, err := menuRawPlan(entries["RemoveServicePort"].action, map[string]string{"ref": "Service/app/discovery", "name": "unused", "reason": "synthetic"}, &program.ValueInputs{})
	if err != nil {
		t.Fatal(err)
	}
	if err = p.Validate(); err != nil {
		t.Fatal(err)
	}
	p, err = menuRawPlan(entries["EnsureDatabaseUser"].action, map[string]string{"target": `{"engine":"mongodb","pod":{"api_version":"v1","kind":"Pod","namespace":"app","name":"database"},"container":"mongo","database":"app"}`, "user": "app", "roles": `[{"database":"app","name":"readWrite"}]`, "password": `{"env":"APP_PASSWORD"}`}, &program.ValueInputs{})
	if err != nil {
		t.Fatal(err)
	}
	if err = p.Validate(); err != nil {
		t.Fatal(err)
	}
	encoded, err := (PlanApplicationResult{Plan: p}).Proto()
	if err != nil {
		t.Fatal(err)
	}
	decoded := planApplicationState(encoded).Result.Plan
	if err = decoded.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestDatabaseUnknownMenuReceiptEarnsNoRepairCredit(t *testing.T) {
	a := databaseAction()
	ref := a.Target.Pod
	object := planObject(ref, map[string]any{"containers": []any{map[string]any{"name": "mongo"}}})
	object.SetUID("one")
	object.SetResourceVersion("1")
	client := &databaseFake{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: object}}, ignore: true}
	loop := NewResponderLoop(IncidentRequest{IncidentID: "database-uncertain", Namespace: "app", RepairPolicy: "recorded-actions"}, nil, nil, nil, PlanExecutor{Client: client, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}, 5)
	loop.MenuConfig = productionResponderMenu()
	loop.Handle(context.Background(), ToolRequest{Command: "menu"})
	response := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: "EnsureDatabaseUser", RoundLimit: 1}, Parameters: map[string]string{"target": `{"engine":"mongodb","pod":{"api_version":"v1","kind":"Pod","namespace":"app","name":"database"},"container":"mongo","database":"app"}`, "user": "app", "roles": `[{"database":"app","name":"readWrite"}]`, "password": `{"env":"APP_PASSWORD"}`}})
	if response.Report == nil || len(loop.MenuRepairActions()) != 0 {
		t.Fatalf("unconfirmed catalog change earned repair credit: %+v", response)
	}
	for _, round := range response.Report.Rounds {
		for _, receipt := range round.Actions {
			if receipt.State == contracts.MitigationActionReceipt_STATE_APPLIED {
				t.Fatal("unknown effect reported applied")
			}
		}
	}
}

func TestDatabaseLiveReadRejectsReplacedSnapshotPod(t *testing.T) {
	a := databaseAction()
	pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: a.Target.Pod.Name, Namespace: "app", UID: "snapshot"}}
	replacement := pod.DeepCopy()
	replacement.UID = "replacement"
	client := kubefake.NewSimpleClientset(replacement)
	view := LiveReadView{DetectionContext: sdktest.Snapshot{NamespaceName: "app", PodList: []corev1.Pod{pod}}, Client: client}
	if _, err := view.DatabaseUsers(a.Target); err == nil || !strings.Contains(err.Error(), "identity differs") {
		t.Fatalf("stale Pod snapshot accepted: %v", err)
	}
	for _, action := range client.Actions() {
		if action.GetResource().Resource != "pods" || action.GetVerb() != "get" {
			t.Fatalf("unexpected sensitive operation: %v", action)
		}
	}
}
