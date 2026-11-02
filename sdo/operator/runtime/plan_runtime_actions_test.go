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
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/httpstream"
	"k8s.io/apimachinery/pkg/util/httpstream/spdy"
	"k8s.io/client-go/kubernetes"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type runtimeActionClient struct {
	*planFakeClient
	deleted   bool
	unchanged bool
	denied    bool
	command   []string
}

func (c *runtimeActionClient) Get(ctx context.Context, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if c.deleted && !c.unchanged {
		return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "pods"}, ref.Name)
	}
	return c.planFakeClient.Get(ctx, ref)
}
func (c *runtimeActionClient) Delete(_ context.Context, ref sdk.ObjectRef, uid types.UID, rv string) error {
	if uid != "pod-uid" || rv != "7" {
		panic("missing identity preconditions")
	}
	c.deleted = true
	return nil
}
func (c *runtimeActionClient) Exec(_ context.Context, ref sdk.ObjectRef, container string, command []string) (int, error) {
	if c.denied {
		return 0, apierrors.NewForbidden(schema.GroupResource{Resource: "pods/exec"}, ref.Name, nil)
	}
	c.command = append([]string(nil), command...)
	return 3, nil
}
func runtimeActionFixture() (PlanExecutor, *runtimeActionClient, sdk.ObjectRef) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "one"}
	pod := planObject(ref, map[string]any{})
	pod.SetUID("pod-uid")
	pod.SetResourceVersion("7")
	c := &runtimeActionClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: pod}}}
	return PlanExecutor{Client: c, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}}, c, ref
}
func TestXV2DeletePodVerifiesIdentity(t *testing.T) {
	for _, unchanged := range []bool{false, true} {
		e, c, ref := runtimeActionFixture()
		c.unchanged = unchanged
		r, err := e.Apply(context.Background(), IncidentRequest{Namespace: "app", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.DeletePod{Ref: ref}}}, false)
		if unchanged {
			if err == nil || r.VerificationPassed {
				t.Fatal("same UID proved deletion")
			}
		} else if err != nil || !r.VerificationPassed || !c.deleted {
			t.Fatalf("delete: %+v %v", r, err)
		}
	}
}
func TestXV2ExecRBACAndExitOnlyReceipt(t *testing.T) {
	for _, denied := range []bool{false, true} {
		e, c, ref := runtimeActionFixture()
		c.denied = denied
		e.Values = program.ValueInputs{Spec: program.ProgramSpec{Params: []program.ParamSpec{{Name: "command", ValueKind: program.StringValue, Default: "/bin/true"}}}}
		r, err := e.Apply(context.Background(), IncidentRequest{Namespace: "app", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.Exec{Pod: ref, Container: "app", Command: []program.Value{program.FromParam("command")}}}}, false)
		if denied {
			if err == nil || r.Applied || r.ActionReports[0].Status != "refused" {
				t.Fatalf("RBAC denial: %+v %v", r, err)
			}
		} else {
			if err != nil || r.ActionReports[0].ExitCode == nil || *r.ActionReports[0].ExitCode != 3 {
				t.Fatalf("exec: %+v %v", r, err)
			}
			raw, _ := json.Marshal(r)
			var fields map[string]any
			if json.Unmarshal(raw, &fields) != nil {
				t.Fatal("receipt JSON invalid")
			}
			if len(c.command) != 1 {
				t.Fatal("command not executed")
			}
		}
	}
}
func TestXV2ScaleWorkloadResolvesParameter(t *testing.T) {
	for _, kind := range []string{"Deployment", "StatefulSet"} {
		ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: kind, Namespace: "app", Name: "app"}
		c := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: planObject(ref, map[string]any{"replicas": int64(1)})}}
		e := PlanExecutor{Client: c, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}, Values: program.ValueInputs{Spec: program.ProgramSpec{Params: []program.ParamSpec{{Name: "replicas", ValueKind: program.IntValue, Default: 2}}}}}
		r, err := e.Apply(context.Background(), IncidentRequest{Namespace: "app", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.ScaleWorkload{Ref: ref, Replicas: program.FromParam("replicas")}}}, false)
		if err != nil || !r.VerificationPassed {
			t.Fatalf("%s scale: %+v %v", kind, r, err)
		}
		v, _, _ := unstructured.NestedFloat64(c.objects[ref].Object, "spec", "replicas")
		if v != 2 {
			t.Fatalf("replicas=%v", v)
		}
	}
}

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

func TestXV2ExecWithoutConfigReportsRefused(t *testing.T) {
	e, _, ref := runtimeActionFixture()
	client := kubernetesfake.NewSimpleClientset(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: ref.Name, Namespace: ref.Namespace, UID: "pod-uid", ResourceVersion: "7"}})
	e.Client = TypedPlanClient{Client: client}
	e.Values = program.ValueInputs{Spec: program.ProgramSpec{Params: []program.ParamSpec{{Name: "command", ValueKind: program.StringValue, Default: "/bin/true"}}}}
	result, err := e.Apply(context.Background(), IncidentRequest{Namespace: "app", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.Exec{Pod: ref, Container: "app", Command: []program.Value{program.FromParam("command")}}}}, false)
	if err == nil || result.Applied || result.VerificationPassed || result.ActionReports[0].Status != "refused" {
		t.Fatalf("nil config must refuse exec before any effect: %+v %v", result, err)
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
