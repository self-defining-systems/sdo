package operatorcli

import (
	"context"
	"encoding/json"
	"io"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sdo.dev/operator/sdk/program"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/apihistory"
	"sdo.dev/operator/sdk"
)

func TestScriptProxyHonorsFenceAndForcesFieldManager(t *testing.T) {
	for _, state := range []string{"open", "verified", "closed"} {
		t.Run(state, func(t *testing.T) {
			gate, err := apihistory.PrepareMutationGate(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if state != "open" {
				guard, err := gate.TryObserve()
				if err != nil || guard == nil {
					t.Fatalf("observe gate: %v", err)
				}
				if state == "verified" {
					err = guard.Fence(time.Unix(1, 0))
				} else {
					err = guard.Seal()
				}
				guard.Close()
				if err != nil {
					t.Fatal(err)
				}
			}
			upstream, _ := url.Parse("https://synthetic.invalid")
			calls := 0
			transport := secretCaptureTransport(func(r *http.Request) (*http.Response, error) {
				calls++
				if r.URL.Query().Get("fieldManager") != sdk.ResponderFieldManager {
					t.Error("script selected its own field manager")
				}
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("{}"))}, nil
			})
			proxy := scriptProxy(upstream, transport, transport, nil, gate)
			response := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPatch, "/api/v1/namespaces/app/configmaps/target?fieldManager=other", strings.NewReader("{}"))
			proxy.ServeHTTP(response, req)
			if state == "open" && (calls != 1 || response.Code != http.StatusOK) ||
				state != "open" && (calls != 0 || response.Code == http.StatusOK) {
				t.Fatalf("%s forwarded %d writes with status %d", state, calls, response.Code)
			}
		})
	}
}

func TestScriptTimeoutDrainsPatchBeforeFence(t *testing.T) {
	gate, err := apihistory.PrepareMutationGate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	upstream, _ := url.Parse("https://synthetic.invalid")
	runCtx, stop := context.WithCancel(t.Context())
	defer stop()
	admitted, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	transport := secretCaptureTransport(func(r *http.Request) (*http.Response, error) {
		close(admitted)
		<-release
		if err := r.Context().Err(); err != nil {
			return nil, err
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("{}"))}, nil
	})
	proxy := drainScriptRequests(runCtx, t.Context(), scriptProxy(upstream, transport, transport, nil, gate))
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/namespaces/app/configmaps/target", strings.NewReader("{}"))
	response := httptest.NewRecorder()
	go func() {
		proxy.ServeHTTP(response, req.WithContext(runCtx))
		close(done)
	}()
	<-admitted
	stop()
	if guard, err := gate.TryObserve(); guard != nil || err != nil {
		t.Fatalf("write lease missing: %v %v", guard, err)
	}
	close(release)
	<-done
	guard, err := gate.TryObserve()
	if err != nil || guard == nil {
		t.Fatalf("observe drained write: %v", err)
	}
	defer guard.Close()
	if err := guard.Fence(time.Unix(1, 0)); err != nil || response.Code != http.StatusOK {
		t.Fatalf("drained PATCH status %d, fence: %v", response.Code, err)
	}
}

func TestScriptRefusalNamesEveryFailedIdentityClause(t *testing.T) {
	index := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "script", Namespace: "control"}, Data: map[string]string{"application_id": "app-1", "namespace": "app", "script_digest": "digest"}}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "script", UID: "uid", Annotations: map[string]string{"sdo.dev/script-application": "app-1"}}}
	same := program.ScriptExecutionResult{ApplicationID: "app-1", JobName: "script", JobUID: "uid", ScriptDigest: "digest"}
	if err := scriptIdentityConflict(index, job, "app", same); err != nil {
		t.Fatal(err)
	}
	for clause, change := range map[string]func(*corev1.ConfigMap, *batchv1.Job, *program.ScriptExecutionResult){
		"script digest": func(_ *corev1.ConfigMap, _ *batchv1.Job, out *program.ScriptExecutionResult) {
			out.ScriptDigest = "other"
		},
		"namespace": func(index *corev1.ConfigMap, _ *batchv1.Job, _ *program.ScriptExecutionResult) {
			index.Data["namespace"] = "other"
		},
		"application": func(_ *corev1.ConfigMap, job *batchv1.Job, _ *program.ScriptExecutionResult) { job.Annotations = nil },
		"Job UID": func(index *corev1.ConfigMap, _ *batchv1.Job, _ *program.ScriptExecutionResult) {
			index.Data["job_uid"] = "other"
		},
		"Job UID unavailable": func(_ *corev1.ConfigMap, _ *batchv1.Job, out *program.ScriptExecutionResult) { out.JobUID = "" },
	} {
		index, job, out := index.DeepCopy(), job.DeepCopy(), same
		change(index, job, &out)
		err := scriptIdentityConflict(index, job, "app", out)
		if err == nil || !strings.Contains(err.Error(), clause) {
			t.Fatalf("%s: %v", clause, err)
		}
		kube := fake.NewSimpleClientset(index)
		refused := refuseScript(t.Context(), kube, "control", "script", "uid", err)
		if !strings.Contains(refused.Error(), "not run") || !strings.Contains(refused.Error(), clause) {
			t.Fatalf("runner logs %v", refused)
		}
		stored, getErr := kube.CoreV1().ConfigMaps("control").Get(t.Context(), "script", metav1.GetOptions{})
		var refusal program.ScriptRefusal
		if getErr != nil || json.Unmarshal([]byte(stored.Data[program.ScriptRefusalKey]), &refusal) != nil || refusal.JobUID != "uid" || !strings.Contains(refusal.Reason, clause) {
			t.Fatalf("published %+v %v", stored, getErr)
		}
	}
}

// A read whose client went away (a script's finished `kubectl rollout status`
// watch) ends at once. The runner's shutdown, and the controller step waiting
// on the script's result, never wait on it until the drain deadline. A read
// whose client stays keeps streaming until the drain ends.
func TestScriptReadEndsWithItsClientOrTheDrain(t *testing.T) {
	for _, c := range []struct {
		name         string
		clientLeaves bool
	}{{"client leaves", true}, {"client stays", false}} {
		t.Run(c.name, func(t *testing.T) {
			gate, err := apihistory.PrepareMutationGate(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			upstream, _ := url.Parse("https://synthetic.invalid")
			watching, done := make(chan struct{}), make(chan struct{})
			drain, stopDrain := context.WithTimeout(t.Context(), time.Minute)
			defer stopDrain()
			var drainEnded error
			transport := secretCaptureTransport(func(r *http.Request) (*http.Response, error) {
				close(watching)
				<-r.Context().Done()
				drainEnded = drain.Err()
				return nil, r.Context().Err()
			})
			proxy := drainScriptRequests(t.Context(), drain, scriptProxy(upstream, transport, transport, nil, gate))
			client, leave := context.WithCancel(t.Context())
			defer leave()
			req := httptest.NewRequest(http.MethodGet, "/apis/apps/v1/namespaces/app/deployments?watch=true", nil).WithContext(client)
			go func() {
				proxy.ServeHTTP(httptest.NewRecorder(), req)
				close(done)
			}()
			<-watching
			if c.clientLeaves {
				leave()
			} else {
				stopDrain()
			}
			<-done
			if c.clientLeaves && drainEnded != nil {
				t.Fatal("a watch whose client left held the proxy until the drain deadline")
			}
			if !c.clientLeaves && drainEnded == nil {
				t.Fatal("a watch whose client stayed ended before the drain did")
			}
		})
	}
}
