package runtime

import (
	"context"
	"fmt"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	"net/http"
	"net/http/httptest"
	"sdo.dev/operator/core"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"sync/atomic"
	"testing"
)

func TestGenericClientCannotReachControlIdentity(t *testing.T) {
	for _, withCapture := range []bool{false, true} {
		t.Run(fmt.Sprintf("capture=%v", withCapture), func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				w.Write([]byte(`{}`))
			}))
			defer server.Close()
			kube := fake.NewSimpleClientset()
			typed := TypedPlanClient{Client: kube, ExecConfig: &rest.Config{Host: server.URL}, ProtectedNamespace: "control"}
			dynamic := dynamicfake.NewSimpleDynamicClient(kruntime.NewScheme())
			client := GenericPlanClient{TypedPlanClient: typed}
			if withCapture {
				client.Objects = &core.GenericCapture{Namespace: "app", Client: dynamic}
			}
			ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "control", Name: "take-controller-identity"}
			pod := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod", "metadata": map[string]any{"name": ref.Name, "namespace": ref.Namespace}, "spec": map[string]any{"serviceAccountName": "sdo-controller"}}}
			for _, mode := range []string{"create", "patch", "delete", "exec", "operation", "raw-create", "raw-exec", "raw-token", "read"} {
				t.Run(mode, func(t *testing.T) {
					var err error
					switch mode {
					case "create":
						_, err = client.Create(context.Background(), ref, pod)
					case "patch":
						err = client.Patch(context.Background(), ref, types.MergePatchType, []byte(`{}`))
					case "delete":
						err = client.Delete(context.Background(), ref, "uid", "1")
					case "exec":
						_, err = client.Exec(context.Background(), ref, "app", []string{"true"})
					case "operation":
						_, err = client.RunOperation(context.Background(), program.ExternalOperation{Exec: &program.Command{Pod: ref, Container: "app", Args: []string{"true"}}})
					case "read":
						_, err = client.Get(context.Background(), ref)
					default:
						path := "/api/v1/namespaces/control/pods"
						if mode == "raw-exec" {
							path += "/controller/exec"
						}
						if mode == "raw-token" {
							path = "/api/v1/namespaces/control/serviceaccounts/sdo-controller/token"
						}
						_, err = client.KubernetesRequest(context.Background(), program.KubernetesRequest{Method: "POST", Path: path})
					}
					if !apierrors.IsForbidden(err) {
						t.Fatalf("control identity boundary mode=%s: %v", mode, err)
					}
				})
			}
			if len(kube.Actions()) != 0 || len(dynamic.Actions()) != 0 || calls.Load() != 0 {
				t.Fatal("control access crossed authenticated client boundary")
			}
			// The boundary preserves ordinary application actions on the same client.
			ref.Namespace = "app"
			pod.SetNamespace("app")
			if _, err := typed.Create(context.Background(), ref, pod); err != nil {
				t.Fatalf("application mutation refused: %v", err)
			}
		})
	}

}

func TestReadViewCannotExecuteWithControlIdentity(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "control", Name: "controller"}
	pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: ref.Name, Namespace: ref.Namespace, UID: "uid"}}
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{}`))
	}))
	defer server.Close()
	client := fake.NewSimpleClientset(&pod)
	view := LiveReadView{DetectionContext: sdktest.Snapshot{NamespaceName: "app", PodList: []corev1.Pod{pod}}, Client: client, DatabaseExecConfig: &rest.Config{Host: server.URL}, ProtectedNamespace: "control"}
	if _, err := view.Object(ref); !apierrors.IsForbidden(err) {
		t.Fatalf("control Pod admitted by diagnosis view: %v", err)
	}
	if _, err := view.Observe(program.ExternalOperation{Exec: &program.Command{Pod: ref, Container: "app", Args: []string{"true"}}}); err == nil {
		t.Fatal("diagnosis can execute with controller identity")
	}
	if _, err := view.KubernetesRead(program.KubernetesRequest{Method: "GET", Path: "/api/v1/namespaces/control/pods/controller/exec"}); err == nil {
		t.Fatalf("raw diagnosis can execute with controller identity: %v", err)
	}
	if len(client.Actions()) != 0 || requests.Load() != 0 {
		t.Fatal("diagnosis crossed protected control boundary")
	}
}
