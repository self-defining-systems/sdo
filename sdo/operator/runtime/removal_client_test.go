package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	"net/http"
	"net/http/httptest"
	"sdo.dev/operator/core"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"testing"
)

func TestGenericRawDeleteCannotReachRemovalIdentity(t *testing.T) {
	deleted := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != "DELETE" || r.URL.Path != "/apis/rbac.authorization.k8s.io/v1/clusterroles/unrelated" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Impersonate-User") != "system:serviceaccount:control:sdo-unexplained-removal" {
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprint(w, `{"kind":"Status","status":"Failure","reason":"Forbidden","code":403}`)
			return
		}
		var options metav1.DeleteOptions
		if json.NewDecoder(r.Body).Decode(&options) != nil || options.Preconditions == nil || options.Preconditions.UID == nil || *options.Preconditions.UID != "uid" {
			t.Error("conditional identity missing")
		}
		deleted = true
		fmt.Fprint(w, `{"kind":"Status","status":"Success"}`)
	}))
	defer server.Close()
	config := &rest.Config{Host: server.URL}
	fakeClient := fake.NewSimpleClientset()
	fakeClient.Resources = []*metav1.APIResourceList{{GroupVersion: "rbac.authorization.k8s.io/v1", APIResources: []metav1.APIResource{{Name: "clusterroles", Kind: "ClusterRole", Verbs: []string{"get", "delete"}}}}}
	removal, err := newUnexplainedRemovalClient(config, &core.GenericCapture{Namespace: "app", Discovery: fakeClient.Discovery()}, "control")
	if err != nil {
		t.Fatal(err)
	}
	generic := TypedPlanClient{ExecConfig: config}
	request := program.KubernetesRequest{Method: "DELETE", Path: "/apis/rbac.authorization.k8s.io/v1/clusterroles/unrelated"}
	if _, err = generic.KubernetesRequest(context.Background(), request); !apierrors.IsForbidden(err) || deleted {
		t.Fatalf("generic raw DELETE gained authority: %v", err)
	}
	// The dedicated client is a different authenticated transport. Production only
	// invokes it after removeUnexplained has checked the current selection.
	ref := sdk.ObjectRef{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole", Name: "unrelated"}
	if err = removal.Delete(context.Background(), ref, "uid", "1"); err != nil || !deleted {
		t.Fatalf("dedicated delete failed: %v", err)
	}
	deleted = false
	if _, err = generic.KubernetesRequest(context.Background(), request); !apierrors.IsForbidden(err) || deleted {
		t.Fatalf("identity leaked back to generic client: %v", err)
	}
	if config.Impersonate.UserName != "" {
		t.Fatal("generic config mutated")
	}
}
