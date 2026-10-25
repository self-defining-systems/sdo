package runtime

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/discovery/fake"
	"k8s.io/client-go/kubernetes"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func TestMenuHidesUnavailableAPIAndShowsActualBudget(t *testing.T) {
	loop := NewResponderLoop(IncidentRequest{IncidentID: "capabilities"}, nil, nil, nil, PlanExecutor{Client: GenericPlanClient{TypedPlanClient: TypedPlanClient{Client: kubefake.NewSimpleClientset()}}}, 10)
	loop.MenuConfig = productionResponderMenu()
	response := loop.Handle(context.Background(), ToolRequest{Command: "menu"})
	for _, option := range response.Menu.GetOptions() {
		if option.OptionId == "KubernetesRequest" {
			t.Fatal("advertised unavailable API transport")
		}
	}
	if response.LoopsAllowed != 5 {
		t.Fatalf("advertised %d rounds but execution caps at five", response.LoopsAllowed)
	}
}

func TestGenericObjectFallbackUsesConfiguredAPI(t *testing.T) {
	for _, kind := range []string{"ServiceAccount", "ClusterRole", "ClusterRoleBinding", "Widget"} {
		t.Run(kind, func(t *testing.T) {
			ref := sdk.ObjectRef{APIVersion: "example.test/v1", Kind: kind, Name: "cleanup"}
			if kind == "ServiceAccount" {
				ref.APIVersion = "v1"
				ref.Namespace = "app"
			}
			if kind == "ClusterRole" || kind == "ClusterRoleBinding" {
				ref.APIVersion = "rbac.authorization.k8s.io/v1"
			}
			object := urObject(ref)
			deleted := false
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.Method {
				case "GET":
					_ = json.NewEncoder(w).Encode(object.Object)
				case "DELETE":
					var options metav1.DeleteOptions
					if err := json.NewDecoder(r.Body).Decode(&options); err != nil || options.Preconditions == nil || options.Preconditions.UID == nil || *options.Preconditions.UID != object.GetUID() || options.Preconditions.ResourceVersion == nil || *options.Preconditions.ResourceVersion != object.GetResourceVersion() {
						t.Error("delete lost exact identity/version preconditions")
					}
					deleted = true
					_, _ = w.Write([]byte(`{"kind":"Status","status":"Success"}`))
				default:
					t.Errorf("unexpected method %s", r.Method)
				}
			}))
			defer server.Close()
			kube := kubefake.NewSimpleClientset()
			kube.Discovery().(*fake.FakeDiscovery).Resources = []*metav1.APIResourceList{{GroupVersion: ref.APIVersion, APIResources: []metav1.APIResource{{Name: "objects", Kind: kind, Namespaced: ref.Namespace != "", Verbs: []string{"get", "delete"}}}}}
			client := GenericPlanClient{TypedPlanClient: TypedPlanClient{Client: kube, ExecConfig: &rest.Config{Host: server.URL}}}
			if _, err := client.Get(context.Background(), ref); err != nil {
				t.Fatalf("generic get unavailable: %v", err)
			}
			if err := client.Delete(context.Background(), ref, object.GetUID(), object.GetResourceVersion()); err != nil || !deleted {
				t.Fatalf("generic delete unavailable: %v", err)
			}
		})
	}
}

func TestSuppliedKubernetesClientRetainsAPITransport(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/namespaces/app/configmaps/item" || r.Header.Get("Authorization") != "Bearer synthetic-canary" {
			t.Error("supplied client lost path or authentication")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"kind":"ConfigMap"}`))
	}))
	defer server.Close()
	kube, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL, BearerToken: "synthetic-canary"})
	if err != nil {
		t.Fatal(err)
	}
	client := TypedPlanClient{Client: kube}
	if !client.hasAPITransport() {
		t.Fatal("real supplied client advertised unavailable API")
	}
	if _, err := client.KubernetesRequest(context.Background(), program.KubernetesRequest{Method: "GET", Path: "/api/v1/namespaces/app/configmaps/item"}); err != nil {
		t.Fatal(err)
	}
}

func TestUnavailableAPIDoesNotClaimDispatchedWrite(t *testing.T) {
	executor := PlanExecutor{Client: TypedPlanClient{Client: kubefake.NewSimpleClientset()}, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
	result, err := executor.Apply(context.Background(), IncidentRequest{}, program.Plan{Actions: []program.Action{program.KubernetesRequest{Method: "DELETE", Path: "/api/v1/namespaces/app/serviceaccounts/cleanup", Reason: "remove injected object"}}}, false)
	if err == nil || result.Applied || len(result.ActionsApplied) > 0 {
		t.Fatalf("missing transport claimed a write: %+v %v", result, err)
	}
}
