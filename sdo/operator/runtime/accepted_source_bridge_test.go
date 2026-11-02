//go:build integration

package runtime

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ktesting "k8s.io/client-go/testing"
	"sdo.dev/operator/sdk"
)

// testAPIResources is the discovery document test API servers publish.
var testAPIResources = map[string][]metav1.APIResource{
	"v1": {
		{Name: "services", Kind: "Service", Namespaced: true}, {Name: "configmaps", Kind: "ConfigMap", Namespaced: true},
		{Name: "pods", Kind: "Pod", Namespaced: true}, {Name: "serviceaccounts", Kind: "ServiceAccount", Namespaced: true},
		{Name: "persistentvolumeclaims", Kind: "PersistentVolumeClaim", Namespaced: true}, {Name: "persistentvolumeclaims/status", Kind: "PersistentVolumeClaim", Namespaced: true},
		{Name: "secrets", Kind: "Secret", Namespaced: true}, {Name: "persistentvolumes", Kind: "PersistentVolume"},
	},
	"apps/v1": {
		{Name: "deployments", Kind: "Deployment", Namespaced: true}, {Name: "statefulsets", Kind: "StatefulSet", Namespaced: true},
		{Name: "daemonsets", Kind: "DaemonSet", Namespaced: true}, {Name: "replicasets", Kind: "ReplicaSet", Namespaced: true},
	},
	"policy/v1":                    {{Name: "poddisruptionbudgets", Kind: "PodDisruptionBudget", Namespaced: true}},
	"rbac.authorization.k8s.io/v1": {{Name: "clusterroles", Kind: "ClusterRole"}, {Name: "clusterrolebindings", Kind: "ClusterRoleBinding"}, {Name: "roles", Kind: "Role", Namespaced: true}, {Name: "rolebindings", Kind: "RoleBinding", Namespaced: true}},
	"storage.k8s.io/v1":            {{Name: "storageclasses", Kind: "StorageClass"}},
}

// serveTestDiscovery answers group-version discovery for test API servers.
func serveTestDiscovery(writer http.ResponseWriter, request *http.Request) bool {
	if request.Method != http.MethodGet {
		return false
	}
	groupVersion := strings.TrimPrefix(strings.TrimPrefix(request.URL.Path, "/apis/"), "/api/")
	resources, ok := testAPIResources[groupVersion]
	if !ok || strings.Count(groupVersion, "/") > 1 {
		return false
	}
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(&metav1.APIResourceList{GroupVersion: groupVersion, APIResources: resources})
	return true
}

// acceptedSourceBridge gives a fake clientset the generic accepted-source REST
// path: discovery is served over HTTP and object GET/apply requests are
// forwarded into the fake's reactors and tracker.
type acceptedSourceBridge struct {
	*fake.Clientset
	discovery discovery.DiscoveryInterface
}

func (b acceptedSourceBridge) Discovery() discovery.DiscoveryInterface { return b.discovery }

func bridgeFake(t *testing.T, client *fake.Clientset) acceptedSourceBridge {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if serveTestDiscovery(writer, request) {
			return
		}
		path := request.URL.Path
		var created *unstructured.Unstructured
		if request.Method == http.MethodPost {
			created = &unstructured.Unstructured{}
			if err := json.NewDecoder(request.Body).Decode(&created.Object); err != nil {
				http.Error(writer, err.Error(), http.StatusBadRequest)
				return
			}
			path += "/" + created.GetName()
		}
		gvr, namespace, name, ok := parseTestObjectPath(path)
		if !ok {
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		var action ktesting.Action
		switch request.Method {
		case http.MethodGet:
			action = ktesting.NewGetAction(gvr, namespace, name)
		case http.MethodPost:
			// The typed fake tracker cannot list unstructured built-in objects.
			typed, err := scheme.Scheme.New(created.GroupVersionKind())
			if err == nil {
				err = kruntime.DefaultUnstructuredConverter.FromUnstructured(created.Object, typed)
			}
			if err != nil {
				http.Error(writer, err.Error(), http.StatusBadRequest)
				return
			}
			action = ktesting.NewCreateAction(gvr, namespace, typed)
		case http.MethodPatch:
			body, _ := io.ReadAll(request.Body)
			action = ktesting.NewPatchAction(gvr, namespace, name, types.ApplyPatchType, body)
		default:
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		invoker := &client.Fake
		if (request.Method == http.MethodPost || request.Method == http.MethodPatch) && request.URL.Query().Get("dryRun") == metav1.DryRunAll {
			// Keep custom server-default reactors, but isolate every dry-run
			// write. POST retains AlreadyExists; PATCH projects source values
			// without repairing the real fixture or changing its watch stream.
			tracker := ktesting.NewObjectTracker(scheme.Scheme, scheme.Codecs.UniversalDecoder())
			if existing, err := client.Tracker().Get(gvr, namespace, name); err == nil {
				if err := tracker.Add(existing); err != nil {
					http.Error(writer, err.Error(), http.StatusInternalServerError)
					return
				}
			} else if !apierrors.IsNotFound(err) {
				http.Error(writer, err.Error(), http.StatusInternalServerError)
				return
			}
			reactions := append([]ktesting.Reactor(nil), client.ReactionChain...)
			reactions[len(reactions)-1] = &ktesting.SimpleReactor{Verb: "*", Resource: "*", Reaction: ktesting.ObjectReaction(tracker)}
			invoker = &ktesting.Fake{ReactionChain: reactions}
		}
		object, err := invoker.Invokes(action, nil)
		writer.Header().Set("Content-Type", "application/json")
		if err != nil {
			status := apierrors.NewInternalError(err).ErrStatus
			if api, ok := err.(apierrors.APIStatus); ok {
				status = api.Status()
			}
			status.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}
			writer.WriteHeader(int(status.Code))
			_ = json.NewEncoder(writer).Encode(&status)
			return
		}
		_ = json.NewEncoder(writer).Encode(object)
	}))
	t.Cleanup(server.Close)
	return acceptedSourceBridge{Clientset: client, discovery: discovery.NewDiscoveryClientForConfigOrDie(&rest.Config{Host: server.URL})}
}

func parseTestObjectPath(path string) (schema.GroupVersionResource, string, string, bool) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	var gv schema.GroupVersion
	switch {
	case len(parts) > 2 && parts[0] == "api":
		gv, parts = schema.GroupVersion{Version: parts[1]}, parts[2:]
	case len(parts) > 3 && parts[0] == "apis":
		gv, parts = schema.GroupVersion{Group: parts[1], Version: parts[2]}, parts[3:]
	default:
		return schema.GroupVersionResource{}, "", "", false
	}
	namespace := ""
	if len(parts) == 4 && parts[0] == "namespaces" {
		namespace, parts = parts[1], parts[2:]
	}
	if len(parts) != 2 {
		return schema.GroupVersionResource{}, "", "", false
	}
	return gv.WithResource(parts[0]), namespace, parts[1], true
}

// Exercise the real accepted-source default-render producer and the typed list
// consumer for both core and grouped resources. Dry-run probes are not live
// application objects, even when their names differ from the accepted object.
func TestAcceptedSourceBridgeDryRunCreateDoesNotPolluteTypedLists(t *testing.T) {
	for _, ref := range []sdk.ObjectRef{
		{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"},
		{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "config"},
	} {
		t.Run(ref.Kind, func(t *testing.T) {
			ctx := context.Background()
			client := fake.NewSimpleClientset()
			bridge := bridgeFake(t, client)
			resource, err := resolveAcceptedResource(bridge, ref)
			if err != nil {
				t.Fatal(err)
			}
			source := &unstructured.Unstructured{Object: map[string]any{"apiVersion": ref.APIVersion, "kind": ref.Kind, "metadata": map[string]any{"name": ref.Name, "namespace": ref.Namespace}}}
			if _, err := resource.renderDefaults(ctx, ref, source); err != nil {
				t.Fatal(err)
			}
			assertCount := func(want int) {
				t.Helper()
				deployments, err := client.AppsV1().Deployments("app").List(ctx, metav1.ListOptions{})
				if err != nil {
					t.Fatal(err)
				}
				configs, err := client.CoreV1().ConfigMaps("app").List(ctx, metav1.ListOptions{})
				if err != nil {
					t.Fatal(err)
				}
				if got := len(deployments.Items) + len(configs.Items); got != want {
					t.Fatalf("typed live lists have %d objects, want %d", got, want)
				}
			}
			assertCount(0)
			body, err := json.Marshal(source.Object)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := bodyOf(ctx, resource.client.Post().AbsPath(strings.TrimSuffix(resource.path, "/"+ref.Name)).Body(body)); err != nil {
				t.Fatal(err)
			}
			assertCount(1)
			// A colliding live probe must be rejected, not reused as defaults.
			probe := source.DeepCopy()
			probe.SetName(ref.Name + defaultRenderSuffix)
			body, err = json.Marshal(probe.Object)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := bodyOf(ctx, resource.client.Post().AbsPath(strings.TrimSuffix(resource.path, "/"+ref.Name)).Body(body)); err != nil {
				t.Fatal(err)
			}
			if _, err := resource.renderDefaults(ctx, ref, source); !apierrors.IsAlreadyExists(err) {
				t.Fatalf("colliding default probe: want AlreadyExists, got %v", err)
			}
			assertCount(2)
		})
	}
}
