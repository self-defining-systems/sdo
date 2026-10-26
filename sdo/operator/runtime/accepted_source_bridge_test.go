package runtime

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	ktesting "k8s.io/client-go/testing"
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
		gvr, namespace, name, ok := parseTestObjectPath(request.URL.Path)
		if !ok {
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		var action ktesting.Action
		switch request.Method {
		case http.MethodGet:
			action = ktesting.NewGetAction(gvr, namespace, name)
		case http.MethodPatch:
			body, _ := io.ReadAll(request.Body)
			action = ktesting.NewPatchAction(gvr, namespace, name, types.ApplyPatchType, body)
		default:
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		object, err := client.Invokes(action, nil)
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
