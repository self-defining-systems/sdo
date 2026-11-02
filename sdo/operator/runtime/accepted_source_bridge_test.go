package runtime

import (
	"encoding/json"
	"net/http"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/kubernetes/fake"
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

