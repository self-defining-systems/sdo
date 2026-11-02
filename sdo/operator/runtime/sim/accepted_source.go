package sim

import (
	"encoding/json"
	"net/http"
	"sort"
	"strings"

	jsonpatch "github.com/evanphx/json-patch"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
)

// groupVersions maps each served plural to the API group version that serves it.
var groupVersions = map[string]string{
	"configmaps": "v1", "services": "v1", "pods": "v1", "persistentvolumeclaims": "v1", "serviceaccounts": "v1", "nodes": "v1", "secrets": "v1", "endpoints": "v1", "events": "v1",
	"deployments": "apps/v1", "replicasets": "apps/v1", "statefulsets": "apps/v1", "daemonsets": "apps/v1",
	"jobs": "batch/v1", "cronjobs": "batch/v1", "ingresses": "networking.k8s.io/v1", "networkpolicies": "networking.k8s.io/v1",
	"endpointslices": "discovery.k8s.io/v1", "horizontalpodautoscalers": "autoscaling/v2", "poddisruptionbudgets": "policy/v1",
}

// serveAcceptedSource answers the requests behind a source comparison: API
// discovery, a server-side apply dry run over a live object and a dry-run
// create of accepted source alone (the server's default render). Neither
// dry run persists anything. Other requests are not handled here.
func (tr Transport) serveAcceptedSource(req *http.Request, path []string, reply func(int, []byte) *http.Response) (*http.Response, bool) {
	groupVersion, rest := "", []string(nil)
	switch {
	case len(path) >= 2 && path[0] == "api":
		groupVersion, rest = path[1], path[2:]
	case len(path) >= 3 && path[0] == "apis":
		groupVersion, rest = path[1]+"/"+path[2], path[3:]
	default:
		return nil, false
	}
	if len(rest) == 0 && req.Method == http.MethodGet {
		var resources []metav1.APIResource
		for plural, version := range groupVersions {
			if version == groupVersion {
				resources = append(resources, metav1.APIResource{Name: plural, Kind: resources2kind(plural), Namespaced: plural != "nodes"})
			}
		}
		sort.Slice(resources, func(i, j int) bool { return resources[i].Name < resources[j].Name })
		return reply(http.StatusOK, mustJSON(&metav1.APIResourceList{GroupVersion: groupVersion, APIResources: resources})), true
	}
	if req.URL.Query().Get("dryRun") != metav1.DryRunAll {
		return nil, false
	}
	namespace := ""
	if len(rest) >= 2 && rest[0] == "namespaces" {
		namespace, rest = rest[1], rest[2:]
	}
	notFound := func(name string) (*http.Response, bool) {
		return reply(http.StatusNotFound, mustJSON(metav1.Status{TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"}, Status: metav1.StatusFailure, Reason: metav1.StatusReasonNotFound, Code: http.StatusNotFound, Message: name + " not found"})), true
	}
	kube := tr.World.Kube
	switch {
	case req.Method == http.MethodPatch && len(rest) == 2:
		kind := resources2kind(rest[0])
		ref := sdk.ObjectRef{APIVersion: groupVersion, Kind: kind, Namespace: namespace, Name: rest[1]}
		live := kube.objects[ref]
		if live == nil {
			return notFound(ref.Name)
		}
		var patch []byte
		if req.Body != nil {
			patch = readAll(req.Body)
		}
		before, _ := json.Marshal(live.Object)
		merged, err := jsonpatch.MergePatch(before, patch)
		if err != nil {
			return reply(http.StatusBadRequest, []byte(err.Error())), true
		}
		object := new(unstructured.Unstructured)
		if err := json.Unmarshal(merged, &object.Object); err != nil {
			return reply(http.StatusBadRequest, []byte(err.Error())), true
		}
		defaultObject(object)
		return reply(http.StatusOK, mustJSON(object.Object)), true
	case req.Method == http.MethodPost && len(rest) == 1:
		var object unstructured.Unstructured
		if err := json.Unmarshal(readAll(req.Body), &object.Object); err != nil {
			return reply(http.StatusBadRequest, []byte(err.Error())), true
		}
		object.SetAPIVersion(groupVersion)
		object.SetKind(resources2kind(rest[0]))
		object.SetNamespace(namespace)
		ref := sdk.ObjectRef{APIVersion: groupVersion, Kind: object.GetKind(), Namespace: namespace, Name: object.GetName()}
		if kube.objects[ref] != nil {
			return reply(http.StatusConflict, mustJSON(metav1.Status{TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"}, Status: metav1.StatusFailure, Reason: metav1.StatusReasonAlreadyExists, Code: http.StatusConflict})), true
		}
		defaultObject(&object)
		return reply(http.StatusCreated, mustJSON(object.Object)), true
	}
	return nil, false
}

func resources2kind(plural string) string { return resources[plural] }

func readAll(body interface{ Read([]byte) (int, error) }) []byte {
	var out strings.Builder
	buffer := make([]byte, 4096)
	for {
		n, err := body.Read(buffer)
		out.Write(buffer[:n])
		if err != nil {
			return []byte(out.String())
		}
	}
}

// defaultObject fills the fields the API server defaults on write. Only the
// kinds the simulation models are defaulted; a field a caller declared is kept.
func defaultObject(object *unstructured.Unstructured) {
	if object.GetKind() != "Service" {
		return
	}
	spec, _ := object.Object["spec"].(map[string]any)
	if spec == nil {
		spec = map[string]any{}
		object.Object["spec"] = spec
	}
	for key, value := range map[string]any{"type": "ClusterIP", "sessionAffinity": "None", "internalTrafficPolicy": "Cluster", "ipFamilyPolicy": "SingleStack", "ipFamilies": []any{"IPv4"}} {
		if _, ok := spec[key]; !ok {
			spec[key] = value
		}
	}
	ports, _ := spec["ports"].([]any)
	for _, item := range ports {
		if port, ok := item.(map[string]any); ok {
			if _, ok := port["protocol"]; !ok {
				port["protocol"] = "TCP"
			}
		}
	}
}
