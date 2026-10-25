package runtime

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	jsonpatch "github.com/evanphx/json-patch"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
)

// This HTTP edge shares the client-go object tracker with the native installer
// and controller. kubectl/HY restore therefore change the very same cluster.
func bringupRehearsalAPI(t *testing.T, client *fake.Clientset, install func() error) string {
	t.Helper()
	resources := map[string]map[string]string{
		"v1":                           {"namespaces": "Namespace", "pods": "Pod", "services": "Service", "endpoints": "Endpoints", "configmaps": "ConfigMap", "persistentvolumeclaims": "PersistentVolumeClaim", "serviceaccounts": "ServiceAccount", "events": "Event"},
		"apps/v1":                      {"deployments": "Deployment", "replicasets": "ReplicaSet", "statefulsets": "StatefulSet", "daemonsets": "DaemonSet"},
		"batch/v1":                     {"jobs": "Job", "cronjobs": "CronJob"},
		"networking.k8s.io/v1":         {"networkpolicies": "NetworkPolicy"},
		"rbac.authorization.k8s.io/v1": {"roles": "Role", "rolebindings": "RoleBinding"},
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		send := func(status int, value any) { w.WriteHeader(status); _ = json.NewEncoder(w).Encode(value) }
		fail := func(err error) {
			if s, ok := err.(apierrors.APIStatus); ok {
				send(int(s.Status().Code), s.Status())
			} else {
				send(500, map[string]any{"kind": "Status", "message": err.Error()})
			}
		}
		path := strings.Trim(r.URL.Path, "/")
		if path == "rehearsal/install" && r.Method == http.MethodPost {
			if err := install(); err != nil {
				fail(err)
				return
			}
			send(200, map[string]bool{"installed": true})
			return
		}
		if path == "api" {
			send(200, map[string]any{"kind": "APIVersions", "versions": []string{"v1"}})
			return
		}
		if path == "apis" {
			groups := []any{}
			for version := range resources {
				if version == "v1" {
					continue
				}
				parts := strings.Split(version, "/")
				v := map[string]string{"groupVersion": version, "version": parts[1]}
				groups = append(groups, map[string]any{"name": parts[0], "versions": []any{v}, "preferredVersion": v})
			}
			send(200, map[string]any{"kind": "APIGroupList", "groups": groups})
			return
		}
		parts := strings.Split(path, "/")
		version := ""
		var rest []string
		if len(parts) >= 2 && parts[0] == "api" {
			version = parts[1]
			rest = parts[2:]
		} else if len(parts) >= 3 && parts[0] == "apis" {
			version = parts[1] + "/" + parts[2]
			rest = parts[3:]
		} else {
			send(404, map[string]any{"kind": "Status", "reason": "NotFound", "code": 404})
			return
		}
		kinds, ok := resources[version]
		if !ok {
			send(404, map[string]any{"kind": "Status", "reason": "NotFound", "code": 404})
			return
		}
		if len(rest) == 0 {
			list := []any{}
			for name, kind := range kinds {
				list = append(list, map[string]any{"name": name, "singularName": "", "kind": kind, "namespaced": name != "namespaces", "verbs": []string{"get", "list", "create", "update", "patch", "delete"}})
			}
			send(200, map[string]any{"kind": "APIResourceList", "groupVersion": version, "resources": list})
			return
		}
		namespace := ""
		if len(rest) >= 3 && rest[0] == "namespaces" {
			namespace = rest[1]
			rest = rest[2:]
		}
		resource := rest[0]
		kind, ok := kinds[resource]
		if !ok {
			send(404, map[string]any{"kind": "Status", "reason": "NotFound", "code": 404})
			return
		}
		gv, _ := schema.ParseGroupVersion(version)
		gvr := gv.WithResource(resource)
		gvk := gv.WithKind(kind)
		name := ""
		if len(rest) > 1 {
			name = rest[1]
		}
		tracker := client.Tracker()
		switch r.Method {
		case http.MethodGet:
			var obj kruntime.Object
			var err error
			if name == "" {
				obj, err = tracker.List(gvr, gvk, namespace)
			} else {
				obj, err = tracker.Get(gvr, namespace, name)
			}
			if err != nil {
				fail(err)
				return
			}
			if name == "" {
				obj.GetObjectKind().SetGroupVersionKind(gv.WithKind(kind + "List"))
			} else {
				obj.GetObjectKind().SetGroupVersionKind(gvk)
			}
			send(200, obj)
		case http.MethodPatch:
			obj, err := tracker.Get(gvr, namespace, name)
			if err != nil {
				fail(err)
				return
			}
			before, _ := json.Marshal(obj)
			patch, _ := io.ReadAll(r.Body)
			var after []byte
			if strings.Contains(r.Header.Get("Content-Type"), "json-patch") {
				p, e := jsonpatch.DecodePatch(patch)
				if e != nil {
					fail(e)
					return
				}
				after, err = p.Apply(before)
			} else {
				after, err = jsonpatch.MergePatch(before, patch)
			}
			if err != nil {
				fail(err)
				return
			}
			if err = json.Unmarshal(after, obj); err != nil {
				fail(err)
				return
			}
			if err = tracker.Update(gvr, obj, namespace); err != nil {
				fail(err)
				return
			}
			obj.GetObjectKind().SetGroupVersionKind(gvk)
			send(200, obj)
		case http.MethodDelete:
			if name != "" {
				if err := tracker.Delete(gvr, namespace, name); err != nil {
					fail(err)
					return
				}
			} else {
				list, err := tracker.List(gvr, gvk, namespace)
				if err != nil {
					fail(err)
					return
				}
				items, _ := meta.ExtractList(list)
				for _, item := range items {
					a, _ := meta.Accessor(item)
					if err := tracker.Delete(gvr, namespace, a.GetName()); err != nil {
						fail(err)
						return
					}
				}
			}
			send(200, &metav1.Status{Status: "Success", Code: 200})
		case http.MethodPost, http.MethodPut:
			obj, err := scheme.Scheme.New(gvk)
			if err != nil {
				fail(err)
				return
			}
			if err = json.NewDecoder(r.Body).Decode(obj); err != nil {
				fail(err)
				return
			}
			if r.Method == http.MethodPost {
				err = tracker.Create(gvr, obj, namespace)
			} else {
				err = tracker.Update(gvr, obj, namespace)
			}
			if err != nil {
				fail(err)
				return
			}
			obj.GetObjectKind().SetGroupVersionKind(gvk)
			send(200, obj)
		default:
			fail(fmt.Errorf("unsupported rehearsal request %s %s", r.Method, r.URL.Path))
		}
	}))
	t.Cleanup(server.Close)
	config := map[string]any{"apiVersion": "v1", "kind": "Config", "clusters": []any{map[string]any{"name": "test", "cluster": map[string]any{"server": server.URL}}}, "contexts": []any{map[string]any{"name": "test", "context": map[string]any{"cluster": "test", "user": "test", "namespace": "demo"}}}, "users": []any{map[string]any{"name": "test", "user": map[string]any{}}}, "current-context": "test"}
	data, _ := json.Marshal(config)
	filename := filepath.Join(t.TempDir(), "kubeconfig.json")
	if err := os.WriteFile(filename, data, 0600); err != nil {
		t.Fatal(err)
	}
	return filename
}
