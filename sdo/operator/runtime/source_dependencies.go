package runtime

import (
	"sort"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/sdk"
)

// Only accepted PodSpecs and RBAC documents grant dependency identity. Content
// is deliberately unavailable: naming a dependency is not evidence of its data.
func (s *SourceInventory) indexDependencies() {
	s.dependencies = map[sdk.ObjectRef]bool{}
	add := func(api, kind, namespace, name string) {
		if name == "" {
			return
		}
		ref := sdk.ObjectRef{APIVersion: api, Kind: kind, Namespace: namespace, Name: name}
		if s.declared[ref] == nil {
			s.dependencies[ref] = true
		}
	}
	for _, o := range s.declared {
		namespace := o.GetNamespace()
		var path []string
		switch o.GetAPIVersion() + "/" + o.GetKind() {
		case "v1/Pod":
			path = []string{"spec"}
		case "apps/v1/Deployment", "apps/v1/StatefulSet", "apps/v1/DaemonSet", "apps/v1/ReplicaSet", "batch/v1/Job":
			path = []string{"spec", "template", "spec"}
		case "batch/v1/CronJob":
			path = []string{"spec", "jobTemplate", "spec", "template", "spec"}
		}
		if len(path) > 0 {
			spec, _, _ := unstructured.NestedMap(o.Object, path...)
			name, _ := spec["serviceAccountName"].(string)
			add("v1", "ServiceAccount", namespace, name)
			for _, raw := range inventoryList(spec["imagePullSecrets"]) {
				value, _ := raw.(map[string]any)
				name, _ := value["name"].(string)
				add("v1", "Secret", namespace, name)
			}
			for _, raw := range inventoryList(spec["volumes"]) {
				volume, _ := raw.(map[string]any)
				for key, kind := range map[string]string{"configMap": "ConfigMap", "secret": "Secret", "persistentVolumeClaim": "PersistentVolumeClaim"} {
					value, _ := volume[key].(map[string]any)
					field := "name"
					if key == "secret" {
						field = "secretName"
					}
					if key == "persistentVolumeClaim" {
						field = "claimName"
					}
					name, _ := value[field].(string)
					add("v1", kind, namespace, name)
				}
				projected, _ := volume["projected"].(map[string]any)
				for _, raw := range inventoryList(projected["sources"]) {
					value, _ := raw.(map[string]any)
					for key, kind := range map[string]string{"configMap": "ConfigMap", "secret": "Secret"} {
						ref, _ := value[key].(map[string]any)
						name, _ := ref["name"].(string)
						add("v1", kind, namespace, name)
					}
				}
			}
			for _, field := range []string{"containers", "initContainers", "ephemeralContainers"} {
				for _, raw := range inventoryList(spec[field]) {
					container, _ := raw.(map[string]any)
					for _, raw := range inventoryList(container["envFrom"]) {
						value, _ := raw.(map[string]any)
						for key, kind := range map[string]string{"configMapRef": "ConfigMap", "secretRef": "Secret"} {
							ref, _ := value[key].(map[string]any)
							name, _ := ref["name"].(string)
							add("v1", kind, namespace, name)
						}
					}
					for _, raw := range inventoryList(container["env"]) {
						value, _ := raw.(map[string]any)
						from, _ := value["valueFrom"].(map[string]any)
						for key, kind := range map[string]string{"configMapKeyRef": "ConfigMap", "secretKeyRef": "Secret"} {
							ref, _ := from[key].(map[string]any)
							name, _ := ref["name"].(string)
							add("v1", kind, namespace, name)
						}
					}
				}
			}
		}
		if o.GetAPIVersion() != "rbac.authorization.k8s.io/v1" || (o.GetKind() != "RoleBinding" && o.GetKind() != "ClusterRoleBinding") {
			continue
		}
		subjects, _, _ := unstructured.NestedSlice(o.Object, "subjects")
		bound := false
		for _, raw := range subjects {
			subject, _ := raw.(map[string]any)
			if subject["kind"] != "ServiceAccount" {
				continue
			}
			ns, _ := subject["namespace"].(string)
			if ns == "" && o.GetKind() == "RoleBinding" {
				ns = namespace
			}
			if ns != s.Namespace {
				continue
			}
			name, _ := subject["name"].(string)
			add("v1", "ServiceAccount", ns, name)
			bound = true
		}
		if bound {
			role, _, _ := unstructured.NestedMap(o.Object, "roleRef")
			kind, _ := role["kind"].(string)
			name, _ := role["name"].(string)
			ns := namespace
			if kind == "ClusterRole" {
				ns = ""
			}
			if kind == "Role" || kind == "ClusterRole" {
				add("rbac.authorization.k8s.io/v1", kind, ns, name)
			}
		}
	}
	refs := make([]sdk.ObjectRef, 0, len(s.dependencies))
	for ref := range s.dependencies {
		refs = append(refs, ref)
	}
	sort.Slice(refs, func(i, j int) bool {
		return refs[i].APIVersion+"/"+refs[i].Kind+"/"+refs[i].Namespace+"/"+refs[i].Name < refs[j].APIVersion+"/"+refs[j].Kind+"/"+refs[j].Namespace+"/"+refs[j].Name
	})
	for _, ref := range refs {
		s.EvidenceGaps = append(s.EvidenceGaps, rendered.InventoryEvidenceGap{Kind: "referenced_dependency_content", Ref: ref, Reason: "accepted source names this dependency but does not declare its contents"})
	}
}
func inventoryList(value any) []any { list, _ := value.([]any); return list }
