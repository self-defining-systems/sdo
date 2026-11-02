package runtime

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"testing"
	"time"
)

func TestSourceInventoryAcceptedDependenciesAreDeclaredWithContentGap(t *testing.T) {
	deployment := inventoryObject("apps/v1", "Deployment", "app", "geo")
	podSpec := map[string]any{
		"serviceAccountName": "geo-sa", "imagePullSecrets": []any{map[string]any{"name": "pull"}},
		"volumes": []any{
			map[string]any{"name": "script", "configMap": map[string]any{"name": "mongo-geo-script"}},
			map[string]any{"name": "key", "secret": map[string]any{"secretName": "key"}},
			map[string]any{"name": "data", "persistentVolumeClaim": map[string]any{"claimName": "data"}},
			map[string]any{"name": "projected", "projected": map[string]any{"sources": []any{map[string]any{"configMap": map[string]any{"name": "projected"}}, map[string]any{"secret": map[string]any{"name": "projected-key"}}}}},
		},
		"initContainers": []any{map[string]any{"name": "init", "envFrom": []any{map[string]any{"configMapRef": map[string]any{"name": "init"}}}}},
		"containers":     []any{map[string]any{"name": "geo", "envFrom": []any{map[string]any{"secretRef": map[string]any{"name": "env-secret"}}}, "env": []any{map[string]any{"name": "X", "valueFrom": map[string]any{"configMapKeyRef": map[string]any{"name": "env"}}}, map[string]any{"name": "Y", "valueFrom": map[string]any{"secretKeyRef": map[string]any{"name": "env-key"}}}}}},
	}
	_ = unstructured.SetNestedMap(deployment.Object, podSpec, "spec", "template", "spec")
	binding := inventoryObject("rbac.authorization.k8s.io/v1", "RoleBinding", "app", "geo")
	binding.Object["subjects"] = []any{map[string]any{"kind": "ServiceAccount", "name": "bound"}}
	binding.Object["roleRef"] = map[string]any{"kind": "ClusterRole", "name": "reader"}
	index := NewSourceInventory("app", "accepted", []*unstructured.Unstructured{deployment, binding})
	live := []*unstructured.Unstructured{deployment.DeepCopy(), binding.DeepCopy()}
	for kind, names := range map[string][]string{"ConfigMap": {"mongo-geo-script", "projected", "init", "env"}, "PersistentVolumeClaim": {"data"}, "ServiceAccount": {"geo-sa", "bound"}} {
		for _, name := range names {
			live = append(live, inventoryObject("v1", kind, "app", name))
		}
	}
	live = append(live, inventoryObject("rbac.authorization.k8s.io/v1", "ClusterRole", "", "reader"))
	if got := index.Findings(live, time.Unix(1, 0)); len(got) != 0 {
		t.Fatalf("accepted dependencies became unexplained: %+v", got)
	}
	if len(index.EvidenceGaps) != 13 {
		t.Fatalf("unverified dependency contents must remain typed gaps: %+v", index.EvidenceGaps)
	}
	// A reference injected only into live state cannot declare its target.
	live[0].Object["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)["serviceAccountName"] = "injected"
	live = append(live, inventoryObject("v1", "ServiceAccount", "app", "injected"))
	if got := index.Findings(live, time.Unix(2, 0)); len(got) != 1 || got[0].PrimaryResource.Name != "injected" {
		t.Fatalf("live references granted source authority: %+v", got)
	}
}
