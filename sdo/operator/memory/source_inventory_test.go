package memory

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestSourceFactsOfProjectsRenderedObjects(t *testing.T) {
	deployment := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "frontend", "namespace": "shop"},
		"spec": map[string]any{
			"selector": map[string]any{"matchLabels": map[string]any{"app": "frontend"}},
			"template": map[string]any{
				"metadata": map[string]any{"labels": map[string]any{"app": "frontend"}},
				"spec": map[string]any{
					"serviceAccountName": "web",
					"containers":         []any{map[string]any{"name": "web", "image": "registry.invalid/web:v1"}},
				},
			},
		},
	}}
	service := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Service", "metadata": map[string]any{"name": "frontend"}}}
	nameless := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "ConfigMap"}}

	facts := SourceFactsOf([]*unstructured.Unstructured{service, nameless, deployment}, "helm")

	if len(facts.Resources) != 2 {
		t.Fatalf("resources = %v, want the two named objects", facts.Resources)
	}
	inventory := ArchitectureInventory(facts)
	for _, want := range []string{"- Deployment/shop/frontend (source: helm)", "- Service/default/frontend (source: helm)", "ServiceAccount/web", "image:registry.invalid/web:v1", "selector:app=frontend"} {
		if !strings.Contains(inventory, want) {
			t.Errorf("inventory lacks %q:\n%s", want, inventory)
		}
	}
}
