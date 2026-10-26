package install

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"testing"
)

func TestDiscoveryCaptureGrantsCoverNewKindsWithoutSecretReads(t *testing.T) {
	resources := []*metav1.APIResourceList{{GroupVersion: "new.example/v1", APIResources: []metav1.APIResource{{Kind: "NewKind", Name: "newkinds", Namespaced: true, Verbs: []string{"get", "list", "patch"}}, {Kind: "ClusterKind", Name: "clusterkinds", Verbs: []string{"get", "list", "patch"}}}}, {GroupVersion: "v1", APIResources: []metav1.APIResource{{Kind: "Secret", Name: "secrets", Namespaced: true, Verbs: []string{"get", "list"}}, {Kind: "ServiceAccount", Name: "serviceaccounts", Namespaced: true, Verbs: []string{"get", "list"}}, {Kind: "TokenRequest", Name: "serviceaccounts/token", Namespaced: true, Verbs: []string{"create"}}}}}
	plan, err := Resources(testConfig(), resources...)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, role := range plan.ClusterRoles {
		if role.Name == captureGrantName(testConfig().controlNamespace()) {
			for _, rule := range role.Rules {
				for _, resource := range rule.Resources {
					seen[resource] = true
					for _, verb := range rule.Verbs {
						if verb != "get" && verb != "list" {
							t.Fatalf("capture granted mutation: %+v", rule)
						}
					}
				}
			}
		}
	}
	for _, name := range []string{"newkinds", "clusterkinds", "serviceaccounts"} {
		if !seen[name] {
			t.Fatalf("generic capture lacks %s", name)
		}
	}
	for _, name := range []string{"secrets", "serviceaccounts/token", "*"} {
		if seen[name] {
			t.Fatalf("capture granted %s", name)
		}
	}
}

func TestInventoryWatchesNeedOnlyNonSecretReadGrants(t *testing.T) {
	resources := []*metav1.APIResourceList{{GroupVersion: "rbac.authorization.k8s.io/v1", APIResources: []metav1.APIResource{{Kind: "ClusterRole", Name: "clusterroles", Verbs: []string{"get", "list", "watch", "patch"}}, {Kind: "ClusterRoleBinding", Name: "clusterrolebindings", Verbs: []string{"get", "list", "watch", "patch"}}}}}
	plan, err := Resources(testConfig(), resources...)
	if err != nil {
		t.Fatal(err)
	}
	watched := map[string]bool{}
	for _, role := range plan.ClusterRoles {
		if role.Name != captureGrantName(testConfig().controlNamespace()) {
			continue
		}
		for _, rule := range role.Rules {
			for _, verb := range rule.Verbs {
				if verb == "watch" {
					watched[rule.Resources[0]] = true
				}
				if verb != "get" && verb != "list" && verb != "watch" {
					t.Fatalf("inventory acquired mutation: %+v", rule)
				}
			}
		}
	}
	if !watched["clusterroles"] || !watched["clusterrolebindings"] {
		t.Fatalf("missing RBAC informer authority: %v", watched)
	}
	if inventoryWatchResource("", "secrets") {
		t.Fatal("Secret inventory was enabled")
	}
}

func TestDiscoveredMetricsUsageGetsReadOnlyGrants(t *testing.T) {
	plan, err := Resources(testConfig(), &metav1.APIResourceList{GroupVersion: "metrics.k8s.io/v1beta1", APIResources: []metav1.APIResource{
		{Name: "pods", Kind: "PodMetrics", Namespaced: true, Verbs: []string{"get", "list"}},
		{Name: "nodes", Kind: "NodeMetrics", Verbs: []string{"get", "list"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, role := range plan.ClusterRoles {
		if role.Name != captureGrantName(testConfig().controlNamespace()) {
			continue
		}
		for _, rule := range role.Rules {
			if len(rule.APIGroups) != 1 || rule.APIGroups[0] != "metrics.k8s.io" {
				continue
			}
			for _, resource := range rule.Resources {
				seen[resource] = true
			}
			for _, verb := range rule.Verbs {
				if verb != "get" && verb != "list" {
					t.Fatalf("metrics obtained extra authority: %+v", rule)
				}
			}
		}
	}
	if !seen["pods"] || !seen["nodes"] {
		t.Fatalf("metrics read grants missing: %v", seen)
	}
}
