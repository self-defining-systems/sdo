package install

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
