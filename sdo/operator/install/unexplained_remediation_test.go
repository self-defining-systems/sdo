package install

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"testing"
)

func TestUnexplainedRemovalGrantsDedicatedIdentityOnly(t *testing.T) {
	resources := []*metav1.APIResourceList{{GroupVersion: "v1", APIResources: []metav1.APIResource{{Name: "serviceaccounts", Kind: "ServiceAccount", Namespaced: true, Verbs: []string{"get", "delete"}}, {Name: "secrets", Kind: "Secret", Namespaced: true, Verbs: []string{"get", "delete"}}, {Name: "pods/exec", Kind: "Pod", Namespaced: true, Verbs: []string{"get", "delete"}}}}, {GroupVersion: "rbac.authorization.k8s.io/v1", APIResources: []metav1.APIResource{{Name: "clusterroles", Kind: "ClusterRole", Verbs: []string{"get", "delete"}}, {Name: "clusterrolebindings", Kind: "ClusterRoleBinding", Verbs: []string{"get", "delete"}}, {Name: "roles", Kind: "Role", Namespaced: true, Verbs: []string{"get", "delete"}}}}}
	p := Plan{}
	if err := addUnexplainedRemovalGrants(Config{Namespace: "app", ControlNamespace: "control"}, &p, resources); err != nil {
		t.Fatal(err)
	}
	if len(p.AppRoles) != 1 || p.AppRoles[0].Namespace != "app" || len(p.ClusterRoles) != 1 || len(p.ClusterRoles[0].Rules) != 2 {
		t.Fatalf("missing grants: %+v", p)
	}
	for _, grant := range append(p.AppRoles[0].Rules, p.ClusterRoles[0].Rules...) {
		if len(grant.Verbs) != 2 || grant.Verbs[0] != "get" || grant.Verbs[1] != "delete" || grant.Resources[0] == "secrets" || grant.Resources[0] == "pods/exec" || grant.Resources[0] == "*" {
			t.Fatalf("excess authority: %+v", grant)
		}
	}
	for _, binding := range p.AppBindings {
		for _, subject := range binding.Subjects {
			if subject.Name != "sdo-unexplained-removal" {
				t.Fatal("worker gained authority")
			}
		}
	}
	for _, binding := range p.ClusterBindings {
		for _, subject := range binding.Subjects {
			if subject.Name != "sdo-unexplained-removal" {
				t.Fatal("worker gained authority")
			}
		}
	}
}

func TestRemovalIdentityCannotBeUsedByGenericAPI(t *testing.T) {
	p := Plan{}
	resources := []*metav1.APIResourceList{{GroupVersion: "rbac.authorization.k8s.io/v1", APIResources: []metav1.APIResource{{Name: "clusterroles", Kind: "ClusterRole", Verbs: []string{"get", "delete"}}}}}
	if err := addUnexplainedRemovalGrants(Config{Namespace: "app", ControlNamespace: "control"}, &p, resources); err != nil {
		t.Fatal(err)
	}
	if len(p.ServiceAccounts) != 1 || p.ServiceAccounts[0].Name != "sdo-unexplained-removal" {
		t.Fatal("missing dedicated removal identity")
	}
	for _, b := range p.ClusterBindings {
		for _, s := range b.Subjects {
			if s.Name == "sdo-controller" {
				t.Fatal("generic controller gained removal grants")
			}
		}
	}
}
