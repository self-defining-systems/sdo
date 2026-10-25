package install

import (
	"context"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"testing"
)

func TestUnexplainedRemovalGrantsDedicatedIdentityOnly(t *testing.T) {
	resources := []*metav1.APIResourceList{{GroupVersion: "v1", APIResources: []metav1.APIResource{{Name: "serviceaccounts", Kind: "ServiceAccount", Namespaced: true, Verbs: []string{"get", "delete"}}, {Name: "secrets", Kind: "Secret", Namespaced: true, Verbs: []string{"get", "delete"}}, {Name: "pods/exec", Kind: "Pod", Namespaced: true, Verbs: []string{"get", "delete"}}}}, {GroupVersion: "rbac.authorization.k8s.io/v1", APIResources: []metav1.APIResource{{Name: "clusterroles", Kind: "ClusterRole", Verbs: []string{"get", "delete"}}, {Name: "clusterrolebindings", Kind: "ClusterRoleBinding", Verbs: []string{"get", "delete"}}, {Name: "roles", Kind: "Role", Namespaced: true, Verbs: []string{"get", "delete"}}}}}
	p := Plan{}
	if err := addUnexplainedRemovalGrants(Config{Namespace: "app", ControlNamespace: "control"}, &p, resources); err != nil {
		t.Fatal(err)
	}

	if len(p.AppRoles) != 1 || p.AppRoles[0].Namespace != "app" || len(p.ClusterRoles) != 2 {
		t.Fatalf("missing isolated grants: %+v", p)
	}
	var removalRules []rbacv1.PolicyRule
	for _, role := range p.ClusterRoles {
		if role.Name == "sdo-unexplained-removal-control" {
			removalRules = append(removalRules, role.Rules...)
		}
		if role.Name == "sdo-unexplained-removal-control-impersonation" {
			if len(role.Rules) != 1 || len(role.Rules[0].Resources) != 1 || role.Rules[0].Resources[0] != "users" || len(role.Rules[0].ResourceNames) != 1 || role.Rules[0].ResourceNames[0] != "sdo-unexplained-removal:control" {
				t.Fatalf("excess impersonation authority: %+v", role)
			}
		}
	}
	if len(removalRules) != 2 {
		t.Fatal("missing cluster removal rules")
	}
	for _, grant := range append(p.AppRoles[0].Rules, removalRules...) {
		if len(grant.Verbs) != 2 || grant.Verbs[0] != "get" || grant.Verbs[1] != "delete" || grant.Resources[0] == "secrets" || grant.Resources[0] == "pods/exec" || grant.Resources[0] == "*" {
			t.Fatalf("excess authority: %+v", grant)
		}
	}
	for _, binding := range p.AppBindings {
		for _, subject := range binding.Subjects {
			if subject.Kind != "User" || subject.Name != "sdo-unexplained-removal:control" {
				t.Fatal("workload gained removal authority")
			}
		}
	}
	for _, binding := range p.ClusterBindings {
		if binding.RoleRef.Name != "sdo-unexplained-removal-control" {
			continue
		}
		for _, subject := range binding.Subjects {
			if subject.Kind != "User" || subject.Name != "sdo-unexplained-removal:control" {
				t.Fatal("workload gained removal authority")
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
	if len(p.ServiceAccounts) != 0 {
		t.Fatal("removal identity must not be available to workloads")
	}
	for _, b := range p.ClusterBindings {
		if b.RoleRef.Name != "sdo-unexplained-removal-control" {
			continue
		}
		for _, s := range b.Subjects {
			if s.Name == "sdo-controller" {
				t.Fatal("generic controller gained removal grants")
			}
		}
	}
}

func TestRemovalIdentityCannotBeAssumedByWorkload(t *testing.T) {
	p := Plan{}
	resources := []*metav1.APIResourceList{{GroupVersion: "rbac.authorization.k8s.io/v1", APIResources: []metav1.APIResource{{Name: "clusterroles", Kind: "ClusterRole", Verbs: []string{"get", "delete"}}}}}
	if err := addUnexplainedRemovalGrants(Config{Namespace: "app", ControlNamespace: "control"}, &p, resources); err != nil {
		t.Fatal(err)
	}
	if len(p.ServiceAccounts) != 0 {
		t.Fatal("removal identity can be assumed by a Pod or token request")
	}
	found := false
	for _, binding := range p.ClusterBindings {
		for _, subject := range binding.Subjects {
			if subject.Name == "sdo-unexplained-removal:control" {
				if subject.Kind != "User" {
					t.Fatal("removal grant is not user-only")
				}
				found = true
			}
		}
	}
	if !found {
		t.Fatal("missing removal-only user")
	}
}
func TestColocatedRemovalAuthorityIsDisabled(t *testing.T) {
	p := Plan{}
	resources := []*metav1.APIResourceList{{GroupVersion: "rbac.authorization.k8s.io/v1", APIResources: []metav1.APIResource{{Name: "clusterroles", Kind: "ClusterRole", Verbs: []string{"get", "delete"}}}}}
	if err := addUnexplainedRemovalGrants(Config{Namespace: "app"}, &p, resources); err != nil {
		t.Fatal(err)
	}
	if len(p.ClusterRoles) != 0 || len(p.AppRoles) != 0 || len(p.ControlRoles) != 0 {
		t.Fatal("colocated workloads can acquire the controller identity and removal authority")
	}
}

func TestColocatedReinstallRevokesLegacyRemovalAuthority(t *testing.T) {
	ctx := context.Background()
	name := "sdo-unexplained-removal-app"
	client := fake.NewSimpleClientset(
		&rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app"}},
		&rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app"}},
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: name}},
		&rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: name}},
	)
	if err := (Installer{Client: client}).reconcileUnexplainedRemovalGrants(ctx, Config{Namespace: "app"}, Plan{}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.RbacV1().ClusterRoles().Get(ctx, name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatal("legacy cluster deletion retained")
	}
	if _, err := client.RbacV1().ClusterRoleBindings().Get(ctx, name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatal("legacy cluster removal binding retained")
	}
	if _, err := client.RbacV1().Roles("app").Get(ctx, name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatal("legacy namespaced deletion retained")
	}
	if _, err := client.RbacV1().RoleBindings("app").Get(ctx, name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatal("legacy namespaced removal binding retained")
	}
}
