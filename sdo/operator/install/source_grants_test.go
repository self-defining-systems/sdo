package install

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes/fake"
)

func TestAcceptedSourceGrantsAreNamedControllerOnlyAndRevoked(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	client.Resources = []*metav1.APIResourceList{{GroupVersion: "rbac.authorization.k8s.io/v1", APIResources: []metav1.APIResource{{Name: "clusterroles", Kind: "ClusterRole", Namespaced: false}, {Name: "clusterrolebindings", Kind: "ClusterRoleBinding", Namespaced: false}}}}
	role := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole", "metadata": map[string]any{"name": "app-cleanup"}, "rules": []any{}}}
	if err := ReconcileAcceptedSourceGrants(ctx, client, "app", "control", []*unstructured.Unstructured{role}); err != nil {
		t.Fatal(err)
	}
	roles, err := client.RbacV1().ClusterRoles().List(ctx, metav1.ListOptions{})
	if err != nil || len(roles.Items) != 1 {
		t.Fatalf("roles: %+v %v", roles, err)
	}
	rules := roles.Items[0].Rules
	if len(rules) != 1 || len(rules[0].ResourceNames) != 1 || rules[0].ResourceNames[0] != "app-cleanup" {
		t.Fatalf("grant is not source-named: %+v", rules)
	}
	for _, verb := range []string{"get", "patch", "update", "escalate", "bind"} {
		found := false
		for _, actual := range rules[0].Verbs {
			found = found || actual == verb
		}
		if !found {
			t.Fatalf("missing %s", verb)
		}
	}
	bindings, _ := client.RbacV1().ClusterRoleBindings().List(ctx, metav1.ListOptions{})
	if len(bindings.Items) != 1 {
		t.Fatalf("bindings: %+v", bindings)
	}
	for _, subject := range bindings.Items[0].Subjects {
		if subject.Namespace != "control" || (subject.Name != "sdo-controller" && subject.Name != "sdo-operate") {
			t.Fatalf("worker or foreign authority: %+v", subject)
		}
	}
	role.SetName("replacement")
	if err := ReconcileAcceptedSourceGrants(ctx, client, "app", "control", []*unstructured.Unstructured{role}); err != nil {
		t.Fatal(err)
	}
	roles, _ = client.RbacV1().ClusterRoles().List(ctx, metav1.ListOptions{})
	if roles.Items[0].Rules[0].ResourceNames[0] != "replacement" {
		t.Fatal("stale object authority survived source change")
	}
	if err := ReconcileAcceptedSourceGrants(ctx, client, "app", "control", nil); err != nil {
		t.Fatal(err)
	}
	roles, _ = client.RbacV1().ClusterRoles().List(ctx, metav1.ListOptions{})
	bindings, _ = client.RbacV1().ClusterRoleBindings().List(ctx, metav1.ListOptions{})
	if len(roles.Items) != 0 || len(bindings.Items) != 0 {
		t.Fatal("removed source retained cluster authority")
	}
}
