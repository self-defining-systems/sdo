package install

import (
	"context"
	"slices"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// Every ServiceAccount SDO runs as resolves, through a cluster-wide binding, to
// the wildcard rule, in both namespace layouts; ensureClusterGrants (the step of
// a fresh install and of install-existing) restores that grant after it was
// narrowed by hand.
func TestEverySDOIdentityHoldsTheWildcardGrant(t *testing.T) {
	for name, cfg := range map[string]Config{"split namespaces": testConfig(), "single namespace": func() Config { c := testConfig(); c.ControlNamespace = c.Namespace; return c }()} {
		plan, err := Resources(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if len(plan.ClusterRoles) != 1 || len(plan.ClusterRoles[0].Rules) != 1 {
			t.Fatalf("%s: want one ClusterRole with one rule, got %v", name, plan.ClusterRoles)
		}
		rule := plan.ClusterRoles[0].Rules[0]
		if !slices.Equal(rule.APIGroups, []string{"*"}) || !slices.Equal(rule.Resources, []string{"*"}) || !slices.Equal(rule.Verbs, []string{"*"}) {
			t.Fatalf("%s: rule = %v, want * on * with *", name, rule)
		}
		if len(plan.ClusterBindings) != 1 || plan.ClusterBindings[0].RoleRef.Name != plan.ClusterRoles[0].Name {
			t.Fatalf("%s: bindings do not resolve to the ClusterRole: %v", name, plan.ClusterBindings)
		}
		bound := map[string]bool{}
		for _, subject := range plan.ClusterBindings[0].Subjects {
			bound[subject.Namespace+"/"+subject.Name] = true
		}
		for _, account := range plan.ServiceAccounts {
			if !bound[account.Namespace+"/"+account.Name] {
				t.Errorf("%s: ServiceAccount %s/%s is not bound to the wildcard grant", name, account.Namespace, account.Name)
			}
		}
		if len(plan.ServiceAccounts) == 0 {
			t.Fatalf("%s: plan has no ServiceAccounts", name)
		}
		client := fake.NewSimpleClientset()
		installer := Installer{Client: client}
		narrowed := plan.ClusterRoles[0].DeepCopy()
		narrowed.Rules[0].Resources = []string{"configmaps"}
		if _, err := client.RbacV1().ClusterRoles().Create(context.Background(), narrowed, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
		if err := installer.ensureClusterGrants(context.Background(), plan); err != nil {
			t.Fatal(err)
		}
		got, err := client.RbacV1().ClusterRoles().Get(context.Background(), plan.ClusterRoles[0].Name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got.Rules[0].Resources, []string{"*"}) {
			t.Errorf("%s: re-applied rule = %v, want the wildcard", name, got.Rules[0])
		}
	}
}
