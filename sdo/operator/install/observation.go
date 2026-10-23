package install

import (
	"fmt"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Read grants belong to the controller only. They grant no foreign Secret read
// and no repair access; ambient permissive grants cannot widen runtime policy.
func addObservationGrants(plan *Plan, c Config) {
	for i, t := range c.ObservationCeiling.Targets {
		name := fmt.Sprintf("sdo-observe-%s-%d", c.Namespace, i)
		resource := map[string]string{"Deployment": "deployments", "Service": "services", "Endpoints": "endpoints", "MutatingWebhookConfiguration": "mutatingwebhookconfigurations", "ValidatingWebhookConfiguration": "validatingwebhookconfigurations"}[t.Kind]
		group := ""
		if t.Kind == "Deployment" {
			group = "apps"
		}
		if t.Cluster() {
			group = "admissionregistration.k8s.io"
		}
		r := rule(group, []string{resource}, "get", "list")
		if t.Cluster() {
			r.Verbs = []string{"get"}
			r.ResourceNames = []string{t.Name}
			plan.ObservationClusterRoles = append(plan.ObservationClusterRoles, &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: name}, Rules: []rbacv1.PolicyRule{r}})
			plan.ObservationClusterBindings = append(plan.ObservationClusterBindings, &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: name}, Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Name: "sdo-controller", Namespace: c.controlNamespace()}, {Kind: "ServiceAccount", Name: "sdo-operate", Namespace: c.controlNamespace()}}, RoleRef: rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: name}})
		} else {
			plan.ObservationRoles = append(plan.ObservationRoles, role(t.Namespace, name, []rbacv1.PolicyRule{r}))
			b := binding(t.Namespace, name, "sdo-controller", c.controlNamespace())
			b.Subjects = append(b.Subjects, rbacv1.Subject{Kind: "ServiceAccount", Name: "sdo-operate", Namespace: c.controlNamespace()})
			plan.ObservationBindings = append(plan.ObservationBindings, b)
		}
	}
}
