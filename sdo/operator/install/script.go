package install

import (
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"strings"
)

// Script repairs have the controller's application authority plus namespace-local
// mutation/exec/Secret reads. Script tools remain app-agnostic; RBAC is the edge.
func addScriptGrants(c Config, plan *Plan) {
	plan.ServiceAccounts = append(plan.ServiceAccounts, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "sdo-script", Namespace: c.Namespace}})
	rules := []rbacv1.PolicyRule{rule("*", []string{"*"}, "get", "list", "watch", "create", "update", "patch", "delete")}
	plan.AppRoles = append(plan.AppRoles, role(c.Namespace, "sdo-script", rules))
	plan.AppBindings = append(plan.AppBindings, binding(c.Namespace, "sdo-script", "sdo-script", c.Namespace))
	plan.AppRoles = append(plan.AppRoles, role(c.Namespace, "sdo-script-controller", []rbacv1.PolicyRule{rule("", []string{"configmaps"}, "get", "create", "update", "patch"), rule("batch", []string{"jobs"}, "get", "list", "watch", "create", "delete")}))
	plan.AppBindings = append(plan.AppBindings, binding(c.Namespace, "sdo-script-controller", "sdo-controller", c.controlNamespace()))
	// Preserve explicitly installed cluster-resource grants, including observation
	// grants, without introducing a new cluster-wide repair permission.
	for _, bindings := range []*[]*rbacv1.ClusterRoleBinding{&plan.ClusterBindings, &plan.ObservationClusterBindings} {
		for _, existing := range *bindings {
			owned := false
			for _, subject := range existing.Subjects {
				if subject.Namespace == c.controlNamespace() && (subject.Name == "sdo-controller" || subject.Name == "sdo-operate" || subject.Name == "sdo-responder") {
					owned = true
				}
			}
			if owned {
				copy := existing.DeepCopy()
				copy.Name = strings.TrimSuffix(existing.Name, "-script") + "-script"
				copy.Subjects = []rbacv1.Subject{{Kind: "ServiceAccount", Name: "sdo-script", Namespace: c.Namespace}}
				*bindings = append(*bindings, copy)
			}
		}
	}
}
