package install

import (
	"k8s.io/utils/ptr"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Only the removal identity receives the additional get/delete grants. The
// controller may impersonate that exact ServiceAccount only at the checked
// removal callback; generic plans retain the controller identity.
func addUnexplainedRemovalGrants(c Config, p *Plan, resources []*metav1.APIResourceList) error {
	var namespaced, cluster []rbacv1.PolicyRule
	seen := map[string]bool{}
	for _, list := range resources {
		gv, err := schema.ParseGroupVersion(list.GroupVersion)
		if err != nil {
			return err
		}
		for _, resource := range list.APIResources {
			if !inventoryWatchResource(gv.Group, resource.Name) || resource.Kind == "Secret" || resource.Name == "secrets" || strings.Contains(resource.Name, "/") || !slices.Contains(resource.Verbs, "get") || !slices.Contains(resource.Verbs, "delete") {
				continue
			}
			key := gv.Group + "/" + resource.Name
			if seen[key] {
				continue
			}
			seen[key] = true
			grant := rule(gv.Group, []string{resource.Name}, "get", "delete")
			if resource.Namespaced {
				namespaced = append(namespaced, grant)
			} else {
				cluster = append(cluster, grant)
			}
		}
	}
	identity := "sdo-unexplained-removal"
	p.ServiceAccounts = append(p.ServiceAccounts, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: identity, Namespace: c.controlNamespace()}, AutomountServiceAccountToken: ptr.To(false)})
	impersonate := rule("", []string{"serviceaccounts"}, "impersonate")
	impersonate.ResourceNames = []string{identity}
	name := "sdo-unexplained-removal-" + c.controlNamespace()
	p.ControlRoles = append(p.ControlRoles, role(c.controlNamespace(), name, []rbacv1.PolicyRule{impersonate}))
	p.ControlBindings = append(p.ControlBindings, binding(c.controlNamespace(), name, "sdo-controller", c.controlNamespace()))
	if len(namespaced) > 0 {
		p.AppRoles = append(p.AppRoles, role(c.Namespace, name, namespaced))
		p.AppBindings = append(p.AppBindings, binding(c.Namespace, name, identity, c.controlNamespace()))
	}
	if len(cluster) > 0 {
		p.ClusterRoles = append(p.ClusterRoles, &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: name}, Rules: cluster})
		p.ClusterBindings = append(p.ClusterBindings, &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: name}, RoleRef: rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: name}, Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Name: identity, Namespace: c.controlNamespace()}}})
	}
	return nil
}
