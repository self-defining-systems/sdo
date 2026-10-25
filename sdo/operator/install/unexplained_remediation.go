package install

import (
	"slices"
	"strings"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Only the controller receives get/delete authority for discovered non-secret
// objects. Workers keep their existing grants. The typed menu checks current
// source absence, onboarding, control ownership and exact UID/RV in Go.
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
	name := c.controlNamespace() + "-unexplained-removal"
	if len(namespaced) > 0 {
		p.AppRoles = append(p.AppRoles, role(c.Namespace, name, namespaced))
		p.AppBindings = append(p.AppBindings, binding(c.Namespace, name, "sdo-controller", c.controlNamespace()))
	}
	if len(cluster) > 0 {
		p.ClusterRoles = append(p.ClusterRoles, &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: name}, Rules: cluster})
		p.ClusterBindings = append(p.ClusterBindings, &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: name}, RoleRef: rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: name}, Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Name: "sdo-controller", Namespace: c.controlNamespace()}}})
	}
	return nil
}
