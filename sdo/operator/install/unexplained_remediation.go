package install

import (
	"context"
	"fmt"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"slices"
	"strings"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Only the removal identity receives the additional get/delete grants. The
// controller may impersonate that exact non-workload user only at the checked
// removal callback; generic plans retain the controller identity.
func addUnexplainedRemovalGrants(c Config, p *Plan, resources []*metav1.APIResourceList) error {
	// Co-located untrusted workloads can select the controller ServiceAccount.
	// Keep added authority disabled unless control credentials are isolated.
	if c.controlNamespace() == c.Namespace {
		return nil
	}
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

	if len(namespaced) == 0 && len(cluster) == 0 {
		return nil
	}
	identity := "sdo-unexplained-removal:" + c.controlNamespace()
	name := "sdo-unexplained-removal-" + c.controlNamespace()
	impersonate := rule("", []string{"users"}, "impersonate")
	impersonate.ResourceNames = []string{identity}
	impersonationName := name + "-impersonation"
	p.ClusterRoles = append(p.ClusterRoles, &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: impersonationName}, Rules: []rbacv1.PolicyRule{impersonate}})
	p.ClusterBindings = append(p.ClusterBindings, &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: impersonationName}, RoleRef: rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: impersonationName}, Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Name: "sdo-controller", Namespace: c.controlNamespace()}}})
	if len(namespaced) > 0 {
		p.AppRoles = append(p.AppRoles, role(c.Namespace, name, namespaced))
		p.AppBindings = append(p.AppBindings, &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: c.Namespace}, RoleRef: rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: name}, Subjects: []rbacv1.Subject{{Kind: "User", APIGroup: rbacv1.GroupName, Name: identity}}})
	}
	if len(cluster) > 0 {
		p.ClusterRoles = append(p.ClusterRoles, &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: name}, Rules: cluster})
		p.ClusterBindings = append(p.ClusterBindings, &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: name}, RoleRef: rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: name}, Subjects: []rbacv1.Subject{{Kind: "User", APIGroup: rbacv1.GroupName, Name: identity}}})
	}
	return nil
}

// Reinstallation must revoke retired grants, including #280's controller-bound
// authority in a co-located namespace, before applying the current plan.
func (i Installer) reconcileUnexplainedRemovalGrants(ctx context.Context, cfg Config, plan Plan) error {
	name := "sdo-unexplained-removal-" + cfg.controlNamespace()
	desiredRoles := map[string]bool{}
	for _, role := range plan.AppRoles {
		if role.Namespace == cfg.Namespace {
			desiredRoles[role.Name] = true
		}
	}
	if !desiredRoles[name] {
		if err := i.Client.RbacV1().RoleBindings(cfg.Namespace).Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("revoke removal RoleBinding: %w", err)
		}
		if err := i.Client.RbacV1().Roles(cfg.Namespace).Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("revoke removal Role: %w", err)
		}
	}
	desiredCluster := map[string]bool{}
	for _, role := range plan.ClusterRoles {
		desiredCluster[role.Name] = true
	}
	for _, name := range []string{name, name + "-impersonation"} {
		if desiredCluster[name] {
			continue
		}
		if err := i.Client.RbacV1().ClusterRoleBindings().Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("revoke removal ClusterRoleBinding: %w", err)
		}
		if err := i.Client.RbacV1().ClusterRoles().Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("revoke removal ClusterRole: %w", err)
		}
	}
	return nil
}
