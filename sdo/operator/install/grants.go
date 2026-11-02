package install

import (
	"context"
	"fmt"

	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SDO does not limit its identities through Kubernetes RBAC. Every
// ServiceAccount SDO runs as (controller, operate, responder, image builder,
// script Jobs) is bound to one wildcard ClusterRole, so no repair, replay or
// diagnosis is refused for a missing grant. What a program or responder may do
// is decided by runtime policy and review, not by per-kind Roles.
func adminGrantName(control string) string { return "sdo-admin-" + control }

func addAdminGrant(c Config, plan *Plan) {
	control := c.controlNamespace()
	name := adminGrantName(control)
	subjects := []rbacv1.Subject{{Kind: "ServiceAccount", Name: "sdo-script", Namespace: c.Namespace}}
	for _, account := range []string{"sdo-controller", "sdo-operate", "sdo-responder", "sdo-image-builder"} {
		subjects = append(subjects, rbacv1.Subject{Kind: "ServiceAccount", Name: account, Namespace: control})
	}
	if control != c.Namespace {
		// The controller impersonates this user for the checked removal callback.
		subjects = append(subjects, rbacv1.Subject{Kind: "User", APIGroup: rbacv1.GroupName, Name: UnexplainedRemovalUser(control)})
	}
	plan.ClusterRoles = append(plan.ClusterRoles, &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: name}, Rules: []rbacv1.PolicyRule{{APIGroups: []string{"*"}, Resources: []string{"*"}, Verbs: []string{"*"}}}})
	plan.ClusterBindings = append(plan.ClusterBindings, &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: name}, RoleRef: rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: name}, Subjects: subjects})
}

// UnexplainedRemovalUser is the identity the runtime impersonates to remove an
// object the program did not explain.
func UnexplainedRemovalUser(control string) string { return "sdo-unexplained-removal:" + control }

// ensureClusterGrants applies the plan's ClusterRoles and bindings. Fresh
// installs, install-existing and inspect-existing all go through it, so a
// reinstall restores the grant after a manual edit or a namespace recreation.
func (i Installer) ensureClusterGrants(ctx context.Context, p Plan) error {
	for _, want := range p.ClusterRoles {
		roles := i.Client.RbacV1().ClusterRoles()
		old, err := roles.Get(ctx, want.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			_, err = roles.Create(ctx, want, metav1.CreateOptions{})
		} else if err == nil {
			updated := want.DeepCopy()
			updated.ResourceVersion = old.ResourceVersion
			_, err = roles.Update(ctx, updated, metav1.UpdateOptions{})
		}
		if err != nil {
			return fmt.Errorf("ensure ClusterRole %s: %w", want.Name, err)
		}
	}
	for _, want := range p.ClusterBindings {
		bindings := i.Client.RbacV1().ClusterRoleBindings()
		old, err := bindings.Get(ctx, want.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			_, err = bindings.Create(ctx, want, metav1.CreateOptions{})
		} else if err == nil {
			updated := want.DeepCopy()
			updated.ResourceVersion = old.ResourceVersion
			_, err = bindings.Update(ctx, updated, metav1.UpdateOptions{})
		}
		if err != nil {
			return fmt.Errorf("ensure ClusterRoleBinding %s: %w", want.Name, err)
		}
	}
	return nil
}
