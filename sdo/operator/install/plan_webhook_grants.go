package install

import (
	"context"
	"fmt"
	"slices"

	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const PlanWebhookGrantAnnotation = "sdo.dev/named-plan-webhooks"

func planWebhookGrantName(namespace string) string { return "sdo-plan-webhooks-" + namespace }
func addPlanWebhookGrants(c Config, p *Plan) {
	if len(c.PlanWebhookNames) == 0 {
		return
	}
	names := slices.Clone(c.PlanWebhookNames)
	slices.Sort(names)
	names = slices.Compact(names)
	r := rule("admissionregistration.k8s.io", []string{"mutatingwebhookconfigurations", "validatingwebhookconfigurations"}, "get", "delete")
	r.ResourceNames = names
	name := planWebhookGrantName(c.controlNamespace())
	p.ClusterRoles = append(p.ClusterRoles, &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: name}, Rules: []rbacv1.PolicyRule{r}})
	p.ClusterBindings = append(p.ClusterBindings, &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: name}, RoleRef: rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: name}, Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Name: "sdo-controller", Namespace: c.controlNamespace()}}})
}

func (i Installer) removeUnconfiguredPlanWebhookGrant(ctx context.Context, c Config) error {
	if len(c.PlanWebhookNames) > 0 {
		return nil
	}
	name := planWebhookGrantName(c.controlNamespace())
	if err := i.Client.RbacV1().ClusterRoleBindings().Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("revoke named plan webhook binding: %w", err)
	}
	if err := i.Client.RbacV1().ClusterRoles().Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("revoke named plan webhook role: %w", err)
	}
	return nil
}
