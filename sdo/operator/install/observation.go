package install

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strconv"
	"strings"

	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Read grants belong to the controller only. They grant no foreign Secret read
// and no repair access; ambient permissive grants cannot widen runtime policy.
func addObservationGrants(plan *Plan, c Config) {
	for _, t := range c.ObservationCeiling.Targets {
		name := fmt.Sprintf("sdo-observe-%s-%x", c.Namespace, sha256.Sum256([]byte(t.Key())))
		labels := map[string]string{observationOwnerLabel: c.Namespace}
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
			plan.ObservationClusterRoles = append(plan.ObservationClusterRoles, &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels}, Rules: []rbacv1.PolicyRule{r}})
			plan.ObservationClusterBindings = append(plan.ObservationClusterBindings, &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels}, Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Name: "sdo-controller", Namespace: c.controlNamespace()}, {Kind: "ServiceAccount", Name: "sdo-operate", Namespace: c.controlNamespace()}}, RoleRef: rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: name}})
		} else {
			observedRole := role(t.Namespace, name, []rbacv1.PolicyRule{r})
			observedRole.Labels = labels
			plan.ObservationRoles = append(plan.ObservationRoles, observedRole)
			b := binding(t.Namespace, name, "sdo-controller", c.controlNamespace())
			b.Labels = labels
			b.Subjects = append(b.Subjects, rbacv1.Subject{Kind: "ServiceAccount", Name: "sdo-operate", Namespace: c.controlNamespace()})
			plan.ObservationBindings = append(plan.ObservationBindings, b)
		}
	}
}

const observationOwnerLabel = "sdo.dev/observation-owner"

// observationOwned recognizes stable owned identities, plus the reserved
// index names emitted by the original installer for upgrade cleanup.
func observationOwned(meta metav1.Object, app string) bool {
	if owner := meta.GetLabels()[observationOwnerLabel]; owner != "" {
		return owner == app
	}
	suffix, ok := strings.CutPrefix(meta.GetName(), "sdo-observe-"+app+"-")
	if !ok || suffix == "" {
		return false
	}
	index, err := strconv.ParseUint(suffix, 10, 32)
	return err == nil && strconv.FormatUint(index, 10) == suffix
}

// Reconcile all four grant kinds, including old namespaces and control-account
// subjects. Revoke bindings before roles, and never touch other installers.
func (i Installer) reconcileObservationGrants(ctx context.Context, cfg Config, plan Plan) error {
	desired := map[string]bool{}
	for _, r := range plan.ObservationRoles {
		desired["Role/"+r.Namespace+"/"+r.Name] = true
	}
	for _, b := range plan.ObservationBindings {
		desired["RoleBinding/"+b.Namespace+"/"+b.Name] = true
	}
	for _, r := range plan.ObservationClusterRoles {
		desired["ClusterRole//"+r.Name] = true
	}
	for _, b := range plan.ObservationClusterBindings {
		desired["ClusterRoleBinding//"+b.Name] = true
	}
	stale := func(kind string, obj metav1.Object) bool {
		return observationOwned(obj, cfg.Namespace) && !desired[kind+"/"+obj.GetNamespace()+"/"+obj.GetName()]
	}
	rb, err := i.Client.RbacV1().RoleBindings("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list observation RoleBindings: %w", err)
	}
	for j := range rb.Items {
		b := &rb.Items[j]
		if stale("RoleBinding", b) {
			if err := i.Client.RbacV1().RoleBindings(b.Namespace).Delete(ctx, b.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
				return fmt.Errorf("revoke observation RoleBinding %s/%s: %w", b.Namespace, b.Name, err)
			}
		}
	}
	cb, err := i.Client.RbacV1().ClusterRoleBindings().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list observation ClusterRoleBindings: %w", err)
	}
	for j := range cb.Items {
		b := &cb.Items[j]
		if stale("ClusterRoleBinding", b) {
			if err := i.Client.RbacV1().ClusterRoleBindings().Delete(ctx, b.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
				return fmt.Errorf("revoke observation ClusterRoleBinding %s: %w", b.Name, err)
			}
		}
	}
	roles, err := i.Client.RbacV1().Roles("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list observation Roles: %w", err)
	}
	for j := range roles.Items {
		r := &roles.Items[j]
		if stale("Role", r) {
			if err := i.Client.RbacV1().Roles(r.Namespace).Delete(ctx, r.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
				return fmt.Errorf("revoke observation Role %s/%s: %w", r.Namespace, r.Name, err)
			}
		}
	}
	cr, err := i.Client.RbacV1().ClusterRoles().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list observation ClusterRoles: %w", err)
	}
	for j := range cr.Items {
		r := &cr.Items[j]
		if stale("ClusterRole", r) {
			if err := i.Client.RbacV1().ClusterRoles().Delete(ctx, r.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
				return fmt.Errorf("revoke observation ClusterRole %s: %w", r.Name, err)
			}
		}
	}
	return nil
}
