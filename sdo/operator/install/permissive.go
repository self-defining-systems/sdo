package install

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const PermissiveResponderAnnotation = "sdo.dev/permissive-responder-namespaces"

func permissiveGrantName(control string) string { return control + "-responder-permissive" }

// Enumerate concrete names rather than granting resources=*; Kubernetes RBAC
// is additive and cannot subtract Secret access from a wildcard or cluster-admin.
func addPermissiveGrants(c Config, p *Plan, resources []*metav1.APIResourceList) error {
	if !c.PermissiveResponder {
		if len(c.ResponderNamespaces) != 0 {
			return fmt.Errorf("responder namespaces require permissive responder opt-in")
		}
		return nil
	}
	if c.Namespace == c.controlNamespace() {
		return fmt.Errorf("permissive responder requires a separate control namespace")
	}
	for _, ns := range c.ResponderNamespaces {
		if !namespaceName.MatchString(ns) || ns == c.controlNamespace() || strings.HasPrefix(ns, "kube-") {
			return fmt.Errorf("invalid permissive responder namespace %q", ns)
		}
	}
	if len(resources) == 0 {
		return fmt.Errorf("permissive responder requires complete API resource discovery")
	}
	rules := []rbacv1.PolicyRule{}
	seen := map[string]bool{}
	for _, list := range resources {
		gv, err := schema.ParseGroupVersion(list.GroupVersion)
		if err != nil {
			return fmt.Errorf("discover responder API resources: %w", err)
		}
		for _, r := range list.APIResources {
			verbs := permissiveResourceVerbs(gv.Group, r.Name)
			if !r.Namespaced || len(verbs) == 0 {
				continue
			}
			key := gv.Group + "/" + r.Name
			if seen[key] {
				continue
			}
			seen[key] = true
			rules = append(rules, rule(gv.Group, []string{r.Name}, verbs...))
		}
	}
	if len(rules) == 0 {
		return fmt.Errorf("discovery returned no safe namespaced responder resources")
	}
	slices.SortFunc(rules, func(a, b rbacv1.PolicyRule) int {
		return strings.Compare(a.APIGroups[0]+"/"+a.Resources[0], b.APIGroups[0]+"/"+b.Resources[0])
	})
	name := permissiveGrantName(c.controlNamespace())
	namespaces := append([]string{c.Namespace}, c.ResponderNamespaces...)
	slices.Sort(namespaces)
	for _, ns := range slices.Compact(namespaces) {
		p.AppRoles = append(p.AppRoles, role(ns, name, rules))
		p.AppBindings = append(p.AppBindings, binding(ns, name, "sdo-responder", c.controlNamespace()))
	}
	p.ClusterRoles = append(p.ClusterRoles, &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: name}, Rules: []rbacv1.PolicyRule{
		rule("admissionregistration.k8s.io", []string{"mutatingwebhookconfigurations", "validatingwebhookconfigurations"}, "get", "list", "watch", "delete"),
	}})
	p.ClusterBindings = append(p.ClusterBindings, &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: name},
		Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: "sdo-responder", Namespace: c.controlNamespace()}},
	})
	return nil
}

// Discovery is not a trust decision. Only reviewed built-in group/resource
// pairs can receive added grants; the reviewed custom-API allowlist is empty.
// Do not add subresources or infer trust from Kind/resource names. The baseline
// app Role is unchanged; backend repairs can read these built-ins or delete a
// Pod so its existing controller recreates the same spec.
func permissiveResourceVerbs(group, resource string) []string {
	switch group {
	case "":
		switch resource {
		case "pods":
			return []string{"get", "list", "watch", "delete"}
		case "services", "endpoints", "events":
			return []string{"get", "list", "watch"}
		}
	case "apps":
		switch resource {
		case "deployments", "replicasets", "statefulsets", "daemonsets":
			return []string{"get", "list", "watch"}
		}
	case "discovery.k8s.io":
		if resource == "endpointslices" {
			return []string{"get", "list", "watch"}
		}
	case "networking.k8s.io":
		if resource == "networkpolicies" {
			return []string{"get", "list", "watch"}
		}
	}
	return nil
}

func (i Installer) resources(c Config) (Plan, error) {
	if !c.PermissiveResponder {
		return Resources(c)
	}
	_, resources, err := i.Client.Discovery().ServerGroupsAndResources()
	if err != nil {
		return Plan{}, fmt.Errorf("discover permissive responder resources: %w", err)
	}
	return Resources(c, resources...)
}

func (i Installer) ensurePermissiveClusterGrants(ctx context.Context, p Plan) error {
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

// Revoking the option or narrowing its namespace list must remove previous grants.
func (i Installer) removeStalePermissiveGrants(ctx context.Context, c Config, prior *batchv1.Job) error {
	settings := prior.Spec.Template.Annotations[PermissiveResponderAnnotation]
	if settings == "" {
		return nil
	}
	var namespaces []string
	if err := json.Unmarshal([]byte(settings), &namespaces); err != nil {
		return fmt.Errorf("decode previous permissive responder namespaces: %w", err)
	}
	name := permissiveGrantName(c.controlNamespace())
	namespaces = append(namespaces, c.Namespace)
	for _, ns := range namespaces {
		if c.PermissiveResponder && (ns == c.Namespace || slices.Contains(c.ResponderNamespaces, ns)) {
			continue
		}
		if err := i.Client.RbacV1().RoleBindings(ns).Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		if err := i.Client.RbacV1().Roles(ns).Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	if !c.PermissiveResponder {
		if err := i.Client.RbacV1().ClusterRoleBindings().Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		if err := i.Client.RbacV1().ClusterRoles().Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

// A fault may create its backend only during injection. Prepare its namespace
// now so the responder's Role already exists when that happens.
func (i Installer) ensurePermissiveNamespaces(ctx context.Context, c Config) error {
	if !c.PermissiveResponder {
		return nil
	}
	for _, ns := range c.ResponderNamespaces {
		if _, err := i.Client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("ensure responder namespace %s: %w", ns, err)
		}
	}
	return nil
}
