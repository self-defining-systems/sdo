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
const permissiveReadOwnerLabel = "sdo.dev/responder-read-owner"

func permissiveReadGrantName(control string) string { return permissiveGrantName(control) + "-read" }

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
	readRules := []rbacv1.PolicyRule{}
	clusterRules := []rbacv1.PolicyRule{}
	seen := map[string]bool{}
	for _, list := range resources {
		gv, err := schema.ParseGroupVersion(list.GroupVersion)
		if err != nil {
			return fmt.Errorf("discover responder API resources: %w", err)
		}
		for _, r := range list.APIResources {
			verbs := permissiveResourceVerbs(gv.Group, r.Name)
			if !r.Namespaced {
				if permissiveClusterRead(gv.Group, r.Name) && !seen["cluster/"+gv.Group+"/"+r.Name] {
					seen["cluster/"+gv.Group+"/"+r.Name] = true
					clusterRules = append(clusterRules, rule(gv.Group, []string{r.Name}, "get", "list", "watch"))
				}
				continue
			}
			if len(verbs) == 0 {
				continue
			}
			key := gv.Group + "/" + r.Name
			if seen[key] {
				continue
			}
			seen[key] = true
			rules = append(rules, rule(gv.Group, []string{r.Name}, verbs...))
			if permissiveNamespacedRead(gv.Group, r.Name) {
				readRules = append(readRules, rule(gv.Group, []string{r.Name}, "get", "list", "watch"))
			}
		}
	}
	if len(rules) == 0 {
		return fmt.Errorf("discovery returned no safe namespaced responder resources")
	}
	for _, policy := range [][]rbacv1.PolicyRule{rules, readRules, clusterRules} {
		slices.SortFunc(policy, func(a, b rbacv1.PolicyRule) int {
			return strings.Compare(a.APIGroups[0]+"/"+a.Resources[0], b.APIGroups[0]+"/"+b.Resources[0])
		})
	}
	name := permissiveGrantName(c.controlNamespace())
	namespaces := append([]string{c.Namespace}, c.ResponderNamespaces...)
	slices.Sort(namespaces)
	for _, ns := range slices.Compact(namespaces) {
		p.AppRoles = append(p.AppRoles, role(ns, name, rules))
		p.AppBindings = append(p.AppBindings, binding(ns, name, "sdo-responder", c.controlNamespace()))
	}
	clusterRules = append(clusterRules, rule("admissionregistration.k8s.io", []string{"mutatingwebhookconfigurations", "validatingwebhookconfigurations"}, "get", "list", "watch", "create", "update", "patch", "delete"))
	p.ClusterRoles = append(p.ClusterRoles, &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: name}, Rules: clusterRules}, &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: permissiveReadGrantName(c.controlNamespace())}, Rules: readRules})
	addPermissiveReadBindings(c, p, namespaces)
	p.ClusterBindings = append(p.ClusterBindings, &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: name},
		Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: "sdo-responder", Namespace: c.controlNamespace()}},
	})
	return nil
}

// Discovery cannot authorize unknown APIs or credential, identity, proxy and
// escalation endpoints. These reviewed names are intentionally concrete.
func permissiveNamespacedRead(group, resource string) bool {
	switch group {
	case "":
		return slices.Contains([]string{"pods", "pods/log", "services", "endpoints", "events", "configmaps", "persistentvolumeclaims", "resourcequotas", "limitranges", "replicationcontrollers"}, resource)
	case "apps":
		return slices.Contains([]string{"deployments", "replicasets", "statefulsets", "daemonsets", "controllerrevisions", "deployments/scale", "replicasets/scale", "statefulsets/scale"}, resource)
	case "batch":
		return slices.Contains([]string{"jobs", "cronjobs"}, resource)
	case "autoscaling":
		return resource == "horizontalpodautoscalers"
	case "policy":
		return resource == "poddisruptionbudgets"
	case "discovery.k8s.io":
		return resource == "endpointslices"
	case "networking.k8s.io":
		return slices.Contains([]string{"networkpolicies", "ingresses"}, resource)
	case "events.k8s.io":
		return resource == "events"
	case "coordination.k8s.io":
		return resource == "leases"
	case "storage.k8s.io":
		return resource == "csistoragecapacities"
	}
	return false
}

func permissiveClusterRead(group, resource string) bool {
	switch group {
	case "":
		return slices.Contains([]string{"nodes", "namespaces", "persistentvolumes"}, resource)
	case "storage.k8s.io":
		return slices.Contains([]string{"storageclasses", "csidrivers", "csinodes", "volumeattachments"}, resource)
	case "networking.k8s.io":
		return resource == "ingressclasses"
	case "scheduling.k8s.io":
		return resource == "priorityclasses"
	}
	return false
}

func permissiveResourceVerbs(group, resource string) []string {
	if group == "" && slices.Contains([]string{"pods/exec", "pods/attach", "pods/portforward"}, resource) {
		return []string{"get", "create"}
	}
	if !permissiveNamespacedRead(group, resource) {
		return nil
	}
	verbs := []string{"get", "list", "watch"}
	switch group {
	case "":
		if slices.Contains([]string{"pods", "services", "configmaps", "persistentvolumeclaims", "replicationcontrollers"}, resource) {
			return append(verbs, "create", "update", "patch", "delete")
		}
	case "apps":
		if strings.HasSuffix(resource, "/scale") {
			return []string{"get", "update", "patch"}
		}
		if resource != "controllerrevisions" {
			return append(verbs, "create", "update", "patch", "delete")
		}
	case "batch", "autoscaling", "policy", "networking.k8s.io":
		return append(verbs, "create", "update", "patch", "delete")
	}
	return verbs
}

// A ClusterRoleBinding containing namespaced reads would also grant the control
// namespace. Bind the shared read role separately in each admitted namespace.
func addPermissiveReadBindings(c Config, p *Plan, namespaces []string) {
	name := permissiveReadGrantName(c.controlNamespace())
	for _, ns := range namespaces {
		if ns == "" || ns == c.controlNamespace() {
			continue
		}
		exists := slices.ContainsFunc(p.AppBindings, func(b *rbacv1.RoleBinding) bool { return b.Name == name && b.Namespace == ns })
		if exists {
			continue
		}
		p.AppBindings = append(p.AppBindings, &rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: map[string]string{permissiveReadOwnerLabel: c.controlNamespace()}},
			RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: name},
			Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: "sdo-responder", Namespace: c.controlNamespace()}},
		})
	}
	// Discovery can extend the plan after Resources applied its ownership labels.
	labelInstallObjects(c.Namespace, p.AppBindings...)
	slices.SortFunc(p.AppBindings, func(a, b *rbacv1.RoleBinding) int {
		return strings.Compare(a.Namespace+"/"+a.Name, b.Namespace+"/"+b.Name)
	})
}

func (i Installer) resources(c Config) (Plan, error) {
	_, resources, err := i.Client.Discovery().ServerGroupsAndResources()
	if err != nil {
		return Plan{}, fmt.Errorf("discover controller/responder resources: %w", err)
	}
	plan, err := Resources(c, resources...)
	if err != nil {
		return Plan{}, err
	}
	if !c.PermissiveResponder {
		return plan, nil
	}
	namespaces, err := i.Client.CoreV1().Namespaces().List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return Plan{}, fmt.Errorf("discover responder read namespaces: %w", err)
	}
	names := make([]string, 0, len(namespaces.Items))
	for _, ns := range namespaces.Items {
		names = append(names, ns.Name)
	}
	addPermissiveReadBindings(c, &plan, names)
	return plan, nil
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
		bindings, err := i.Client.RbacV1().RoleBindings(metav1.NamespaceAll).List(ctx, metav1.ListOptions{LabelSelector: permissiveReadOwnerLabel + "=" + c.controlNamespace()})
		if err != nil {
			return err
		}
		for _, b := range bindings.Items {
			if b.Name != permissiveReadGrantName(c.controlNamespace()) || b.RoleRef.Kind != "ClusterRole" || b.RoleRef.Name != b.Name {
				continue
			}
			if err := i.Client.RbacV1().RoleBindings(b.Namespace).Delete(ctx, b.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
		}
		if err := i.Client.RbacV1().ClusterRoles().Delete(ctx, permissiveReadGrantName(c.controlNamespace()), metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
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
