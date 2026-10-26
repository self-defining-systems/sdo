package install

import (
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"slices"
	"strings"
)

func captureGrantName(control string) string { return control + "-object-capture" }

// Capture needs the same object universe as incident responders. Grant only
// concrete discovered resources and read verbs, to the controller alone. The
// capture reader still bounds collection to app and incident-touched scopes.
// Kubernetes RBAC has no subtraction, so Secrets cannot use a wildcard grant.
func addCaptureGrants(c Config, p *Plan, resources []*metav1.APIResourceList) error {
	// The controller's informers list and watch these cluster-wide whatever
	// discovery returns; without the grant they never sync and it is blind.
	var rules []rbacv1.PolicyRule
	seen := map[string]bool{}
	for _, resource := range []string{"rolebindings", "clusterroles", "clusterrolebindings"} {
		seen["rbac.authorization.k8s.io/"+resource] = true
		rules = append(rules, rule("rbac.authorization.k8s.io", []string{resource}, "get", "list", "watch"))
	}
	for _, list := range resources {
		gv, err := schema.ParseGroupVersion(list.GroupVersion)
		if err != nil {
			return err
		}
		for _, resource := range list.APIResources {
			if strings.Contains(resource.Name, "/") || resource.Kind == "Secret" || (gv.Group == "" && resource.Name == "secrets") {
				continue
			}
			verbs := []string{}
			readVerbs := []string{"get", "list"}
			if inventoryWatchResource(gv.Group, resource.Name) {
				readVerbs = append(readVerbs, "watch")
			}
			for _, verb := range readVerbs {
				if slices.Contains(resource.Verbs, verb) {
					verbs = append(verbs, verb)
				}
			}
			key := gv.Group + "/" + resource.Name
			if len(verbs) == 0 || seen[key] {
				continue
			}
			seen[key] = true
			rules = append(rules, rule(gv.Group, []string{resource.Name}, verbs...))
		}
	}
	if len(rules) == 0 {
		return nil
	}
	slices.SortFunc(rules, func(a, b rbacv1.PolicyRule) int {
		return strings.Compare(a.APIGroups[0]+"/"+a.Resources[0], b.APIGroups[0]+"/"+b.Resources[0])
	})
	name := captureGrantName(c.controlNamespace())
	p.ClusterRoles = append(p.ClusterRoles, &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: name}, Rules: rules})
	p.ClusterBindings = append(p.ClusterBindings, &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: name}, RoleRef: rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: name}, Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Name: "sdo-controller", Namespace: c.controlNamespace()}}})
	return nil
}

// Only the source inventory's reviewed non-secret collections need watch.
func inventoryWatchResource(group, resource string) bool {
	switch group + "/" + resource {
	case "/configmaps", "/services", "/serviceaccounts", "/pods", "/persistentvolumeclaims",
		"apps/deployments", "apps/statefulsets", "apps/daemonsets", "apps/replicasets",
		"batch/jobs", "batch/cronjobs", "networking.k8s.io/networkpolicies",
		"rbac.authorization.k8s.io/roles", "rbac.authorization.k8s.io/rolebindings",
		"rbac.authorization.k8s.io/clusterroles", "rbac.authorization.k8s.io/clusterrolebindings":
		return true
	}
	return false
}
