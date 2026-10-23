package install

import (
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func secretPathDiscovery() []*metav1.APIResourceList {
	resources := discoveryFixture()
	for group, names := range map[string][]string{
		"v1":                     {"services", "endpoints", "events", "pods/log", "pods/proxy", "services/proxy", "pods/attach", "pods/portforward", "pods/ephemeralcontainers", "persistentvolumeclaims"},
		"apps/v1":                {"replicasets", "statefulsets", "daemonsets", "deployments/scale", "deployments/status"},
		"discovery.k8s.io/v1":    {"endpointslices"},
		"events.k8s.io/v1":       {"events"},
		"networking.k8s.io/v1":   {"networkpolicies", "ingresses"},
		"batch/v1":               {"jobs", "cronjobs"},
		"external-secrets.io/v1": {"externalsecrets", "externalsecrets/status", "secretstores"},
		// Benign names and kinds are not evidence that an API is safe.
		"unreviewed.example/v1": {"pods", "deployments", "credentials", "credentials/status"},
	} {
		list := &metav1.APIResourceList{GroupVersion: group}
		for _, name := range names {
			list.APIResources = append(list.APIResources, metav1.APIResource{Name: name, Namespaced: true})
		}
		resources = append(resources, list)
	}
	return resources
}

func secretPathPlan(t *testing.T) Plan {
	t.Helper()
	cfg := testConfig()
	cfg.PermissiveResponder = true
	cfg.ResponderNamespaces = []string{"policy-system"}
	plan, err := Resources(cfg, secretPathDiscovery()...)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

// These tests exercise authorization prerequisites of each RV9 blocking path.
// They do not simulate a controller, admission or data exfiltration. Positive
// controls ensure that these are bound namespaces and the permitted repair works.
func TestPermissiveBackendSecretPathsDenied(t *testing.T) {
	plan := secretPathPlan(t)
	paths := []struct {
		name, group, resource string
		verbs                 []string
	}{
		{"mount_or_project_identity", "", "pods", []string{"create", "update", "patch", "deletecollection"}},
		{"select_serviceaccount", "", "serviceaccounts", []string{"get", "list", "watch", "create", "update", "patch", "delete"}},
		{"copy_in_configmap", "", "configmaps", []string{"get", "list", "watch", "create", "update", "patch"}},
		{"log_disclosure", "", "pods/log", []string{"get", "list", "watch"}},
		{"pod_proxy_disclosure", "", "pods/proxy", []string{"get", "create"}},
		{"service_proxy_disclosure", "", "services/proxy", []string{"get", "create"}},
		{"helper_job", "batch", "jobs", []string{"get", "list", "watch", "create", "update", "patch", "delete"}},
		{"scheduled_job", "batch", "cronjobs", []string{"get", "list", "watch", "create", "update", "patch", "delete"}},
	}
	for _, path := range paths {
		t.Run(path.name, func(t *testing.T) {
			if !planAllows(plan, "policy-system", "", "pods", "delete") {
				t.Fatal("positive control: backend restart denied")
			}
			for _, verb := range path.verbs {
				if planAllows(plan, "policy-system", path.group, path.resource, verb) {
					t.Errorf("backend Secret path allowed: %s/%s", path.resource, verb)
				}
			}
		})
	}
}

func TestPermissiveWebhookBackendTakeoverDenied(t *testing.T) {
	plan := secretPathPlan(t)
	for group, resources := range map[string][]string{
		"":     {"pods", "services"},
		"apps": {"deployments", "replicasets", "statefulsets", "daemonsets", "deployments/scale", "deployments/status"},
	} {
		for _, resource := range resources {
			for _, verb := range []string{"create", "update", "patch", "deletecollection"} {
				if planAllows(plan, "policy-system", group, resource, verb) {
					t.Errorf("backend takeover allowed: %s/%s", resource, verb)
				}
			}
			if resource != "pods" && planAllows(plan, "policy-system", group, resource, "delete") {
				t.Errorf("backend deletion allowed: %s", resource)
			}
		}
	}
	for _, resource := range []string{"mutatingwebhookconfigurations", "validatingwebhookconfigurations"} {
		for _, verb := range []string{"get", "list", "watch", "delete"} {
			if !planAllows(plan, "", "admissionregistration.k8s.io", resource, verb) {
				t.Errorf("webhook repair denied: %s/%s", resource, verb)
			}
		}
	}
}

func TestPermissiveCustomSecretControllersDenied(t *testing.T) {
	plan := secretPathPlan(t)
	for _, ns := range []string{"demo", "policy-system", "demo-sdo", "unrelated", ""} {
		for group, resources := range map[string][]string{
			"external-secrets.io": {"externalsecrets", "externalsecrets/status", "secretstores"},
			"example.dev":         {"widgets"},
			"unreviewed.example":  {"pods", "deployments", "credentials", "credentials/status"},
		} {
			for _, resource := range resources {
				for _, verb := range []string{"get", "list", "watch", "create", "update", "patch", "delete", "deletecollection"} {
					if planAllows(plan, ns, group, resource, verb) {
						t.Errorf("custom controller allowed: %s/%s/%s/%s", ns, group, resource, verb)
					}
				}
			}
		}
	}
	if !planAllows(plan, "demo", "apps", "deployments", "patch") {
		t.Fatal("baseline app repair lost")
	}
}

func TestPermissiveReviewedBuiltinsOnly(t *testing.T) {
	plan := secretPathPlan(t)
	allowed := map[string][]string{
		"":                  {"pods", "services", "endpoints", "events"},
		"apps":              {"deployments", "replicasets", "statefulsets", "daemonsets"},
		"discovery.k8s.io":  {"endpointslices"},
		"networking.k8s.io": {"networkpolicies"},
	}
	for group, resources := range allowed {
		for _, resource := range resources {
			for _, verb := range []string{"get", "list", "watch", "create", "update", "patch", "delete", "deletecollection", "bind", "escalate", "impersonate"} {
				want := verb == "get" || verb == "list" || verb == "watch" || (group == "" && resource == "pods" && verb == "delete")
				if got := planAllows(plan, "policy-system", group, resource, verb); got != want {
					t.Errorf("%s/%s/%s allowed=%v want=%v", group, resource, verb, got, want)
				}
			}
		}
	}
	// Every added namespaced rule is checked, even if future discovery expands.
	for _, role := range plan.AppRoles {
		if role.Name != permissiveGrantName("demo-sdo") {
			continue
		}
		for _, rule := range role.Rules {
			for _, group := range rule.APIGroups {
				for _, resource := range rule.Resources {
					reviewed := false
					for _, name := range allowed[group] {
						if name == resource {
							reviewed = true
						}
					}
					if !reviewed {
						t.Errorf("unreviewed grant: %s/%s", group, resource)
					}
				}
			}
		}
	}
}

func TestPlanAllowsClusterRoleRefsAndGroups(t *testing.T) {
	for _, subject := range []rbacv1.Subject{
		{Kind: "Group", Name: "system:authenticated"},
		{Kind: "Group", Name: "system:serviceaccounts"},
		{Kind: "Group", Name: "system:serviceaccounts:demo-sdo"},
		{Kind: "User", Name: "system:serviceaccount:demo-sdo:sdo-responder"},
		{Kind: "ServiceAccount", Name: "sdo-responder", Namespace: "demo-sdo"},
	} {
		t.Run(subject.Kind+"/"+subject.Name, func(t *testing.T) {
			p := Plan{
				ClusterRoles: []*rbacv1.ClusterRole{{ObjectMeta: metav1.ObjectMeta{Name: "ambient"}, Rules: []rbacv1.PolicyRule{rule("", []string{"secrets", "*/proxy"}, "get")}}},
				AppBindings:  []*rbacv1.RoleBinding{{ObjectMeta: metav1.ObjectMeta{Name: "binding", Namespace: "policy-system"}, RoleRef: rbacv1.RoleRef{Kind: "ClusterRole", Name: "ambient"}, Subjects: []rbacv1.Subject{subject}}},
			}
			if !planAllows(p, "policy-system", "", "secrets", "get") || !planAllows(p, "policy-system", "", "services/proxy", "get") {
				t.Fatal("ClusterRole reference or subject not resolved")
			}
			if planAllows(p, "unrelated", "", "secrets", "get") {
				t.Fatal("RoleBinding escaped namespace")
			}
			p.ClusterBindings = []*rbacv1.ClusterRoleBinding{{RoleRef: rbacv1.RoleRef{Kind: "ClusterRole", Name: "ambient"}, Subjects: []rbacv1.Subject{subject}}}
			if !planAllows(p, "unrelated", "", "secrets", "get") {
				t.Fatal("cluster group binding not resolved")
			}
			p.AppBindings[0].Subjects = []rbacv1.Subject{{Kind: "Group", Name: "system:serviceaccounts:other"}}
			p.ClusterBindings = nil
			if planAllows(p, "policy-system", "", "secrets", "get") {
				t.Fatal("unrelated group matched")
			}
			p.AppBindings[0].Subjects = []rbacv1.Subject{subject}
			p.AppBindings[0].RoleRef.Kind = "Role"
			if planAllows(p, "policy-system", "", "secrets", "get") {
				t.Fatal("Role ref resolved to ClusterRole")
			}
		})
	}
}
