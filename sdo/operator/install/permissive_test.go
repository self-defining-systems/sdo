package install

import (
	"context"
	"encoding/json"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	fakediscovery "k8s.io/client-go/discovery/fake"
	"k8s.io/client-go/kubernetes/fake"
	"strings"
	"testing"
)

func discoveryFixture() []*metav1.APIResourceList {
	return []*metav1.APIResourceList{
		{GroupVersion: "v1", APIResources: []metav1.APIResource{
			{Name: "pods", Namespaced: true}, {Name: "pods/exec", Namespaced: true},
			{Name: "secrets", Namespaced: true}, {Name: "secrets/status", Namespaced: true},
			{Name: "serviceaccounts/token", Namespaced: true}, {Name: "nodes/proxy"},
			{Name: "serviceaccounts", Namespaced: true},
			{Name: "configmaps", Namespaced: true},
		}},
		{GroupVersion: "apps/v1", APIResources: []metav1.APIResource{{Name: "deployments", Namespaced: true}}},
		{GroupVersion: "example.dev/v1", APIResources: []metav1.APIResource{{Name: "widgets", Namespaced: true}}},
		{GroupVersion: "rbac.authorization.k8s.io/v1", APIResources: []metav1.APIResource{{Name: "roles", Namespaced: true}, {Name: "rolebindings", Namespaced: true}}},
	}
}

// This authorizer resolves the generated subject/binding/role graph, including
// wildcard matching, rather than asserting that particular rules were appended.
func planAllows(p Plan, namespace, group, resource, verb string) bool {
	subjectMatches := func(subjects []rbacv1.Subject) bool {
		for _, s := range subjects {
			if s.Kind == "ServiceAccount" && s.Name == "sdo-responder" && s.Namespace == "demo-sdo" {
				return true
			}
		}
		return false
	}
	matches := func(rules []rbacv1.PolicyRule) bool {
		for _, r := range rules {
			has := func(values []string, want string) bool {
				for _, v := range values {
					if v == "*" || v == want {
						return true
					}
				}
				return false
			}
			if has(r.APIGroups, group) && has(r.Resources, resource) && has(r.Verbs, verb) {
				return true
			}
		}
		return false
	}
	for _, b := range append(append([]*rbacv1.RoleBinding{}, p.ControlBindings...), p.AppBindings...) {
		if b.Namespace != namespace || !subjectMatches(b.Subjects) {
			continue
		}
		for _, r := range append(append([]*rbacv1.Role{}, p.ControlRoles...), p.AppRoles...) {
			if r.Namespace == namespace && r.Name == b.RoleRef.Name && matches(r.Rules) {
				return true
			}
		}
	}
	for _, b := range p.ClusterBindings {
		if !subjectMatches(b.Subjects) {
			continue
		}
		for _, r := range p.ClusterRoles {
			if r.Name == b.RoleRef.Name && matches(r.Rules) {
				return true
			}
		}
	}
	return false
}

func TestPermissiveResponderAuthorizer(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		cfg := testConfig()
		cfg.PermissiveResponder = enabled
		if enabled {
			cfg.ResponderNamespaces = []string{"policy-system"}
		}
		p, err := Resources(cfg, discoveryFixture()...)
		if err != nil {
			t.Fatal(err)
		}
		for _, kind := range []string{"mutatingwebhookconfigurations", "validatingwebhookconfigurations"} {
			for _, verb := range []string{"get", "list", "watch", "patch", "create", "update", "delete"} {
				if got := planAllows(p, "", "admissionregistration.k8s.io", kind, verb); got != enabled {
					t.Errorf("enabled=%v webhook %s/%s allowed=%v", enabled, kind, verb, got)
				}
			}
		}
		for _, ns := range []string{"demo", "policy-system"} {
			if got := planAllows(p, ns, "example.dev", "widgets", "deletecollection"); got != enabled {
				t.Errorf("enabled=%v namespace=%s CRD allowed=%v", enabled, ns, got)
			}
			if !planAllows(p, ns, "apps", "deployments", "patch") && enabled {
				t.Errorf("restart denied in %s", ns)
			}
		}
		for _, ns := range []string{"", "demo", "policy-system", "demo-sdo", "unrelated"} {
			for _, resource := range []string{"secrets", "secrets/status", "serviceaccounts/token"} {
				for _, verb := range []string{"get", "list", "watch", "create", "update", "patch", "delete"} {
					if planAllows(p, ns, "", resource, verb) {
						t.Errorf("credential endpoint allowed: %s/%s/%s", ns, resource, verb)
					}
				}
			}
		}
		if planAllows(p, "unrelated", "apps", "deployments", "patch") || planAllows(p, "demo-sdo", "apps", "deployments", "patch") || planAllows(p, "", "", "nodes/proxy", "get") {
			t.Fatal("grant escaped lane")
		}
		for _, verb := range []string{"bind", "escalate", "impersonate"} {
			if planAllows(p, "demo", "", "serviceaccounts", verb) {
				t.Fatalf("ServiceAccount escalation allowed: %s", verb)
			}
			if planAllows(p, "demo", "rbac.authorization.k8s.io", "roles", verb) {
				t.Fatalf("escalation allowed: %s", verb)
			}
		}
	}
}

func TestPermissiveResponderRejectsControlNamespaceAndUnoptedScope(t *testing.T) {
	cfg := testConfig()
	cfg.ResponderNamespaces = []string{"policy-system"}
	if _, err := Resources(cfg); err == nil {
		t.Fatal("foreign namespaces without opt-in")
	}
	cfg.PermissiveResponder = true
	for _, ns := range []string{"demo-sdo", "kube-system", "bad/name"} {
		cfg.ResponderNamespaces = []string{ns}
		if _, err := Resources(cfg, discoveryFixture()...); err == nil {
			t.Fatalf("unsafe namespace accepted: %s", ns)
		}
	}
	cfg.ControlNamespace = "demo"
	cfg.ResponderNamespaces = nil
	if _, err := Resources(cfg, discoveryFixture()...); err == nil || !strings.Contains(err.Error(), "separate") {
		t.Fatalf("permissive mode requires separate control namespace: %v", err)
	}
}

func TestPermissiveGrantsPersistRestoreAndRevoke(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig()
	cfg.PermissiveResponder = true
	cfg.ResponderNamespaces = []string{"policy-system"}
	client := fake.NewSimpleClientset()
	client.Discovery().(*fakediscovery.FakeDiscovery).Resources = discoveryFixture()
	installer := Installer{Client: client}
	plan, err := installer.resources(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := installer.ensurePermissiveNamespaces(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	for _, r := range plan.AppRoles {
		if err := installer.upsertRole(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	for _, b := range plan.AppBindings {
		if err := installer.upsertBinding(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	if err := installer.ensurePermissiveClusterGrants(ctx, plan); err != nil {
		t.Fatal(err)
	}
	name := permissiveGrantName(cfg.controlNamespace())
	if err := client.RbacV1().RoleBindings("demo").Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	inspect := InspectConfig{ControlNamespace: cfg.controlNamespace(), Namespace: cfg.Namespace, Application: cfg.Application, RepositoryPVC: cfg.RepositoryPVC, ControllerImage: cfg.ControllerImage}
	if err := installer.restoreApplicationGrantsWithSettings(ctx, inspect, plan.Job.Spec.Template.Annotations[PermissiveResponderAnnotation]); err != nil {
		t.Fatal(err)
	}
	if _, err := client.RbacV1().RoleBindings("demo").Get(ctx, name, metav1.GetOptions{}); err != nil {
		t.Fatalf("restore permissive binding: %v", err)
	}
	if _, err := client.RbacV1().ClusterRoleBindings().Get(ctx, name, metav1.GetOptions{}); err != nil {
		t.Fatalf("webhook binding absent: %v", err)
	}
	strict, err := Resources(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	if plan.Job.Annotations[InstallFingerprintAnnotation] == strict.Job.Annotations[InstallFingerprintAnnotation] {
		t.Fatal("permission mode absent from install fingerprint")
	}
	cfg.PermissiveResponder = false
	cfg.ResponderNamespaces = nil
	if err := installer.removeStalePermissiveGrants(ctx, cfg, plan.Job); err != nil {
		t.Fatal(err)
	}
	for _, ns := range []string{"demo", "policy-system"} {
		if _, err := client.RbacV1().Roles(ns).Get(ctx, name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
			t.Fatalf("old grant survived in %s: %v", ns, err)
		}
		if _, err := client.RbacV1().RoleBindings(ns).Get(ctx, name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
			t.Fatalf("old binding survived in %s: %v", ns, err)
		}
	}
	if _, err := client.RbacV1().ClusterRoleBindings().Get(ctx, name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("old cluster binding survived: %v", err)
	}
}

func FuzzPermissiveSecretReadsDenied(f *testing.F) {
	f.Add("demo", "get", "secrets")
	f.Add("policy-system", "list", "secrets/status")
	f.Add("demo-sdo", "watch", "secrets")
	f.Fuzz(func(t *testing.T, namespace, verb, resource string) {
		if namespace == "" {
			namespace = "demo"
		}
		// The canary is deliberately present in every Secret-bearing config input.
		cfg := testConfig()
		cfg.Credentials = map[string][]byte{"TOKEN": []byte("PX1_SECRET_CANARY")}
		cfg.RegistryPushDockerConfig = []byte("PX1_SECRET_CANARY")
		cfg.PermissiveResponder = true
		cfg.ResponderNamespaces = []string{"policy-system"}
		p, err := Resources(cfg, discoveryFixture()...)
		if err != nil {
			t.Fatal(err)
		}
		if resource != "secrets" && !strings.HasPrefix(resource, "secrets/") {
			resource = "secrets"
		}
		if planAllows(p, namespace, "", resource, verb) {
			t.Fatalf("Secret access allowed: %q/%q/%q", namespace, resource, verb)
		}
		grants, err := json.Marshal(struct {
			Roles        []*rbacv1.Role
			ClusterRoles []*rbacv1.ClusterRole
		}{p.AppRoles, p.ClusterRoles})
		if err != nil || strings.Contains(string(grants), "PX1_SECRET_CANARY") {
			t.Fatalf("Secret value entered grants: %v", err)
		}
	})
}
