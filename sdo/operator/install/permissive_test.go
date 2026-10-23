package install

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	fakediscovery "k8s.io/client-go/discovery/fake"
	"k8s.io/client-go/kubernetes/fake"
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
				if got := planAllows(p, "", "admissionregistration.k8s.io", kind, verb); got != (enabled && verb != "patch" && verb != "create" && verb != "update") {
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
		if resource != "secrets" && !strings.HasPrefix(resource, "secrets/") {
			resource = "secrets"
		}
		// The canary is deliberately present in every Secret-bearing config input.
		cfg := testConfig()
		cfg.Credentials = map[string][]byte{"TOKEN": []byte("PX1_SECRET_CANARY")}
		cfg.RegistryPushDockerConfig = []byte("PX1_SECRET_CANARY")
		cfg.PermissiveResponder = true
		cfg.ResponderNamespaces = []string{"policy-system"}
		discovered := append(discoveryFixture(), &metav1.APIResourceList{GroupVersion: "v1", APIResources: []metav1.APIResource{{Name: resource, Namespaced: true}}})
		p, err := Resources(cfg, discovered...)
		if err != nil {
			t.Fatal(err)
		}
		if planAllows(p, namespace, "", resource, verb) {
			t.Fatalf("Secret access allowed: %q/%q/%q", namespace, resource, verb)
		}
		grants, err := json.Marshal(struct {
			Roles        []*rbacv1.Role
			ClusterRoles []*rbacv1.ClusterRole
		}{append(append([]*rbacv1.Role{}, p.ControlRoles...), p.AppRoles...), p.ClusterRoles})
		if err != nil || strings.Contains(string(grants), "PX1_SECRET_CANARY") {
			t.Fatalf("Secret value entered grants: %v", err)
		}
	})
}

// Secret-path checks use the complete effective grant graph, including the
// baseline Role: excluding a resource only from discovery is insufficient.
func TestPermissiveIndirectSecretPathsDenied(t *testing.T) {
	cfg := testConfig()
	cfg.PermissiveResponder = true
	cfg.ResponderNamespaces = []string{"policy-system"}
	discovered := discoveryFixture()
	discovered = append(discovered,
		&metav1.APIResourceList{GroupVersion: "v1", APIResources: []metav1.APIResource{
			{Name: "pods/log", Namespaced: true}, {Name: "pods/proxy", Namespaced: true},
			{Name: "services/proxy", Namespaced: true}, {Name: "nodes/proxy", Namespaced: true},
			{Name: "users", Namespaced: true}, {Name: "groups", Namespaced: true},
			{Name: "pods/attach", Namespaced: true}, {Name: "pods/portforward", Namespaced: true},
			{Name: "pods/ephemeralcontainers", Namespaced: true}, {Name: "replicationcontrollers", Namespaced: true},
			{Name: "persistentvolumeclaims", Namespaced: true},
		}},
		&metav1.APIResourceList{GroupVersion: "apps/v1", APIResources: []metav1.APIResource{
			{Name: "statefulsets", Namespaced: true}, {Name: "daemonsets", Namespaced: true},
			{Name: "replicasets", Namespaced: true}, {Name: "deployments/rollback", Namespaced: true},
		}},
		&metav1.APIResourceList{GroupVersion: "batch/v1", APIResources: []metav1.APIResource{
			{Name: "jobs", Namespaced: true}, {Name: "cronjobs", Namespaced: true},
		}},
		&metav1.APIResourceList{GroupVersion: "certificates.k8s.io/v1", APIResources: []metav1.APIResource{
			{Name: "certificatesigningrequests", Namespaced: true}, {Name: "certificatesigningrequests/approval", Namespaced: true}, {Name: "signers", Namespaced: true},
		}},
		&metav1.APIResourceList{GroupVersion: "external-secrets.io/v1", APIResources: []metav1.APIResource{
			{Name: "externalsecrets", Namespaced: true}, {Name: "externalsecrets/status", Namespaced: true},
		}},
	)
	plan, err := Resources(cfg, discovered...)
	if err != nil {
		t.Fatal(err)
	}
	allVerbs := []string{"get", "list", "watch", "create", "update", "patch", "delete", "deletecollection", "bind", "escalate", "impersonate", "approve", "sign"}
	paths := []struct {
		name, group, resource string
		verbs                 []string
	}{
		{"webhook", "admissionregistration.k8s.io", "mutatingwebhookconfigurations", []string{"create", "update", "patch"}},
		{"webhook", "admissionregistration.k8s.io", "validatingwebhookconfigurations", []string{"create", "update", "patch"}},
		{"exec", "", "pods/exec", allVerbs},
		{"attach", "", "pods/attach", allVerbs},
		{"portforward", "", "pods/portforward", allVerbs},
		{"ephemeral", "", "pods/ephemeralcontainers", allVerbs},
		{"serviceaccount", "", "serviceaccounts", []string{"bind", "escalate", "impersonate"}},
		{"identity", "", "users", []string{"impersonate"}},
		{"identity", "", "groups", []string{"impersonate"}},
		{"token request", "", "serviceaccounts/token", allVerbs},
		{"CSR", "certificates.k8s.io", "certificatesigningrequests", allVerbs},
		{"CSR approval", "certificates.k8s.io", "certificatesigningrequests/approval", allVerbs},
		{"signer", "certificates.k8s.io", "signers", allVerbs},
		{"kubelet proxy", "", "nodes/proxy", allVerbs},
		{"RBAC", "rbac.authorization.k8s.io", "roles", []string{"bind", "escalate", "impersonate"}},
		{"RBAC", "rbac.authorization.k8s.io", "rolebindings", []string{"bind", "escalate", "impersonate"}},
		{"Secret", "", "secrets", []string{"get", "list", "watch", "create", "update", "patch", "delete", "deletecollection"}},
		{"Secret subresource", "", "secrets/status", []string{"get", "list", "watch", "create", "update", "patch", "delete", "deletecollection"}},
	}
	for _, path := range paths {
		t.Run(path.name+"/"+path.resource, func(t *testing.T) {
			for _, ns := range []string{"", "demo", "policy-system", "demo-sdo", "kube-system", "unrelated"} {
				// The app Role already grants streaming access. PX1c excludes
				// only new discovery grants, preserving that baseline literally.
				if ns == "demo" && (path.resource == "pods/exec" || path.resource == "pods/attach" || path.resource == "pods/portforward") {
					continue
				}
				for _, verb := range path.verbs {
					if planAllows(plan, ns, path.group, path.resource, verb) {
						t.Errorf("Secret path allowed: namespace=%s group=%s resource=%s verb=%s", ns, path.group, path.resource, verb)
					}
				}
			}
		})
	}
}

func TestPermissiveSecretPathBaseline(t *testing.T) {
	strict, err := Resources(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []struct{ group, resource, verb string }{
		{"", "pods", "create"}, {"batch", "jobs", "create"},
		{"apps", "deployments", "patch"}, {"", "pods/exec", "create"},
		{"", "pods/attach", "create"}, {"", "pods/portforward", "create"},
		{"", "pods/log", "get"}, {"", "configmaps", "get"},
	} {
		if !planAllows(strict, "demo", path.group, path.resource, path.verb) {
			t.Errorf("pre-PX1 baseline changed: %+v", path)
		}
		if planAllows(strict, "policy-system", path.group, path.resource, path.verb) {
			t.Errorf("foreign grant incorrectly classified as pre-existing: %+v", path)
		}
	}
}

func TestPermissiveSecretPathsRevokedOnRestore(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig()
	cfg.PermissiveResponder = true
	cfg.ResponderNamespaces = []string{"policy-system"}
	want, err := Resources(cfg, discoveryFixture()...)
	if err != nil {
		t.Fatal(err)
	}
	client := fake.NewSimpleClientset()
	client.Discovery().(*fakediscovery.FakeDiscovery).Resources = discoveryFixture()
	installer := Installer{Client: client}
	// Simulate previously installed permissive wildcard grants and baseline
	// streaming privileges. Restoration must replace, rather than merge, them.
	for _, r := range append(want.ControlRoles, want.AppRoles...) {
		old := r.DeepCopy()
		if old.Name == "sdo-responder" || old.Name == permissiveGrantName(cfg.controlNamespace()) {
			old.Rules = []rbacv1.PolicyRule{rule("*", []string{"*"}, "*")}
		}
		if err := installer.upsertRole(ctx, old); err != nil {
			t.Fatal(err)
		}
	}
	for _, b := range append(want.ControlBindings, want.AppBindings...) {
		if err := installer.upsertBinding(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	old := want
	old.ClusterRoles = []*rbacv1.ClusterRole{want.ClusterRoles[0].DeepCopy()}
	old.ClusterRoles[0].Rules = []rbacv1.PolicyRule{rule("*", []string{"*"}, "*")}
	if err := installer.ensurePermissiveClusterGrants(ctx, old); err != nil {
		t.Fatal(err)
	}
	// The control-namespace Role is unchanged by PX1c. Restore its real baseline;
	// inspect-existing intentionally only reconciles application/foreign grants.
	for _, r := range want.ControlRoles {
		if err := installer.upsertRole(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	inspect := InspectConfig{ControlNamespace: cfg.controlNamespace(), Namespace: cfg.Namespace, Application: cfg.Application, RepositoryPVC: cfg.RepositoryPVC, ControllerImage: cfg.ControllerImage}
	if err := installer.restoreApplicationGrantsWithSettings(ctx, inspect, want.Job.Spec.Template.Annotations[PermissiveResponderAnnotation]); err != nil {
		t.Fatal(err)
	}
	got := want
	got.AppRoles = nil
	for _, r := range want.AppRoles {
		actual, err := client.RbacV1().Roles(r.Namespace).Get(ctx, r.Name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		got.AppRoles = append(got.AppRoles, actual)
	}
	got.ClusterRoles = nil
	for _, r := range want.ClusterRoles {
		actual, err := client.RbacV1().ClusterRoles().Get(ctx, r.Name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		got.ClusterRoles = append(got.ClusterRoles, actual)
	}
	for _, ns := range []string{"demo", "policy-system", "kube-system", "demo-sdo"} {
		for _, path := range []struct{ group, resource, verb string }{
			{"", "secrets", "get"}, {"", "pods/exec", "create"},
			{"", "pods/attach", "create"}, {"", "pods/portforward", "create"},
			{"", "serviceaccounts/token", "create"},
			{"admissionregistration.k8s.io", "mutatingwebhookconfigurations", "patch"},
		} {
			if ns == "demo" && (path.resource == "pods/exec" || path.resource == "pods/attach" || path.resource == "pods/portforward") {
				continue
			}
			if planAllows(got, ns, path.group, path.resource, path.verb) {
				t.Errorf("stale permission survived in %s: %+v", ns, path)
			}
		}
	}
}

func TestPermissiveBroadRepairsRetained(t *testing.T) {
	cfg := testConfig()
	cfg.PermissiveResponder = true
	cfg.ResponderNamespaces = []string{"policy-system"}
	discovered := append(discoveryFixture(), &metav1.APIResourceList{GroupVersion: "external-secrets.io/v1", APIResources: []metav1.APIResource{
		{Name: "externalsecrets", Namespaced: true}, {Name: "externalsecrets/status", Namespaced: true},
	}}, &metav1.APIResourceList{GroupVersion: "batch/v1", APIResources: []metav1.APIResource{{Name: "jobs", Namespaced: true}, {Name: "cronjobs", Namespaced: true}}})
	discovered = append(discovered, &metav1.APIResourceList{GroupVersion: "v1", APIResources: []metav1.APIResource{
		{Name: "pods/log", Namespaced: true}, {Name: "pods/proxy", Namespaced: true}, {Name: "services/proxy", Namespaced: true},
	}})
	plan, err := Resources(cfg, discovered...)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []struct{ group, resource string }{
		{"", "pods"}, {"", "configmaps"}, {"", "serviceaccounts"},
		{"", "pods/log"}, {"", "pods/proxy"}, {"", "services/proxy"},
		{"apps", "deployments"}, {"batch", "jobs"}, {"batch", "cronjobs"},
		{"rbac.authorization.k8s.io", "roles"}, {"rbac.authorization.k8s.io", "rolebindings"},
		{"example.dev", "widgets"}, {"external-secrets.io", "externalsecrets"}, {"external-secrets.io", "externalsecrets/status"},
	} {
		for _, verb := range []string{"get", "list", "watch", "create", "update", "patch", "delete", "deletecollection"} {
			for _, ns := range []string{"demo", "policy-system"} {
				if !planAllows(plan, ns, path.group, path.resource, verb) {
					t.Errorf("broad repair denied in %s: %+v/%s", ns, path, verb)
				}
			}
			for _, ns := range []string{"demo-sdo", "kube-system", "unrelated"} {
				if ns == "demo-sdo" && path.group == "" && path.resource == "configmaps" {
					continue // Existing control Role grants ConfigMap access.
				}
				if planAllows(plan, ns, path.group, path.resource, verb) {
					t.Errorf("broad repair escaped scope in %s: %+v/%s", ns, path, verb)
				}
			}
		}
	}
	strict, err := Resources(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	for _, baseline := range append(strict.ControlRoles, strict.AppRoles...) {
		for _, actual := range append(plan.ControlRoles, plan.AppRoles...) {
			if actual.Name == baseline.Name && actual.Namespace == baseline.Namespace && !reflect.DeepEqual(actual, baseline) {
				t.Errorf("baseline Role changed: %s/%s", baseline.Namespace, baseline.Name)
			}
		}
	}
}
