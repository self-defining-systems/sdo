package install

import (
	"context"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	fakediscovery "k8s.io/client-go/discovery/fake"
	"k8s.io/client-go/kubernetes/fake"
	"testing"
)

func TestPermissiveBaselineReachAndAddedGrantControlExclusion(t *testing.T) {
	cfg := testConfig()
	cfg.PermissiveResponder = true
	cfg.ResponderNamespaces = []string{"policy-system"}
	client := fake.NewSimpleClientset(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "unrelated"}}, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "demo-sdo"}}, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "kube-system"}})
	discovered := secretPathDiscovery()
	discovered = append(discovered, &metav1.APIResourceList{GroupVersion: "v1", APIResources: []metav1.APIResource{{Name: "nodes"}, {Name: "namespaces"}}})
	client.Discovery().(*fakediscovery.FakeDiscovery).Resources = discovered
	p, err := (Installer{Client: client}).resources(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, ns := range []string{"demo", "policy-system", "unrelated", "kube-system"} {
		for _, r := range []string{"pods", "configmaps", "services", "pods/log"} {
			if !planAllows(p, ns, "", r, "get") {
				t.Errorf("baseline read denied: %s/%s", ns, r)
			}
		}
	}
	for _, r := range []string{"nodes", "namespaces"} {
		if !planAllows(p, "", "", r, "list") {
			t.Errorf("cluster read denied: %s", r)
		}
	}
	for _, ns := range []string{"demo", "policy-system"} {
		for _, verb := range []string{"get", "create", "update", "patch", "delete"} {
			if !planAllows(p, ns, "", "configmaps", verb) {
				t.Errorf("repair denied: %s/configmaps/%s", ns, verb)
			}
		}
		if !planAllows(p, ns, "", "pods/exec", "create") {
			t.Errorf("baseline exec denied: %s", ns)
		}
	}
	for _, ns := range []string{"unrelated", "kube-system"} {
		for _, verb := range []string{"create", "update", "patch", "delete"} {
			if planAllows(p, ns, "", "configmaps", verb) {
				t.Errorf("repair escaped: %s/configmaps/%s", ns, verb)
			}
		}
	}
	// The pre-existing transport Role is a separate dependency, never broadened.
	for _, b := range p.AppBindings {
		if b.Namespace == "demo-sdo" {
			t.Errorf("added control binding: %s", b.Name)
		}
	}
	for _, ns := range []string{"", "demo", "policy-system", "unrelated", "kube-system", "demo-sdo"} {
		for _, r := range []string{"secrets", "serviceaccounts", "serviceaccounts/token", "nodes/proxy", "pods/proxy", "services/proxy"} {
			for _, verb := range []string{"get", "list", "watch", "create", "patch", "delete", "impersonate"} {
				if planAllows(p, ns, "", r, verb) {
					t.Errorf("credential/auth API allowed: %s/%s/%s", ns, r, verb)
				}
			}
		}
		for _, r := range []string{"roles", "rolebindings", "clusterroles", "clusterrolebindings"} {
			if planAllows(p, ns, "rbac.authorization.k8s.io", r, "create") {
				t.Errorf("RBAC escalation allowed: %s/%s", ns, r)
			}
		}
	}
}

func TestPermissiveReadBindingsRestoreInventoryAndRevokeOnlyOwner(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig()
	cfg.PermissiveResponder = true
	cfg.ResponderNamespaces = []string{"policy-system"}
	client := fake.NewSimpleClientset(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "unrelated"}}, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "demo-sdo"}})
	client.Discovery().(*fakediscovery.FakeDiscovery).Resources = secretPathDiscovery()
	installer := Installer{Client: client}
	p, err := installer.resources(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range p.AppBindings {
		if err := installer.upsertBinding(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	other := p.AppBindings[len(p.AppBindings)-1].DeepCopy()
	other.Name = "other-owner"
	other.Labels = map[string]string{permissiveReadOwnerLabel: "other-sdo"}
	if err := installer.upsertBinding(ctx, other); err != nil {
		t.Fatal(err)
	}
	// A namespace appearing after the initial install is granted on inspection.
	if _, err := client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "new-observed"}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	inspect := InspectConfig{Namespace: cfg.Namespace, ControlNamespace: cfg.controlNamespace(), Application: cfg.Application, RepositoryPVC: cfg.RepositoryPVC, ControllerImage: cfg.ControllerImage}
	if err := installer.restoreApplicationGrantsWithSettings(ctx, inspect, p.Job.Spec.Template.Annotations[PermissiveResponderAnnotation]); err != nil {
		t.Fatal(err)
	}
	if _, err := client.RbacV1().RoleBindings("new-observed").Get(ctx, permissiveReadGrantName(cfg.controlNamespace()), metav1.GetOptions{}); err != nil {
		t.Fatal(err)
	}
	cfg.PermissiveResponder = false
	cfg.ResponderNamespaces = nil
	if err := installer.removeStalePermissiveGrants(ctx, cfg, p.Job); err != nil {
		t.Fatal(err)
	}
	got, err := client.RbacV1().RoleBindings(metav1.NamespaceAll).List(ctx, metav1.ListOptions{LabelSelector: permissiveReadOwnerLabel + "=demo-sdo"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Items) != 0 {
		t.Fatalf("read bindings survived opt-out: %d", len(got.Items))
	}
	if _, err := client.RbacV1().RoleBindings(other.Namespace).Get(ctx, other.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("other owner's binding changed: %v", err)
	}
}

func FuzzPermissiveDiscoveryExcludesCredentialAPIs(f *testing.F) {
	for _, seed := range []string{"secrets", "secrets/status", "serviceaccounts/token", "nodes/proxy", "pods/proxy", "services/proxy", "users", "groups", "roles", "clusterrolebindings", "credentials"} {
		f.Add(seed, true)
		f.Add(seed, false)
	}
	f.Fuzz(func(t *testing.T, resource string, namespaced bool) {
		if len(resource) > 100 {
			return
		}
		cfg := testConfig()
		cfg.PermissiveResponder = true
		cfg.ResponderNamespaces = []string{"policy-system"}
		discovered := secretPathDiscovery()
		for _, group := range []string{"v1", "rbac.authorization.k8s.io/v1", "authentication.k8s.io/v1", "authorization.k8s.io/v1", "certificates.k8s.io/v1", "external-secrets.io/v1", "unreviewed.example/v1"} {
			discovered = append(discovered, &metav1.APIResourceList{GroupVersion: group, APIResources: []metav1.APIResource{{Name: resource, Namespaced: namespaced}}})
		}
		p, err := Resources(cfg, discovered...)
		if err != nil {
			t.Fatal(err)
		}
		for _, ns := range []string{"", "demo", "policy-system", "demo-sdo", "unrelated"} {
			for _, r := range []string{"secrets", "secrets/status", "serviceaccounts", "serviceaccounts/token", "nodes/proxy", "pods/proxy", "services/proxy"} {
				for _, v := range []string{"get", "list", "watch", "create", "update", "patch", "delete", "impersonate"} {
					if planAllows(p, ns, "", r, v) {
						t.Fatalf("credential canary granted: %s/%s/%s", ns, r, v)
					}
				}
			}
			for _, g := range []string{"rbac.authorization.k8s.io", "authentication.k8s.io", "authorization.k8s.io", "certificates.k8s.io", "external-secrets.io", "unreviewed.example"} {
				for _, v := range []string{"get", "create", "bind", "escalate", "impersonate"} {
					if planAllows(p, ns, g, resource, v) {
						t.Fatalf("unreviewed/auth canary granted: %s/%s/%s", g, resource, v)
					}
				}
			}
		}
	})
}
