package install

import (
	"maps"
	"slices"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// builtinNamespacedKinds is the discovery a real cluster offers for the kinds the
// permissive responder reviews. Kinds outside the reviewed set (secrets, RBAC,
// custom resources) are listed too, so the test proves they stay ungranted.
func builtinNamespacedKinds() []*metav1.APIResourceList {
	namespaced := func(names ...string) []metav1.APIResource {
		out := []metav1.APIResource{}
		for _, n := range names {
			out = append(out, metav1.APIResource{Name: n, Namespaced: true})
		}
		return out
	}
	return []*metav1.APIResourceList{
		{GroupVersion: "v1", APIResources: namespaced("pods", "pods/log", "pods/exec", "pods/attach", "pods/portforward", "services", "endpoints", "events", "configmaps", "persistentvolumeclaims", "resourcequotas", "limitranges", "replicationcontrollers", "secrets", "serviceaccounts", "serviceaccounts/token")},
		{GroupVersion: "apps/v1", APIResources: namespaced("deployments", "replicasets", "statefulsets", "daemonsets", "controllerrevisions", "deployments/scale", "replicasets/scale", "statefulsets/scale")},
		{GroupVersion: "batch/v1", APIResources: namespaced("jobs", "cronjobs")},
		{GroupVersion: "autoscaling/v2", APIResources: namespaced("horizontalpodautoscalers")},
		{GroupVersion: "policy/v1", APIResources: namespaced("poddisruptionbudgets")},
		{GroupVersion: "discovery.k8s.io/v1", APIResources: namespaced("endpointslices")},
		{GroupVersion: "networking.k8s.io/v1", APIResources: namespaced("networkpolicies", "ingresses")},
		{GroupVersion: "events.k8s.io/v1", APIResources: namespaced("events")},
		{GroupVersion: "coordination.k8s.io/v1", APIResources: namespaced("leases")},
		{GroupVersion: "storage.k8s.io/v1", APIResources: namespaced("csistoragecapacities")},
		{GroupVersion: "rbac.authorization.k8s.io/v1", APIResources: namespaced("roles", "rolebindings")},
		{GroupVersion: "example.dev/v1", APIResources: namespaced("widgets")},
	}
}

// permissiveReadOnlyWant is the exact list of namespaced kinds the permissive
// responder can read but not write. The reason for each lives on its
// permissiveKinds entry; a new readable kind needs a write decision there.
var permissiveReadOnlyWant = []string{
	"/events", "/pods/log", "apps/controllerrevisions", "events.k8s.io/events", "storage.k8s.io/csistoragecapacities",
}

func TestPermissiveKindsDecideWriteOrNameAReason(t *testing.T) {
	seen := map[string]bool{}
	readOnly := []string{}
	for _, k := range permissiveKinds {
		key := k.Group + "/" + k.Resource
		if seen[key] {
			t.Errorf("duplicate kind %s", key)
		}
		seen[key] = true
		if (len(k.Verbs) == 0) == (k.ReadOnly == "") {
			t.Errorf("%s must either grant write verbs or name why it is read-only", key)
		}
		if k.ReadOnly != "" {
			readOnly = append(readOnly, key)
		}
	}
	slices.Sort(readOnly)
	if !slices.Equal(readOnly, permissiveReadOnlyWant) {
		t.Errorf("read-only kinds = %v, want %v", readOnly, permissiveReadOnlyWant)
	}
}

func responderAppRole(t *testing.T, p Plan, namespace string) *rbacv1.Role {
	t.Helper()
	for _, r := range p.AppRoles {
		if r.Namespace == namespace && r.Name == permissiveGrantName("demo-sdo") {
			return r
		}
	}
	t.Fatalf("no permissive responder Role in %s", namespace)
	return nil
}

func TestPermissiveResponderWritesEveryReadableKindExceptDenyList(t *testing.T) {
	cfg := testConfig()
	cfg.PermissiveResponder = true
	cfg.ResponderNamespaces = []string{"policy-system"}
	plan, err := Resources(cfg, builtinNamespacedKinds()...)
	if err != nil {
		t.Fatal(err)
	}
	writeVerbs := []string{"create", "update", "patch", "delete"}
	for _, ns := range []string{"demo", "policy-system"} {
		role := responderAppRole(t, plan, ns)
		readOnly := map[string]bool{}
		for _, rule := range role.Rules {
			for _, group := range rule.APIGroups {
				for _, resource := range rule.Resources {
					readable := slices.Contains(rule.Verbs, "get") && slices.Contains(rule.Verbs, "list") && slices.Contains(rule.Verbs, "watch")
					writable := slices.ContainsFunc(writeVerbs, func(v string) bool { return slices.Contains(rule.Verbs, v) })
					if readable && !writable {
						readOnly[group+"/"+resource] = true
					}
				}
			}
		}
		// Subresources the responder exercises (exec, scale) and the read-only
		// log stream are decided by name; the ratchet is about whole kinds.
		got := slices.Sorted(maps.Keys(readOnly))
		want := permissiveReadOnlyWant
		if !slices.Equal(got, want) {
			t.Errorf("%s: readable-but-not-writable kinds = %v, want exactly the deny list %v", ns, got, want)
		}
	}
}

func TestPermissiveResponderWritesQuotasAndLimitRanges(t *testing.T) {
	cfg := testConfig()
	cfg.PermissiveResponder = true
	plan, err := Resources(cfg, builtinNamespacedKinds()...)
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []struct{ group, resource string }{
		{"", "resourcequotas"}, {"", "limitranges"}, {"", "endpoints"},
		{"discovery.k8s.io", "endpointslices"}, {"coordination.k8s.io", "leases"},
	} {
		for _, verb := range []string{"get", "list", "watch", "create", "update", "patch", "delete"} {
			if !planAllows(plan, "demo", kind.group, kind.resource, verb) {
				t.Errorf("responder cannot %s %s/%s in the app namespace", verb, kind.group, kind.resource)
			}
		}
	}
	for _, denied := range []struct{ group, resource string }{
		{"", "events"}, {"events.k8s.io", "events"}, {"apps", "controllerrevisions"}, {"", "pods/log"},
		{"", "secrets"}, {"", "serviceaccounts"}, {"rbac.authorization.k8s.io", "roles"}, {"rbac.authorization.k8s.io", "rolebindings"}, {"example.dev", "widgets"},
	} {
		for _, verb := range []string{"create", "update", "patch", "delete"} {
			if planAllows(plan, "demo", denied.group, denied.resource, verb) {
				t.Errorf("responder can %s %s/%s", verb, denied.group, denied.resource)
			}
		}
	}
}

func TestPermissiveResponderEditsOnlyAdmittedNamespaces(t *testing.T) {
	cfg := testConfig()
	cfg.PermissiveResponder = true
	cfg.ResponderNamespaces = []string{"policy-system"}
	plan, err := Resources(cfg, builtinNamespacedKinds()...)
	if err != nil {
		t.Fatal(err)
	}
	var edits []rbacv1.PolicyRule
	for _, r := range plan.ClusterRoles {
		if r.Name != permissiveGrantName("demo-sdo") {
			continue
		}
		for _, rule := range r.Rules {
			if slices.Contains(rule.Resources, "namespaces") && slices.Contains(rule.Verbs, "patch") {
				edits = append(edits, rule)
			}
		}
	}
	if len(edits) != 1 {
		t.Fatalf("want one namespace edit rule, got %v", edits)
	}
	if !slices.Equal(edits[0].ResourceNames, []string{"demo", "policy-system"}) || !slices.Equal(edits[0].Verbs, []string{"update", "patch"}) {
		t.Errorf("namespace edits must be update/patch on the admitted namespaces by name, got %+v", edits[0])
	}
}
