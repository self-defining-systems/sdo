package runtime

import (
	"strings"
	"testing"
	"time"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"sdo.dev/operator/install"
	"sdo.dev/operator/sdk"
)

func grants(rules []rbacv1.PolicyRule, group, resource, verb string) bool {
	for _, rule := range rules {
		has := func(values []string, want string) bool {
			for _, value := range values {
				if value == want || value == "*" {
					return true
				}
			}
			return false
		}
		if has(rule.APIGroups, group) && has(rule.Resources, resource) && has(rule.Verbs, verb) {
			return true
		}
	}
	return false
}

// Every kind the controller watches must be readable, and every kind adoption
// handles must be patchable, by the controller that runs it, in both install
// layouts. The table is the source inventory's own kind
// table, so a new kind cannot enter adoption without its grant; a kind left
// out of adoption must say why.
func TestAdoptionHandledKindsHaveControllerGrants(t *testing.T) {
	split := install.Config{Namespace: "app", ControlNamespace: "app-sdo", Application: "app", Model: "gpt-fixture", TimeoutSeconds: 60, ControllerImage: "controller:fixture", ResponderImage: "responder:fixture", ValidatorImage: "validator:fixture", RepositoryPVC: "repository", CredentialsSecret: "fixture", AcceptedCommit: strings.Repeat("a", 40), NativeCommand: []string{"/usr/local/bin/sdo-operator"}, BuildKitImage: "moby/buildkit@sha256:" + strings.Repeat("a", 64), RegistryPushSecret: "registry-push", RegistryPushDockerConfig: []byte("{}")}
	single := split
	single.ControlNamespace = ""
	for name, cfg := range map[string]install.Config{"split namespaces": split, "single namespace": single} {
		plan, err := install.Resources(cfg)
		if err != nil {
			t.Fatal(err)
		}
		var controller []rbacv1.PolicyRule
		for _, role := range append(append([]*rbacv1.Role{}, plan.AppRoles...), plan.ControlRoles...) {
			if role.Name == "sdo-controller" && role.Namespace == cfg.Namespace {
				controller = role.Rules
			}
		}
		if len(controller) == 0 {
			t.Fatalf("%s: no controller Role in the application namespace", name)
		}
		for key, kind := range sourceInventoryKinds {
			if !strings.HasPrefix(kind.Resource, "cluster") {
				// The controller watches every namespaced inventory kind.
				for _, verb := range []string{"get", "list", "watch"} {
					if !grants(controller, kind.Group, kind.Resource, verb) {
						t.Errorf("%s: the controller watches %s but its Role lacks %s on %s/%s", name, key, verb, kind.Group, kind.Resource)
					}
				}
			}
			if kind.NotAdopted != "" {
				for _, verb := range []string{"patch", "escalate", "bind"} {
					if grants(controller, kind.Group, kind.Resource, verb) {
						t.Errorf("%s: %s is excluded from adoption (%s) yet the controller holds %s", name, key, kind.NotAdopted, verb)
					}
				}
				continue
			}
			for _, verb := range []string{"get", "patch", "create"} {
				if !grants(controller, kind.Group, kind.Resource, verb) {
					t.Errorf("%s: adoption handles %s but the controller Role lacks %s on %s/%s", name, key, verb, kind.Group, kind.Resource)
				}
			}
		}
	}
}

// Every informer the controller starts must have list and watch grants in
// both install layouts: a Role in the application namespace for namespaced
// informers, a ClusterRole bound to the controller for cluster-wide ones. An
// informer without them never syncs, and the controller is blind. The set of
// informers comes from the cache itself, with every supported kind watched.
func TestControllerInformersHaveListWatchGrantsInBothLayouts(t *testing.T) {
	watches := []sdk.WatchKind{}
	lists := map[string]*metav1.APIResourceList{}
	for key, resource := range informerResources {
		slash := strings.LastIndex(key, "/")
		apiVersion, kind := key[:slash], key[slash+1:]
		watches = append(watches, sdk.WatchKind{APIVersion: apiVersion, Kind: kind})
		list := lists[apiVersion]
		if list == nil {
			list = &metav1.APIResourceList{GroupVersion: apiVersion}
			lists[apiVersion] = list
		}
		list.APIResources = append(list.APIResources, metav1.APIResource{
			Name: resource.Resource, Kind: kind, Namespaced: !inventoryClusterKind(kind), Verbs: metav1.Verbs{"get", "list", "watch"},
		})
	}
	discovery := []*metav1.APIResourceList{}
	for _, list := range lists {
		discovery = append(discovery, list)
	}
	cache, err := NewKubernetesCache(KubernetesCacheConfig{Namespace: "app", Client: fake.NewSimpleClientset(), TrackDrift: true},
		[]sdk.Detector{scheduledDetector{spec: sdk.DetectorSpec{ID: "all", Interval: time.Second, Watches: watches}}})
	if err != nil {
		t.Fatal(err)
	}
	required := cache.RequiredGrants()
	if len(required) != len(informerResources) {
		t.Fatalf("started %d informers, want one per supported kind (%d)", len(required), len(informerResources))
	}
	split := install.Config{Namespace: "app", ControlNamespace: "app-sdo", Application: "app", Model: "gpt-fixture", TimeoutSeconds: 60, ControllerImage: "controller:fixture", ResponderImage: "responder:fixture", ValidatorImage: "validator:fixture", RepositoryPVC: "repository", CredentialsSecret: "fixture", AcceptedCommit: strings.Repeat("a", 40), NativeCommand: []string{"/usr/local/bin/sdo-operator"}, BuildKitImage: "moby/buildkit@sha256:" + strings.Repeat("a", 64), RegistryPushSecret: "registry-push", RegistryPushDockerConfig: []byte("{}")}
	single := split
	single.ControlNamespace = ""
	for name, cfg := range map[string]install.Config{"split namespaces": split, "single namespace": single} {
		plan, err := install.Resources(cfg)
		if err != nil {
			t.Fatal(err)
		}
		var namespaced, clusterWide []rbacv1.PolicyRule
		for _, role := range append(append([]*rbacv1.Role{}, plan.AppRoles...), plan.ControlRoles...) {
			if role.Name == "sdo-controller" && role.Namespace == cfg.Namespace {
				namespaced = role.Rules
			}
		}
		for _, role := range plan.ClusterRoles {
			for _, binding := range plan.ClusterBindings {
				if binding.RoleRef.Name != role.Name {
					continue
				}
				for _, subject := range binding.Subjects {
					if subject.Kind == "ServiceAccount" && subject.Name == "sdo-controller" {
						clusterWide = append(clusterWide, role.Rules...)
					}
				}
			}
		}
		for _, grant := range required {
			rules, scope := namespaced, "its Role in the application namespace"
			if grant.ClusterWide {
				rules, scope = clusterWide, "a ClusterRole bound to sdo-controller"
			}
			for _, verb := range []string{"list", "watch"} {
				if !grants(rules, grant.Group, grant.Resource, verb) {
					t.Errorf("%s: the controller starts an informer for %s but %s lacks %s on %s/%s", name, grant.Key, scope, verb, grant.Group, grant.Resource)
				}
			}
		}
	}
}
