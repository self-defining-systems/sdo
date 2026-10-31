package install

import (
	rbacv1 "k8s.io/api/rbac/v1"
	"testing"
)

func TestSnapshotRBACCoversAllKindsAndReadOnlyNodes(t *testing.T) {
	for _, split := range []bool{false, true} {
		cfg := testConfig()
		if !split {
			cfg.ControlNamespace = cfg.Namespace
		}
		plan, err := Resources(cfg)
		if err != nil {
			t.Fatal(err)
		}
		var rules []rbacv1.PolicyRule
		for _, role := range append(plan.ControlRoles, plan.AppRoles...) {
			if role.Name == "sdo-controller" && role.Namespace == cfg.Namespace {
				rules = role.Rules
			}
		}
		for _, resource := range []string{"statefulsets", "daemonsets", "jobs", "cronjobs", "persistentvolumeclaims", "serviceaccounts", "horizontalpodautoscalers", "poddisruptionbudgets", "ingresses", "secrets"} {
			for _, verb := range []string{"list", "watch"} {
				found := false
				for _, rule := range rules {
					found = found || contains(rule.Resources, resource) && contains(rule.Verbs, verb)
				}
				if !found {
					t.Fatalf("split=%v missing %s %s", split, verb, resource)
				}
			}
		}
		if len(plan.ClusterRoles) != 1 || len(plan.ClusterBindings) != 1 {
			t.Fatal("missing cluster node reader")
		}
		for _, rule := range plan.ClusterRoles[0].Rules {
			if !contains(rule.Resources, "nodes") {
				t.Fatal("not nodes")
			}
			for _, verb := range rule.Verbs {
				if verb != "get" && verb != "list" && verb != "watch" {
					t.Fatal("node writes permitted")
				}
			}
		}
		for _, subject := range plan.ClusterBindings[0].Subjects {
			if subject.Namespace != cfg.controlNamespace() {
				t.Fatal("node role bound to wrong namespace")
			}
		}
	}
}
