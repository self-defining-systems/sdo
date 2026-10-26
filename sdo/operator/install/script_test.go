package install

import (
	"testing"
)

func TestScriptJobsHaveApplicationLocalRepairIdentity(t *testing.T) {
	for _, split := range []bool{false, true} {
		c := testConfig()
		if !split {
			c.ControlNamespace = c.Namespace
		}
		plan, err := Resources(c)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, sa := range plan.ServiceAccounts {
			if sa.Name == "sdo-script" && sa.Namespace == c.Namespace {
				found = true
			}
		}
		if !found {
			t.Fatal("no app-local script identity")
		}
		roles := append(plan.AppRoles, plan.ControlRoles...)
		bindings := append(plan.AppBindings, plan.ControlBindings...)
		allowed := func(resource, verb string) bool {
			for _, binding := range bindings {
				for _, subject := range binding.Subjects {
					if subject.Name != "sdo-script" || subject.Namespace != c.Namespace {
						continue
					}
					for _, role := range roles {
						if binding.Namespace == role.Namespace && binding.RoleRef.Name == role.Name {
							for _, rule := range role.Rules {
								if (contains(rule.Resources, resource) || contains(rule.Resources, "*")) && (contains(rule.Verbs, verb) || contains(rule.Verbs, "*")) {
									return true
								}
							}
						}
					}
				}
			}
			return false
		}
		for _, pair := range [][2]string{{"secrets", "get"}, {"pods/exec", "create"}, {"deployments", "patch"}, {"configmaps", "update"}, {"jobs", "get"}} {
			if !allowed(pair[0], pair[1]) {
				t.Fatalf("split=%v missing %s %s", split, pair[1], pair[0])
			}
		}

	}
}
