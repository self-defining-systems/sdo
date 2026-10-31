package install

import "testing"

func TestUsageProxyGrants(t *testing.T) {
	for _, split := range []bool{false, true} {
		cfg := testConfig()
		cfg.DisableObservability = true // This test isolates the controller node-reader grant.
		if !split {
			cfg.ControlNamespace = cfg.Namespace
		}
		plan, err := Resources(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if len(plan.ClusterRoles) != 1 || len(plan.ClusterBindings) != 1 {
			t.Fatal("missing node proxy cluster grants")
		}
		nodes, proxy := false, false
		for _, rule := range plan.ClusterRoles[0].Rules {
			for _, verb := range rule.Verbs {
				if verb != "get" && verb != "list" && verb != "watch" {
					t.Fatalf("unexpected permission %s", verb)
				}
				for _, res := range rule.Resources {
					nodes = nodes || res == "nodes" && verb == "list"
					proxy = proxy || res == "nodes/proxy" && verb == "get"
				}
			}
		}
		if !nodes || !proxy {
			t.Fatal("missing required node permissions")
		}
		for _, subject := range plan.ClusterBindings[0].Subjects {
			if subject.Namespace != cfg.controlNamespace() {
				t.Fatal("wrong service account namespace")
			}
		}
	}
}
