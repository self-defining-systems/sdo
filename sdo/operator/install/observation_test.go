package install

import (
	"encoding/json"
	"testing"

	"sdo.dev/operator/memory/observation"
	"sdo.dev/operator/sdk"
)

func TestObservationPlanGrantsOnlyControllerReads(t *testing.T) {
	for _, split := range []bool{false, true} {
		c := testConfig()
		if !split {
			c.ControlNamespace = c.Namespace
		}
		c.ObservationCeiling = sdk.ObservationScope{Version: 1, Targets: []sdk.ObservationTarget{{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "policy-system"}, {APIVersion: "v1", Kind: "Service", Namespace: "policy-system"}, {APIVersion: "v1", Kind: "Endpoints", Namespace: "policy-system"}, {APIVersion: "admissionregistration.k8s.io/v1", Kind: "MutatingWebhookConfiguration", Name: "mutating"}, {APIVersion: "admissionregistration.k8s.io/v1", Kind: "ValidatingWebhookConfiguration", Name: "validating"}}}
		plan, err := Resources(c)
		if err != nil {
			t.Fatal(err)
		}
		if len(plan.ObservationRoles) != 3 || len(plan.ObservationClusterRoles) != 2 {
			t.Fatalf("missing observation grants: %+v", plan)
		}
		for _, r := range plan.ObservationRoles {
			for _, rule := range r.Rules {
				for _, resource := range rule.Resources {
					if resource == "secrets" {
						t.Fatal("foreign Secret grant")
					}
				}
				for _, verb := range rule.Verbs {
					if verb != "get" && verb != "list" {
						t.Fatal("foreign write grant")
					}
				}
			}
		}
		for _, b := range plan.ObservationBindings {
			if len(b.Subjects) != 2 || b.Subjects[0].Name != "sdo-controller" || b.Subjects[1].Name != "sdo-operate" {
				t.Fatal("responder granted foreign observation")
			}
		}
		for _, r := range plan.ObservationClusterRoles {
			if len(r.Rules) != 1 || len(r.Rules[0].ResourceNames) != 1 || len(r.Rules[0].Verbs) != 1 || r.Rules[0].Verbs[0] != "get" {
				t.Fatalf("cluster observation was not name-scoped: %+v", r)
			}
		}
		for _, b := range plan.ObservationClusterBindings {
			if len(b.Subjects) != 2 || b.Subjects[0].Name != "sdo-controller" || b.Subjects[1].Name != "sdo-operate" {
				t.Fatal("responder granted webhook observation")
			}
		}
		for _, env := range plan.Job.Spec.Template.Spec.Containers[0].Env {
			if env.Name == observation.CeilingEnvironment {
				var roundtrip sdk.ObservationScope
				if err = json.Unmarshal([]byte(env.Value), &roundtrip); err != nil || roundtrip.Identity() != c.ObservationCeiling.Identity() {
					t.Fatal("ceiling transport changed policy")
				}
			}
		}
		c.ResponderEnv = []string{observation.CeilingEnvironment + "={}"}
		if _, err = Resources(c); err == nil {
			t.Fatal("responder policy override accepted")
		}
	}
}
