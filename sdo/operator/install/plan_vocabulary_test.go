package install

import "testing"

func TestProgramVocabularyGrantsExcludeSecretData(t *testing.T) {
	required := map[string]bool{"/serviceaccounts/get": false, "autoscaling/horizontalpodautoscalers/patch": false, "policy/poddisruptionbudgets/patch": false}
	for _, rule := range programApplyRules() {
		for _, group := range rule.APIGroups {
			for _, resource := range rule.Resources {
				for _, verb := range rule.Verbs {
					if resource == "secrets" || resource == "*" {
						t.Fatalf("plan grants must exclude Secret get/list/watch and writes")
					}
					key := group + "/" + resource + "/" + verb
					if _, exists := required[key]; exists {
						required[key] = true
					}
				}
			}
		}
	}
	for key, found := range required {
		if !found {
			t.Errorf("missing narrow grant %s", key)
		}
	}
}

func TestProgramWebhookGrantIsNamedAndSecretFree(t *testing.T) {
	config := testConfig()
	config.PlanWebhookNames = []string{"reviewed-outage.example.invalid"}
	plan, err := Resources(config)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, role := range plan.ClusterRoles {
		if role.Name != planWebhookGrantName(config.controlNamespace()) {
			continue
		}
		found = true
		for _, r := range role.Rules {
			if len(r.ResourceNames) != 1 || r.ResourceNames[0] != config.PlanWebhookNames[0] {
				t.Fatal("webhook grant is not named")
			}
			for _, resource := range r.Resources {
				if resource == "secrets" || resource == "*" {
					t.Fatal("Secret data grant")
				}
			}
			for _, verb := range r.Verbs {
				if verb != "get" && verb != "delete" {
					t.Fatal("excess webhook verb")
				}
			}
		}
	}
	if !found {
		t.Fatal("missing named controller webhook grant")
	}
	config.PlanWebhookNames = nil
	plan, err = Resources(config)
	if err != nil {
		t.Fatal(err)
	}
	for _, role := range plan.ClusterRoles {
		if role.Name == planWebhookGrantName(config.controlNamespace()) {
			t.Fatal("unconfigured controller gets webhook delete")
		}
	}
}
