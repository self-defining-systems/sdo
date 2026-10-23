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
