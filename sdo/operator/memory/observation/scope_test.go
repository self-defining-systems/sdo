package observation

import (
	"sdo.dev/operator/sdk"
	"testing"
)

func TestScopeDeclarationsAreBoundedAndNamed(t *testing.T) {
	header := "---\nschema_version: 1\nobservation_scope:\n  version: 1\n  cluster_kinds:\n  - api_version: admissionregistration.k8s.io/v1\n    kind: MutatingWebhookConfiguration\n    names: [policy.example]\n---\nArchitecture\n"
	scope, err := Parse([]byte(header))
	if err != nil {
		t.Fatal(err)
	}
	if len(scope.Targets) != 1 || scope.Targets[0].Name != "policy.example" {
		t.Fatalf("scope=%+v", scope)
	}
	if err = scope.Within(sdk.ObservationScope{}); err == nil {
		t.Fatal("missing ceiling accepted")
	}
	for _, bad := range []string{"---\nobservation_scope: {version: 2}\n---\nx", "---\nobservation_scope: {version: 1, cluster_kinds: [{api_version: admissionregistration.k8s.io/v1, kind: MutatingWebhookConfiguration}]}\n---\nx", "---\nobservation_scope: {version: 1, forged_ceiling: {}}\n---\nx"} {
		if _, err := Parse([]byte(bad)); err == nil {
			t.Fatalf("bad scope accepted: %s", bad)
		}
	}
}
