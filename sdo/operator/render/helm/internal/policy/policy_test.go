package policy_test

import (
	"errors"
	"fmt"
	"testing"

	"sdo.dev/operator/render/helm/internal/policy"
)

func TestViolatesMatchesRuleThroughJoinAndWrap(t *testing.T) {
	err := fmt.Errorf("render: %w", errors.Join(
		&policy.Error{Rule: policy.RuleCRDs, Chart: "c", File: "crds/x.yaml", Detail: "d"},
		&policy.Error{Rule: policy.RuleForbiddenFunction, Chart: "c"},
	))
	if !policy.Violates(err, policy.RuleCRDs) || !policy.Violates(err, policy.RuleForbiddenFunction) {
		t.Fatalf("both rules must be found in %v", err)
	}
	if policy.Violates(err, policy.RuleDependencyNotVendored) || policy.Violates(nil, policy.RuleCRDs) {
		t.Fatal("an absent rule must not match")
	}
}

func TestHookPhases(t *testing.T) {
	got, err := policy.HookPhases("c", "hook.yaml", []string{"pre-install", "post-install"})
	if err != nil || len(got) != 2 || got[0] != "pre-install" || got[1] != "post-install" {
		t.Fatalf("got %v, %v", got, err)
	}
	for _, e := range []string{"pre-upgrade", "post-delete", "test", "test-success", "pre-rollback", ""} {
		if _, err := policy.HookPhases("c", "hook.yaml", []string{e}); !policy.Violates(err, policy.RuleUnsupportedHook) {
			t.Errorf("%q: want unsupported-hook, got %v", e, err)
		}
	}
}
