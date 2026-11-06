package policy_test

import (
	"errors"
	"fmt"
	"slices"
	"strings"
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

func TestHookPhasesSelectByOperation(t *testing.T) {
	cases := []struct {
		op     policy.Operation
		events []string
		want   []string
	}{
		{policy.OperationInstall, []string{"pre-install", "post-install"}, []string{"pre-install", "post-install"}},
		{policy.OperationInstall, []string{"pre-install", "pre-upgrade"}, []string{"pre-install"}},
		{policy.OperationInstall, []string{"pre-upgrade", "post-delete", "test", "test-success", "pre-rollback", ""}, nil},
		{policy.OperationUpgrade, []string{"pre-install", "pre-upgrade", "post-upgrade"}, []string{"pre-upgrade", "post-upgrade"}},
		{policy.OperationUpgrade, []string{"pre-install", "test"}, nil},
	}
	for _, tc := range cases {
		if got := policy.HookPhases(tc.op, tc.events); !slices.Equal(got, tc.want) {
			t.Errorf("%s %v: got %v, want %v", tc.op, tc.events, got, tc.want)
		}
	}
}

func TestConditionalResolveReportsOnlyKeptFiles(t *testing.T) {
	c := policy.Conditional{
		"c/templates/a.yaml": {{Rule: policy.RuleForbiddenFunction, Chart: "c", File: "templates/a.yaml"}},
		"c/templates/b.yaml": {{Rule: policy.RuleForbiddenFunction, Chart: "c", File: "templates/b.yaml"}},
	}
	if err := c.Resolve(func(string) bool { return false }); err != nil {
		t.Fatalf("no kept file: %v", err)
	}
	err := c.Resolve(func(f string) bool { return f == "c/templates/b.yaml" })
	var perr *policy.Error
	if !errors.As(err, &perr) || perr.File != "templates/b.yaml" || strings.Contains(err.Error(), "a.yaml") {
		t.Fatalf("only b.yaml may be reported: %v", err)
	}
}
