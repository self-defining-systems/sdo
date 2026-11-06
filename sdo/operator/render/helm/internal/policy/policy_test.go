package policy_test

import (
	"runtime/debug"

	"github.com/Masterminds/sprig/v3"
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

// TestEveryTemplateFunctionIsClassified fails when the pinned Helm or sprig
// function map holds a name the policy has not classified, and when the
// classification names a function the maps no longer have.
func TestEveryTemplateFunctionIsClassified(t *testing.T) {
	known := policy.KnownFunctions()
	for name := range sprig.TxtFuncMap() {
		if !known[name] {
			t.Errorf("sprig function %q is not classified in policy", name)
		}
	}
	for _, name := range policy.HelmFunctions() {
		if !known[name] {
			t.Errorf("helm function %q is not classified in policy", name)
		}
	}
	universe := map[string]bool{}
	for name := range sprig.TxtFuncMap() {
		universe[name] = true
	}
	for _, n := range policy.HelmFunctions() {
		universe[n] = true
	}
	for _, n := range policy.BuiltinFunctions() {
		universe[n] = true
	}
	for name := range known {
		if !universe[name] {
			t.Errorf("classified function %q exists in no function map", name)
		}
	}
}

func TestClassificationHasNoOverlapAndForbidsTheNondeterministic(t *testing.T) {
	for _, name := range []string{"tpl", "lookup", "now", "date", "date_in_zone", "dateInZone", "date_modify", "unixEpoch", "toDate", "ago", "env", "expandenv", "randAlpha", "uuidv4", "genCA", "bcrypt", "encryptAES", "getHostByName", "shuffle"} {
		if _, bad := policy.Forbidden(name); !bad {
			t.Errorf("%s must be forbidden", name)
		}
	}
	for _, name := range []string{"include", "toYaml", "required", "default", "printf", "index", "sha256sum", "quote", "derivePassword"} {
		if why, bad := policy.Forbidden(name); bad {
			t.Errorf("%s must be allowed, got %q", name, why)
		}
	}
	if _, bad := policy.Forbidden("noSuchFunction"); !bad {
		t.Error("an unclassified name must be forbidden")
	}
}

// The classification is reviewed against one Helm version; a bump must
// re-read its function map (HelmFunctions) before this constant moves.
func TestClassificationMatchesThePinnedHelm(t *testing.T) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		t.Skip("no build info")
	}
	for _, d := range info.Deps {
		if d.Path == "helm.sh/helm/v3" && d.Version != policy.ReviewedHelmVersion {
			t.Fatalf("helm %s is pinned but the function classification was reviewed for %s: re-read pkg/engine/funcs.go and update HelmFunctions", d.Version, policy.ReviewedHelmVersion)
		}
	}
}
