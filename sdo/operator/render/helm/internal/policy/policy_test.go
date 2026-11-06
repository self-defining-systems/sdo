package policy_test

import (
	"runtime/debug"

	"errors"
	"fmt"
	"github.com/Masterminds/sprig/v3"
	"slices"
	"strings"
	"testing"
	"text/template"

	"sdo.dev/operator/render/helm/internal/engine"
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

// TestEveryTemplateFunctionIsClassified fails when the pinned Helm engine or sprig
// function map holds a name the policy has not classified, and when the
// classification names a function the maps no longer have.
func TestEveryTemplateFunctionIsClassified(t *testing.T) {
	known := policy.KnownFunctions()
	for name := range sprig.TxtFuncMap() {
		if !known[name] {
			t.Errorf("sprig function %q is not classified in policy", name)
		}
	}
	universe := map[string]bool{}
	for name := range sprig.TxtFuncMap() {
		universe[name] = true
	}
	for _, name := range engine.FuncNames() {
		universe[name] = true
		if !known[name] {
			t.Errorf("engine function %q is not classified in policy", name)
		}
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
	for _, name := range []string{"lookup", "now", "date", "date_in_zone", "dateInZone", "date_modify", "unixEpoch", "toDate", "ago", "env", "expandenv", "randAlpha", "uuidv4", "genCA", "bcrypt", "encryptAES", "getHostByName", "shuffle"} {
		if _, bad := policy.Forbidden(name); !bad {
			t.Errorf("%s must be forbidden", name)
		}
	}
	for _, name := range []string{"include", "tpl", "duration", "durationRound", "toYaml", "required", "default", "printf", "index", "sha256sum", "quote", "derivePassword"} {
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
// Restrict stubs every forbidden or unclassified function of a map, adds the
// forbidden ones a map lacks (env and expandenv, which Helm deletes), and keeps
// the allowed ones.
func TestRestrictStubsForbiddenAndUnclassifiedFunctions(t *testing.T) {
	keep := func(string) string { return "kept" }
	funcs := template.FuncMap{"upper": keep, "now": keep, "someFutureFunction": keep}
	policy.Restrict(funcs)
	if got := funcs["upper"].(func(string) string)("x"); got != "kept" {
		t.Fatalf("an allowed function must be left alone, got %q", got)
	}
	for _, name := range []string{"now", "someFutureFunction", "env", "expandenv", "lookup"} {
		stub, ok := funcs[name].(func(...any) (any, error))
		if !ok {
			t.Fatalf("%s is not a stub: %T", name, funcs[name])
		}
		_, err := stub(1, "x")
		var perr *policy.Error
		if !errors.As(err, &perr) || perr.Rule != policy.RuleForbiddenFunction || !strings.Contains(perr.Detail, `"`+name+`"`) {
			t.Fatalf("%s: stub error %v", name, err)
		}
	}
}

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
