package archcheck

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// fixtureConfig is the real rule table re-rooted onto the synthetic module
// used by testdata, with no allowlists.
func fixtureConfig() Config {
	cfg := Rules()
	const module = "fx.test/op"
	cfg.Module = module
	cfg.ImplMayImport = []string{module + "/contracts", module + "/sdk"}
	cfg.ConfigTypes = []ConfigType{{Name: "RawManifestStrategy", Import: module + "/contracts/gen", OwnerPkg: module + "/render/raw"}}
	cfg.RootForbiddenImports = []string{module + "/runtime"}
	cfg.Allow = nil
	cfg.Ratchet = nil
	return cfg
}

func keys(vs []Violation) []string {
	var out []string
	for _, v := range vs {
		out = append(out, string(v.Rule)+" "+v.File)
	}
	sort.Strings(out)
	return out
}

// TestNegativeControls proves each rule fires on its fixture and that the
// positive fixture stays clean, so a rule that silently stops matching fails here.
func TestNegativeControls(t *testing.T) {
	cases := []struct {
		dir  string
		mod  func(*Config)
		want []string
	}{
		{dir: "clean", want: nil},
		{dir: "r1_tool", want: []string{"R1 core/core.go"}},
		{dir: "r1_impl", want: []string{"R1 core/core.go"}},
		{dir: "r2_core", want: []string{"R2 render/helm/helm.go"}},
		{dir: "r2_sibling", want: []string{"R2 render/helm/helm.go"}},
		{dir: "r2_family", want: []string{"R2 render/helm/helm.go"}},
		{dir: "r3_support", want: []string{"R3 render/rendertest/x.go"}},
		{dir: "r6_wiring", mod: func(c *Config) { c.RootForbiddenImports = nil }, want: []string{"R6 runtime/launch.go"}},
		{dir: "r4_assert", want: []string{"R4 deployment/registry.go", "R4 deployment/registry.go"}},
		{dir: "r4_use", want: []string{"R4 lifecycle/compat.go", "R4 lifecycle/compat.go", "R4 lifecycle/compat.go", "R4 lifecycle/compat.go"}},
		{dir: "r4_exempt", want: nil},
		{dir: "r4_test_assert", want: []string{"R4 lifecycle/compat_test.go"}},
		{dir: "r5_ratchet", mod: func(c *Config) {
			c.Ratchet = []RatchetEntry{
				{File: "rose.go", Count: 1, Reason: "r", Slice: "s"},
				{File: "fell.go", Count: 3, Reason: "r", Slice: "s"},
				{File: "gone.go", Count: 1, Reason: "r", Slice: "s"},
			}
		}, want: []string{"R5 fell.go", "R5 gone.go", "R5 new.go", "R5 rose.go"}},
	}
	fired := map[Rule]bool{}
	for _, tc := range cases {
		t.Run(tc.dir, func(t *testing.T) {
			cfg := fixtureConfig()
			if tc.mod != nil {
				tc.mod(&cfg)
			}
			got, err := Check(filepath.Join("testdata", tc.dir), cfg)
			if err != nil {
				t.Fatal(err)
			}
			sort.Strings(tc.want)
			if strings.Join(keys(got), "\n") != strings.Join(tc.want, "\n") {
				t.Fatalf("violations\n got: %v\nwant: %v\n%v", keys(got), tc.want, got)
			}
			for _, v := range got {
				fired[v.Rule] = true
			}
		})
	}
	for _, r := range []Rule{R1, R2, R3, R4, R5, R6} {
		if !fired[r] {
			t.Errorf("no negative control fires %s", r)
		}
	}
}

func TestRatchetMessagesTellAuthorWhatToDo(t *testing.T) {
	cfg := fixtureConfig()
	cfg.Ratchet = []RatchetEntry{{File: "fell.go", Count: 3, Reason: "r", Slice: "s"}}
	got, err := Check(filepath.Join("testdata", "r5_ratchet"), cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range got {
		if v.File == "fell.go" && strings.Contains(v.Message, "lower the Count") {
			return
		}
	}
	t.Fatalf("no lower-the-count message: %v", got)
}

func TestAllowlistSuppressesAndGoesStale(t *testing.T) {
	cfg := fixtureConfig()
	cfg.Allow = []Allow{{Rule: R1, File: "core/core.go", Reason: "r", Slice: "s"}}
	if got, err := Check(filepath.Join("testdata", "r1_tool"), cfg); err != nil || len(got) != 0 {
		t.Fatalf("allowlisted violation reported: %v %v", got, err)
	}
	// The same entry on a clean tree is stale and must fail.
	got, err := Check(filepath.Join("testdata", "clean"), cfg)
	if err != nil || len(got) != 1 || !strings.Contains(got[0].Message, "stale") {
		t.Fatalf("stale allowlist entry not reported: %v %v", got, err)
	}
}

func TestTableEntriesNeedReasonAndSlice(t *testing.T) {
	cfg := Rules()
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	cfg.Allow = append(cfg.Allow, Allow{Rule: R1, File: "x.go"})
	if cfg.Validate() == nil {
		t.Fatal("allow entry without reason accepted")
	}
}

func TestDirectives(t *testing.T) {
	cases := []struct {
		dir      string
		mod      func(*Config)
		want     []string
		contains string
	}{
		{dir: "dir_ok", want: nil},
		{dir: "dir_rose", want: []string{"R5 rat.go"}, contains: "count rose"},
		{dir: "dir_fell", want: []string{"R5 rat.go"}, contains: "lower count= in this file"},
		{dir: "dir_stale_allow", want: []string{"R1 core/core.go"}, contains: "stale archcheck:allow directive"},
		{dir: "dir_stale_ratchet", want: []string{"R5 rat.go"}, contains: "delete this file's archcheck:ratchet directive"},
		{dir: "dir_unknown_until", want: []string{"DIR core/core.go", "R1 core/core.go"}, contains: "not in TransitionalPRs"},
		{dir: "dir_malformed", want: []string{"DIR a.go", "DIR b.go", "DIR c.go", "DIR d.go", "DIR e.go"}},
		{dir: "dir_generated", want: []string{"DIR gen.go", "R5 gen.go"}, contains: "generated files cannot carry"},
		{dir: "dir_conflict", mod: func(c *Config) {
			c.Ratchet = []RatchetEntry{{File: "rat.go", Count: 1, Reason: "r", Slice: "s"}}
		}, want: []string{"DIR rat.go"}, contains: "keep only the directive"},
	}
	for _, tc := range cases {
		t.Run(tc.dir, func(t *testing.T) {
			cfg := fixtureConfig()
			if tc.mod != nil {
				tc.mod(&cfg)
			}
			got, err := Check(filepath.Join("testdata", tc.dir), cfg)
			if err != nil {
				t.Fatal(err)
			}
			sort.Strings(tc.want)
			if strings.Join(keys(got), "\n") != strings.Join(tc.want, "\n") {
				t.Fatalf("violations\n got: %v\nwant: %v\n%v", keys(got), tc.want, got)
			}
			if tc.contains != "" {
				for _, v := range got {
					if strings.Contains(v.Message, tc.contains) {
						return
					}
				}
				t.Fatalf("no message containing %q: %v", tc.contains, got)
			}
		})
	}
}

// TestRuleTableSlicesAreKnownPRs keeps table entries (generated files) on the
// same closed PR list as directives.
func TestRuleTableSlicesAreKnownPRs(t *testing.T) {
	cfg := Rules()
	for _, r := range cfg.Ratchet {
		if !cfg.knownPR(r.Slice) {
			t.Errorf("ratchet entry %s names unknown PR %q", r.File, r.Slice)
		}
	}
	for _, a := range cfg.Allow {
		if !cfg.knownPR(a.Slice) {
			t.Errorf("allow entry %s names unknown PR %q", a.File, a.Slice)
		}
	}
}

// TestRootModuleMayNotImportRuntime builds the fixture at test time (nested
// go.mod files under testdata would be picked up as real modules by tools that
// walk the repository): a non-test root file importing runtime is R7; a test
// file, and files of the nested runtime and integration modules, are not.
func TestRootModuleMayNotImportRuntime(t *testing.T) {
	root := t.TempDir()
	files := map[string]string{
		"lifecycle/x.go":      "package lifecycle\n\nimport _ \"fx.test/op/runtime\"\n",
		"lifecycle/x_test.go": "package lifecycle\n\nimport _ \"fx.test/op/runtime\"\n",
		"runtime/go.mod":      "module fx.test/op/runtime\n",
		"runtime/r.go":        "package runtime\n",
		"integration/go.mod":  "module fx.test/op/integration\n",
		"integration/a.go":    "package integration\n\nimport _ \"fx.test/op/runtime\"\n",
	}
	for name, src := range files {
		p := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(src), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	got, err := Check(root, fixtureConfig())
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"R7 lifecycle/x.go"}; strings.Join(keys(got), "\n") != strings.Join(want, "\n") {
		t.Fatalf("violations %v, want %v", keys(got), want)
	}
}
