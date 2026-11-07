package archcheck

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeModules(t *testing.T, mods map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for dir, src := range mods {
		if err := os.MkdirAll(filepath.Join(root, dir), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, dir, "go.mod"), []byte(src), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func TestModuleCyclesFindsTheRootRequiringRuntime(t *testing.T) {
	root := writeModules(t, map[string]string{
		".":       "module m\n\nrequire (\n\tm/runtime v0.0.0 // indirect\n\tgolang.org/x/tools v1\n)\n",
		"runtime": "module m/runtime\n\nrequire m v0.0.0\n",
	})
	cycles, err := ModuleCycles(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(cycles) != 1 || !strings.Contains(cycles[0], "m -> m/runtime -> m") {
		t.Fatalf("cycles = %v, want m -> m/runtime -> m", cycles)
	}
}

func TestModuleCyclesAllowsATestOnlyModuleOnTop(t *testing.T) {
	root := writeModules(t, map[string]string{
		".":           "module m\n",
		"runtime":     "module m/runtime\n\nrequire m v0.0.0\n",
		"integration": "module m/integration\n\nrequire m v0.0.0\nrequire m/runtime v0.0.0\n\nreplace m => ..\n",
	})
	cycles, err := ModuleCycles(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(cycles) != 0 {
		t.Fatalf("cycles = %v, want none", cycles)
	}
}

// TestRepositoryModulesAreAcyclic runs the cycle check over the real modules.
func TestRepositoryModulesAreAcyclic(t *testing.T) {
	cycles, err := ModuleCycles("../..")
	if err != nil {
		t.Fatal(err)
	}
	if len(cycles) > 0 {
		t.Fatalf("go.mod require cycles: %v (tests that need two cyclic modules belong in sdo/operator/integration)", cycles)
	}
}
