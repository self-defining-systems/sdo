package runtime

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Ratchet: a learned mitigation's plan is built in one place. The stage, the
// responder's plan command, follow-ups and replay validation reach it through
// BuildMitigationPlan; a direct call elsewhere would let them drift apart. The
// exceptions are named: the builder itself, the out-of-process runner for
// working programs, and the SDK's own test helpers.
func TestMitigationPlansAreBuiltOnlyThroughBuildMitigationPlan(t *testing.T) {
	allowed := map[string]bool{
		"runtime/program_mitigator.go":  true,
		"runtime/jit_program_runner.go": true,
		"sdk/sdktest/mitigation.go":     true,
		"sdk/sdktest/generalization.go": true,
		// The memory host runs accepted programs in its own process; the
		// controller reaches them through these RPC seams and builds plans
		// from the validated response.
		"memoryhost/program.go":        true,
		"memoryhost/manager.go":        true,
		"memoryhost/mirror.go":         true,
		"memoryhost/hostio/process.go": true,
		"runtime/memory_host.go":       true,
	}
	root := ".."
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == "testdata" || entry.Name() == "gen" || entry.Name() == "examples" {
				return filepath.SkipDir
			}
			return nil
		}
		relative, _ := filepath.Rel(root, path)
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") || allowed[filepath.ToSlash(relative)] {
			return nil
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(body), ".Mitigate(") {
			t.Errorf("%s calls Mitigate directly; build plans with BuildMitigationPlan", relative)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
