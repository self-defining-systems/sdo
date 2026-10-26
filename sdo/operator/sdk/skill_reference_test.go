package sdk_test

import (
	"os/exec"
	"path/filepath"
	"testing"
)

// The authoring skills' SDK references are generated from this module.
// A stale copy teaches agents an API that no longer exists.
func TestSkillSDKReferenceIsCurrent(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash is required")
	}
	script := filepath.Join("..", "..", "..", "scripts", "gen_skill_references.sh")
	output, err := exec.Command("bash", script, "--check").CombinedOutput()
	if err != nil {
		t.Fatalf("generated skill references are stale (files named below); run scripts/gen_skill_references.sh:\n%s", output)
	}
}
