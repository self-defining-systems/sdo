package validate

import "testing"

// Deployment files are source bytes: whitespace normalization would corrupt a
// patch's context lines, so they are never treated as authored memory text.
func TestDeploymentFilesAreNotAuthoredMemoryText(t *testing.T) {
	for p, want := range map[string]bool{
		".sdo/deployment/patches/0001-fix.patch": false,
		".sdo/deployment/values/prod.yaml":       false,
		".sdo/playbooks/base/README.md":          true,
		".sdo/goal.md":                           true,
	} {
		if got := authoredMemoryText(p); got != want {
			t.Errorf("authoredMemoryText(%q) = %v, want %v", p, got, want)
		}
	}
}
