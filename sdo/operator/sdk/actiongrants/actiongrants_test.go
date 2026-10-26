package actiongrants_test

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"sdo.dev/operator/sdk/actiongrants"
)

// Every Action implementation must say what RBAC it needs, so the installer's
// Role cannot lag the vocabulary.
func TestEveryActionTypeDeclaresItsPermissions(t *testing.T) {
	files, err := filepath.Glob("../program/*.go")
	if err != nil {
		t.Fatal(err)
	}
	declared := regexp.MustCompile(`(?m)^func \((\w+)\) action\(\) \{\}`)
	seen := map[string]bool{}
	for _, file := range files {
		source, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		for _, match := range declared.FindAllStringSubmatch(string(source), -1) {
			seen[match[1]] = true
			entry, ok := actiongrants.ActionPermissions[match[1]]
			if !ok {
				t.Errorf("%s: action type %s has no ActionPermissions entry", file, match[1])
				continue
			}
			if (len(entry.Needs) == 0) == (entry.OpenEnded == "") {
				t.Errorf("%s must declare Needs or OpenEnded, not both and not neither", match[1])
			}
			if entry.Identity == "" {
				t.Errorf("%s names no ServiceAccount that performs it", match[1])
			}
		}
	}
	for name := range actiongrants.ActionPermissions {
		if !seen[name] {
			t.Errorf("ActionPermissions lists %s, which is not an Action type", name)
		}
	}
}
