package program_test

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"sdo.dev/operator/sdk/program"
)

// Every Action implementation must say what RBAC it needs, so the installer's
// Role cannot lag the vocabulary.
func TestEveryActionTypeDeclaresItsPermissions(t *testing.T) {
	files, err := filepath.Glob("*.go")
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
			entry, ok := program.ActionPermissions[match[1]]
			if !ok {
				t.Errorf("%s: action type %s has no ActionPermissions entry", file, match[1])
				continue
			}
			if len(entry.Needs) == 0 && entry.OpenEnded == "" && entry.Elsewhere == "" {
				t.Errorf("%s declares nothing", match[1])
			}
		}
	}
	for name := range program.ActionPermissions {
		if !seen[name] {
			t.Errorf("ActionPermissions lists %s, which is not an Action type", name)
		}
	}
}
