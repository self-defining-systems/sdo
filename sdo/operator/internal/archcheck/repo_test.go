package archcheck

import (
	"strings"
	"testing"
)

// TestRepositoryObeysRules runs the real rule table over the operator module.
func TestRepositoryObeysRules(t *testing.T) {
	violations, err := Check("../..", Rules())
	if err != nil {
		t.Fatal(err)
	}
	if len(violations) > 0 {
		var lines []string
		for _, v := range violations {
			lines = append(lines, v.String())
		}
		t.Fatalf("architecture boundary violations (fix them, or for a shrinking allowlist update internal/archcheck/rules.go):\n%s", strings.Join(lines, "\n"))
	}
}
