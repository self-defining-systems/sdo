package archcheck

import (
	"strings"
	"testing"
)

// TestRepositoryObeysRules runs the real rule table over the operator module.
//
//ci:whole-tree
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
		t.Fatalf("architecture boundary violations (fix them; a count that fell or a stale //archcheck: directive is lowered or deleted in its own file, see docs/testing-guide.md):\n%s", strings.Join(lines, "\n"))
	}
}
