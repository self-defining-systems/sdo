package builder

import (
	"context"
	"strings"
	"testing"
)

// The playbook ID names a checkout directory and a generated Go package, so
// BuildWorkingProgram refuses anything but a lowercase slug before it touches
// the filesystem or a compiler.
func TestBuildWorkingProgramRefusesNonSlugPlaybookIDs(t *testing.T) {
	for _, id := range []string{"", "../escape", "a/b", "Upper", "-leading", "has space", "dot.ted", "under_score", "new\nline"} {
		_, _, err := BuildWorkingProgram(context.Background(), Config{}, id)
		if err == nil || !strings.Contains(err.Error(), "invalid playbook ID") {
			t.Errorf("id %q: want invalid playbook ID, got %v", id, err)
		}
	}
	for _, id := range []string{"a", "service-targetport-mismatch", "0-day"} {
		_, _, err := BuildWorkingProgram(context.Background(), Config{}, id)
		if err != nil && strings.Contains(err.Error(), "invalid playbook ID") {
			t.Errorf("id %q is a valid slug, got %v", id, err)
		}
	}
}
