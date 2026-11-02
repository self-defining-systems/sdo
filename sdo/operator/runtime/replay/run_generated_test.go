package replay

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"sdo.dev/operator/sdk/program"
)

// A generated replay binary whose registry cannot be built must exit non-zero
// with the reason on stderr; exit 0 would read as a clean replay.
func TestRunGeneratedFailsWhenTheRegistryCannotBeBuilt(t *testing.T) {
	var out, summary bytes.Buffer
	load := func() (program.ProgramRegistry, error) { return nil, errors.New("duplicate playbook id") }
	if code := RunGenerated(context.Background(), load, nil, []string{"-fixtures", t.TempDir()}, &out, &summary); code != 2 {
		t.Fatalf("exit code = %d, want 2", code)
	}
	if !strings.Contains(summary.String(), "duplicate playbook id") || out.Len() != 0 {
		t.Fatalf("stdout=%q stderr=%q", out.String(), summary.String())
	}
}
