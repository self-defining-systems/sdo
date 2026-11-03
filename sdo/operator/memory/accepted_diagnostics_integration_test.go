//go:build integration

package memory

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"sdo.dev/operator/memory/layout"
)

func TestAcceptedDiagnosticsAllowsMissingArtifactClasses(t *testing.T) {
	for _, programs := range []bool{false, true} {
		t.Run(fmt.Sprint(programs), func(t *testing.T) {
			root := t.TempDir()
			acceptedGit(t, root, "init", "-q")
			if err := os.MkdirAll(filepath.Join(root, ".sdo"), 0755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, layout.SchemaVersionFile), []byte("3\n"), 0644); err != nil {
				t.Fatal(err)
			}
			if programs {
				path := filepath.Join(root, ".sdo/playbooks/repair/diagnose.go")
				if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte("package repair\n"), 0644); err != nil {
					t.Fatal(err)
				}
			}
			acceptedGit(t, root, "add", ".")
			acceptedGit(t, root, "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "synthetic memory")
			projection, err := AcceptedDiagnostics(context.Background(), root)
			if err != nil {
				t.Fatal(err)
			}
			if projection.Directory != filepath.Join(root, layout.DiagnosticsDir) || projection.Fingerprint == "" || projection.AcceptedRevision == "" {
				t.Fatalf("incomplete empty catalog projection: %+v", projection)
			}
		})
	}
}

func acceptedGit(t *testing.T, root string, args ...string) {
	t.Helper()
	command := exec.Command("git", args...)
	command.Dir = root
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v: %s", args, err, output)
	}
}
