package operatorcli

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"sdo.dev/operator/artifacts"
	"sdo.dev/operator/memory/validate"
)

// A validation seed must satisfy the same whole-tree rules as reflection drafts.
// Compiler-only validation misses Markdown ownership, role placeholders and indexes.
func TestToySeedPassesBrokerMemoryValidation(t *testing.T) {
	root := t.TempDir()
	if err := os.CopyFS(root, os.DirFS(filepath.Join("..", "..", "..", "apps", "toy", "seed"))); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"init", "-q"}, {"add", "."}, {"-c", "user.name=toy", "-c", "user.email=toy@invalid", "commit", "-qm", "seed"}} {
		command := exec.Command("git", args...)
		command.Dir = root
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
	}
	// Startup upkeep must leave a current fixture's accepted commit unchanged.
	if err := (artifacts.BootstrapStore{}).WriteSchema(root); err != nil {
		t.Fatal(err)
	}
	if changed := gitIn(t, root, "status", "--porcelain"); changed != "" {
		t.Fatalf("toy seed requires startup schema migration: %s", changed)
	}
	diagnostics, err := validate.Validate(context.Background(), validate.Request{
		Root: root, Layers: validate.Structural | validate.Semantic | validate.Compile,
		OperatorRoot: "..",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := validate.Err(diagnostics); err != nil {
		t.Fatal(err)
	}
}
