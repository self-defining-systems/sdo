package program

import (
	"os"
	"path/filepath"
	"testing"
)

func TestScriptPublicationAcceptsNoReceiptClaim(t *testing.T) {
	root := t.TempDir()
	const relative = ".sdo/playbooks/synthetic-script/mitigate.go"
	dir := filepath.Dir(filepath.Join(root, relative))
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	source := []byte("package example\nimport \"sdo.dev/operator/sdk/program\"\nvar repair = program.RunScript{Script: \"true\", Reason: \"synthetic repair\"}\n")
	if err := os.WriteFile(filepath.Join(root, relative), source, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte("---\nid: synthetic-script\n---\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Run("corpus", func(t *testing.T) {
		if err := validateScriptDigests(dir, filepath.Join(root, ".sdo", "corpus"), Spec{}); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("no corpus", func(t *testing.T) {
		if err := ValidateScriptOriginsWithoutCorpus(t.Context(), root, "", nil, []string{relative}); err != nil {
			t.Fatal(err)
		}
	})
}
