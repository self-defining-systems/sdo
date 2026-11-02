package builder

import (
	"context"
	"os"
	"path/filepath"
	"sdo.dev/operator/internal/testutil"
	"strings"
	"testing"
)

func TestRetiredProgramIsNotStagedCompiledOrRegistered(t *testing.T) {
	testutil.OfflineGo(t, "..")
	root := programFixture(t)
	base := filepath.Join(root, ".sdo", "playbooks", "service-targetport-mismatch")
	p := filepath.Join(base, "README.md")
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	data = []byte(strings.Replace(string(data), "schema_version: 1\n", "schema_version: 1\nstatus: retired\nretired_reason: mitigation was undone\n", 1))
	if err := os.WriteFile(p, data, 0644); err != nil {
		t.Fatal(err)
	}
	// Valid syntax with an unresolved symbol proves retired code is not compiled.
	if err := os.WriteFile(filepath.Join(base, "diagnose.go"), []byte("package targetport\nvar broken MissingType\n"), 0644); err != nil {
		t.Fatal(err)
	}
	w, err := buildProgramFixture(t, root)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, err := os.Stat(filepath.Join(w.Directory, "playbooks", "service-targetport-mismatch", "diagnose.go")); !os.IsNotExist(err) {
		t.Fatalf("retired source staged: %v", err)
	}
	generated, err := os.ReadFile(filepath.Join(w.Directory, "generated", "programs.go"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(generated), "service-targetport-mismatch") {
		t.Fatal("retired program registered")
	}
	if _, err := w.ValidateAndBuild(context.Background()); err != nil {
		t.Fatal(err)
	}
}
