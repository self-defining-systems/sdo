package operatorcli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAcceptedCheckRejectsInvalidCleanTreeAndDirtyCheckout(t *testing.T) {
	root := t.TempDir()
	gitIn(t, root, "init", "-q")
	gitIn(t, root, "config", "user.name", "Synthetic")
	gitIn(t, root, "config", "user.email", "synthetic@example.test")
	if err := os.MkdirAll(filepath.Join(root, ".sdo"), 0755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, ".sdo/schema-version")
	if err := os.WriteFile(path, []byte("999\n"), 0644); err != nil {
		t.Fatal(err)
	}
	gitIn(t, root, "add", ".")
	gitIn(t, root, "commit", "-qm", "synthetic unsupported memory")
	var out, stderr bytes.Buffer
	getenv := func(string) string { return "" }
	code := RunMemory(context.Background(), []string{"check", "--accepted", "--root", root}, getenv, &out, &stderr)
	if code != exitDiagnostics || !strings.Contains(out.String(), "schema.version") {
		t.Fatalf("code=%d out=%s err=%s", code, &out, &stderr)
	}
	if err := os.WriteFile(path, []byte("3\n"), 0644); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	stderr.Reset()
	code = RunMemory(context.Background(), []string{"check", "--accepted", "--root", root}, getenv, &out, &stderr)
	if code != exitInfra || !strings.Contains(stderr.String(), "uncommitted changes") {
		t.Fatalf("code=%d out=%s err=%s", code, &out, &stderr)
	}
}
