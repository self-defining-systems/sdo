package validate

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestAcceptedAuditChecksWholeTreeBeforeCompile(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, ".sdo"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".sdo/schema-version"), []byte("999\n"), 0644); err != nil {
		t.Fatal(err)
	}
	compiled := false
	ds, err := CheckAccepted(context.Background(), Request{Root: root}, func(context.Context, Request) ([]Diagnostic, error) { compiled = true; return nil, nil })
	if err != nil || compiled || len(ds) == 0 {
		t.Fatalf("audit ds=%v compiled=%v error=%v", ds, compiled, err)
	}
}
