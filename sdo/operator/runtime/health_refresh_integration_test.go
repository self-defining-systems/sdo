package runtime

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/memory"
)

func tracingWrite(t *testing.T, root, path, content string) {
	t.Helper()
	target := filepath.Join(root, path)
	if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
}

func tracingRefresh(t *testing.T, root string, c lifecycle.Coordinator) memory.HealthRevision {
	t.Helper()
	state, err := memory.AcceptedHealthRevision(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.RefreshHealth(context.Background(), lifecycle.Config{Application: "tracing", Namespace: "demo"}, state); err != nil {
		t.Fatal(err)
	}
	accepted, err := memory.AcceptedHealthRevision(context.Background(), root)
	if err != nil || accepted.NeedsRefresh {
		t.Fatalf("health not accepted: %+v %v", accepted, err)
	}
	return accepted
}

