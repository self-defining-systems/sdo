package operatorcli

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBaselineWriteReplacesOnlyNamedSnapshots(t *testing.T) {
	root := filepath.Join(t.TempDir(), "healthy")
	if err := RunBaselineWrite(strings.NewReader(`{"healthy-0.json":{"namespace":"demo"}}`), root); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(filepath.Join(root, "healthy-0.json")); err != nil || !strings.Contains(string(data), `"namespace":"demo"`) {
		t.Fatalf("snapshot not published: %s %v", data, err)
	}
	if err := RunBaselineWrite(strings.NewReader(`{"../escape.json":{}}`), root); err == nil {
		t.Fatal("unsafe snapshot name accepted")
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(root), "escape.json")); !os.IsNotExist(err) {
		t.Fatalf("unsafe snapshot escaped target: %v", err)
	}
}

// A real astronomy namespace exceeds the old per-document MiB even though all
// three complete captures fit inside the writer's total transport budget.
func TestBaselineWriteCompleteLargeNamespaceWithinTotalBudget(t *testing.T) {
	document := json.RawMessage(`{"namespace":"astronomy-shop","configMaps":[{"data":{"fixture":"` + strings.Repeat("x", 2<<20) + `"}}]}`)
	files := map[string]json.RawMessage{"healthy-0.json": document, "healthy-1.json": document, "healthy-2.json": document}
	input, err := json.Marshal(files)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	if err := RunBaselineWrite(strings.NewReader(string(input)), root); err != nil {
		t.Fatalf("bounded complete snapshots rejected: %v", err)
	}
	for name := range files {
		data, err := os.ReadFile(filepath.Join(root, name))
		if err != nil || len(data) != len(document) {
			t.Fatalf("snapshot truncated: %s %d %v", name, len(data), err)
		}
	}
	if err := RunBaselineWrite(strings.NewReader(`{"healthy-0.json":{"padding":"`+strings.Repeat("x", 8<<20)+`"}}`), root); err == nil {
		t.Fatal("aggregate transport limit removed")
	}
}
