package operatorcli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMemoryCommandDefaultsToProcessEnvironment(t *testing.T) {
	t.Setenv(memoryIncidentEnv, "synthetic-incident")
	root := t.TempDir()
	var stdout, stderr bytes.Buffer
	code := RunMemory(context.Background(), []string{"new", "playbook", "repair", "--root", root}, nil, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit %d: %s", code, &stderr)
	}
	data, err := os.ReadFile(filepath.Join(root, ".sdo/playbooks/repair/README.md"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "synthetic-incident") {
		t.Fatalf("incident environment missing from scaffold: %s", data)
	}
}
