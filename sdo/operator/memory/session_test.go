package memory

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPrepareIncidentSessionIsNarrowAndDistinct(t *testing.T) {
	root := t.TempDir()
	first, err := PrepareIncidentSession(root, "tenant", "incident-A")
	if err != nil {
		t.Fatal(err)
	}
	second, err := PrepareIncidentSession(root, "tenant", "incident-B")
	if err != nil {
		t.Fatal(err)
	}
	if first == second || strings.Contains(first, "incident-A") || !strings.HasPrefix(first, "tenant/.sdo-runtime/sessions/") {
		t.Fatalf("session subpaths leak or collide: %q %q", first, second)
	}
	if info, err := os.Stat(filepath.Join(root, strings.TrimPrefix(first, "tenant/"))); err != nil || !info.IsDir() || info.Mode().Perm() != 0700 {
		t.Fatalf("session directory was not prepared privately: %v %v", info, err)
	}
	if _, err := PrepareIncidentSession(root, "../other", "incident-A"); err == nil {
		t.Fatal("escaping PVC subPath accepted")
	}
	if _, err := PrepareIncidentSession(root, "tenant", ""); err == nil {
		t.Fatal("empty incident identity accepted")
	}
	if err := RemoveIncidentSession(root, "incident-A"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, strings.TrimPrefix(first, "tenant/"))); !os.IsNotExist(err) {
		t.Fatalf("acknowledged incident session remains: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, strings.TrimPrefix(second, "tenant/"))); err != nil {
		t.Fatalf("cleanup removed another incident session: %v", err)
	}
}

func TestPrepareIncidentSessionRejectsRuntimeSymlink(t *testing.T) {
	root := t.TempDir()
	if err := os.Symlink(t.TempDir(), filepath.Join(root, ".sdo-runtime")); err != nil {
		t.Fatal(err)
	}
	if _, err := PrepareIncidentSession(root, "", "incident-A"); err == nil {
		t.Fatal("runtime symlink accepted")
	}
}
