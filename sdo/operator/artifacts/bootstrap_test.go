package artifacts

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBootstrapStoreWritesOnlyRoleOwnedArtifacts(t *testing.T) {
	checkout := t.TempDir()
	store := BootstrapStore{}
	if err := store.WriteGoal(checkout, "demo: app", "pods remain healthy"); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteSchema(checkout); err != nil {
		t.Fatal(err)
	}
	goal, err := os.ReadFile(filepath.Join(checkout, ".sdo", "goal.md"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(goal), "owner: human") || !strings.Contains(string(goal), "pods remain healthy") {
		t.Fatalf("invalid goal: %s", goal)
	}
	version, err := os.ReadFile(filepath.Join(checkout, ".sdo", "schema-version"))
	if err != nil {
		t.Fatal(err)
	}
	if string(version) != "3\n" {
		t.Fatalf("invalid schema version: %q", version)
	}
	entries, err := os.ReadDir(filepath.Join(checkout, ".sdo"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 {
		t.Fatalf("bootstrap wrote another role's files: %#v", entries)
	}
	if _, err := os.Stat(filepath.Join(checkout, ".sdo", "diagnostics", "go.mod")); err != nil {
		t.Fatal(err)
	}
}

func TestArchitectureCitesAcceptedSourceCommit(t *testing.T) {
	checkout := t.TempDir()
	store := BootstrapStore{}
	if err := store.WriteArchitecture(checkout, "demo", "accepted-source-sha", "fingerprint", "# Architecture\nSource facts."); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(filepath.Join(checkout, ".sdo", "arch.md"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(content), "generated_at_commit: accepted-source-sha") {
		t.Fatalf("architecture lost canonical provenance: %s", content)
	}
	if strings.Contains(string(content), "proposal-local") {
		t.Fatalf("architecture cited proposal commit: %s", content)
	}
}

func TestArchitectureTaskNeverExposesPhysicalMemoryPaths(t *testing.T) {
	checkout := t.TempDir()
	store := BootstrapStore{}
	if err := store.WriteArchitecture(checkout, "demo", "accepted-source-sha", "fingerprint", "Read .sdo/diagnostics/manifest.yaml"); err == nil {
		t.Fatal("architecture write accepted a physical operational-memory path")
	}
	if err := store.WriteArchitecture(checkout, "demo", "accepted-source-sha", "fingerprint", "# Architecture\nNo path"); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(checkout, ".sdo", "arch.md")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(data, []byte("See .sdo/playbooks/README.md\n")...), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReadArchitecture(checkout); err == nil {
		t.Fatal("architecture task exposed a physical operational-memory path")
	}
}
