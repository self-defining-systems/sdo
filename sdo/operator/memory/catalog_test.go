package memory

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestCatalogSnapshotExposesLogicalPlaybooksAndAcceptedCommit(t *testing.T) {
	root := t.TempDir()
	write := func(name, content string) {
		path := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write(".sdo/schema-version", "1\n")
	write(".sdo/goal.md", "# Goal\n")
	write(".sdo/arch.md", "# Architecture\n")
	write(".sdo/playbooks/README.md", "# Index\n")
	write(".sdo/playbooks/cache-pressure/README.md", "# Fix cache with .sdo/playbooks/cache-pressure/scripts/diagnose.sh\n")
	write(".sdo/playbooks/cache-pressure/scripts/diagnose.sh", "#!/bin/sh\nexit 0\n")
	write(".sdo/diagnostics/detectors/incidents/cache/detector.go", "package cache\n// .sdo/playbooks/cache-pressure/README.md\nfunc (d Detector) Detect() {}\n")
	write(".sdo/diagnostics/manifest.yaml", `apiVersion: sdo.dev/v1alpha1
kind: DetectorManifest
sdkVersion: sdo.dev/v1alpha1
detectors:
  - id: cache-pressure
    package: detectors/incidents/cache
    class: incident
    owner: responder
    watches:
      - apiVersion: v1
        kind: Pod
    persistence:
      firing: 2
    possiblePlaybooks:
      - .sdo/playbooks/cache-pressure/README.md
    originatingIncident: first-incident
`)
	for _, args := range [][]string{{"init", "-q"}, {"add", "."}, {"-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "base"}} {
		command := exec.Command("git", args...)
		command.Dir = root
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
	}
	snapshot, err := OpenCatalogReader(root).Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.GetAcceptedCommit()) != 40 || snapshot.GetGoalMarkdown() != "# Goal\n" || len(snapshot.GetPlaybooks()) != 1 || snapshot.GetPlaybooks()[0].GetId() != "cache-pressure" || len(snapshot.GetDetectors()) != 1 || snapshot.GetDetectors()[0].GetPossiblePlaybookIds()[0] != "cache-pressure" {
		t.Fatalf("incomplete logical snapshot: %+v", snapshot)
	}
	if strings.Contains(snapshot.String(), ".sdo/") || strings.Contains(snapshot.GetPlaybookIndexMarkdown(), "README.md") {
		t.Fatal("physical playbook path leaked into worker snapshot")
	}
	write(".sdo/goal.md", "# unaccepted goal\n")
	if _, err := OpenCatalogReader(root).Snapshot(context.Background()); err == nil {
		t.Fatal("unaccepted operational-memory edit entered worker snapshot")
	}
}

func TestCatalogRejectsPhysicalPlaybookPathOutsideCatalog(t *testing.T) {
	if _, err := PlaybookID("../../outside"); err == nil {
		t.Fatal("unsafe playbook path accepted")
	}
	if _, err := PlaybookID(".sdo/playbooks/cache-pressure/README.md"); err != nil {
		t.Fatal(err)
	}
	if id, err := PlaybookID(".sdo/playbooks/cache-pressure.md"); err != nil || id != "cache-pressure" {
		t.Fatalf("legacy flat playbook reference was not projected: %q, %v", id, err)
	}
}
