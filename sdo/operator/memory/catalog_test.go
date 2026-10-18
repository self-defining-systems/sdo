package memory

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/memory/layout"
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
	const goal = "---\nschema_version: 1\nowner: human\n---\n# Goal\n"
	write(".sdo/schema-version", "2\n")
	write(".sdo/goal.md", goal)
	write(".sdo/arch.md", "---\nschema_version: 1\n---\n# Architecture\n")
	write(".sdo/playbooks/README.md", "# Index\n")
	write(".sdo/playbooks/cache-pressure/README.md", "---\nschema_version: 1\nowner: responder\nfault_class: cache-pressure\noriginating_incident: i\n---\n# Fix cache with .sdo/playbooks/cache-pressure/scripts/diagnose.sh\n")
	write(".sdo/playbooks/cache-pressure/scripts/diagnose.sh", "#!/bin/sh\nexit 0\n")
	err := layout.WriteDetector(root, layout.Detector{Class: layout.ClassIncident, Files: []layout.File{{Name: "detector.go", Content: []byte("package cache\n// .sdo/playbooks/cache-pressure/README.md\nfunc (d Detector) Detect() {}\n")}}, Manifest: layout.DetectorManifest{
		APIVersion: layout.APIVersion, Kind: layout.DetectorManifestKind, SDKVersion: layout.SDKVersion,
		ID: "cache-pressure", Constructor: "New", Class: "incident", Owner: "responder",
		Watches: []layout.Watch{{APIVersion: "v1", Kind: "Pod"}}, Interval: "10s",
		Persistence: layout.Persistence{Firing: 2, Clearing: 2}, Batching: layout.Batching{Severity: "warn", Debounce: "0"},
		PossiblePlaybooks: []string{"cache-pressure"}, OriginatingIncident: "first-incident", OriginatingCommit: "c1",
	}})
	if err != nil {
		t.Fatal(err)
	}
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
	if len(snapshot.GetAcceptedCommit()) != 40 || snapshot.GetGoalMarkdown() != goal || len(snapshot.GetPlaybooks()) != 1 || snapshot.GetPlaybooks()[0].GetId() != "cache-pressure" || len(snapshot.GetDetectors()) != 1 || snapshot.GetDetectors()[0].GetPossiblePlaybookIds()[0] != "cache-pressure" {
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

func TestCatalogSnapshotRejectsUnmigratedMemory(t *testing.T) {
	root := t.TempDir()
	for name, content := range map[string]string{
		".sdo/schema-version":            "1\n",
		".sdo/goal.md":                   "---\nschema_version: 1\n---\n# Goal\n",
		".sdo/diagnostics/manifest.yaml": "detectors: []\n",
	} {
		path := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for _, args := range [][]string{{"init", "-q"}, {"add", "."}, {"-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "base"}} {
		command := exec.Command("git", args...)
		command.Dir = root
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
	}
	if _, err := OpenCatalogReader(root).Snapshot(context.Background()); err == nil || !strings.Contains(err.Error(), "schema-version") {
		t.Fatalf("schema-version 1 repository entered worker snapshot: %v", err)
	}
}
