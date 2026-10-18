package operatorcli

import (
	"archive/tar"
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestRuntimeArtifactsRejectsUnsafeTarEntry(t *testing.T) {
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	if err := writer.WriteHeader(&tar.Header{Name: "../escape", Mode: 0600, Size: 3}); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("bad")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	if err := extractRuntimeArtifacts(&archive, root); err == nil {
		t.Fatal("tar path traversal accepted")
	}
	if _, err := os.Stat(filepath.Join(root, "..", "escape")); !os.IsNotExist(err) {
		t.Fatalf("unsafe file created: %v", err)
	}
}

func TestRuntimeArtifactsExtractsOnlyWhitelistedEvidence(t *testing.T) {
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	content := []byte("one detector fired\n")
	if err := writer.WriteHeader(&tar.Header{Name: "telemetry/detector-firings.jsonl", Mode: 0600, Size: int64(len(content))}); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write(content); err != nil {
		t.Fatal(err)
	}
	trace := []byte("provider session trace\n")
	if err := writer.WriteHeader(&tar.Header{Name: "sessions/8d746cb1/codex/sessions/trace.jsonl", Mode: 0600, Size: int64(len(trace))}); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write(trace); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	if err := extractRuntimeArtifacts(&archive, root); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(root, "telemetry", "detector-firings.jsonl"))
	if err != nil || !bytes.Equal(got, content) {
		t.Fatalf("runtime telemetry lost: %q %v", got, err)
	}
	got, err = os.ReadFile(filepath.Join(root, "sessions", "8d746cb1", "codex", "sessions", "trace.jsonl"))
	if err != nil || !bytes.Equal(got, trace) {
		t.Fatalf("private incident session evidence lost: %q %v", got, err)
	}
}

func TestParseRuntimeArtifactsRequiresOutputDirectory(t *testing.T) {
	cfg, err := ParseRuntimeArtifactsLaunch([]string{"runtime-artifacts", "--control-namespace", "sdo", "--repository-pvc", "repo", "--controller-image", "controller:test", "--output-dir", "/tmp/results"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.OutputDir != "/tmp/results" || cfg.Sync.ControlNamespace != "sdo" {
		t.Fatalf("lost export config: %+v", cfg)
	}
	if _, err := ParseRuntimeArtifactsLaunch([]string{"runtime-artifacts", "--control-namespace", "sdo", "--repository-pvc", "repo", "--controller-image", "controller:test", "--output-dir", "relative"}); err == nil {
		t.Fatal("relative output accepted")
	}
}
