package operatorcli

import (
	"archive/tar"
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRuntimeArtifactsNeverRecordCredentialsOrCaches(t *testing.T) {
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	files := []string{
		"sessions/id/codex/auth.json",
		"sessions/id/codex/cache/remote_plugin_catalog/catalog.json",
		"sessions/id/claude/credentials.json",
		"sessions/id/model_io/token.json",
		"codex/sessions/auth.json",
		"claude/projects/credential.jsonl",
		"sessions/id/claude/projects/app/cache/session.jsonl",
		"sessions/id/codex/sessions/2026/10/21/rollout-a.jsonl",
		"sessions/id/model_io/request/response.json",
		"sessions/id/checkpoints/request/request.json",
		"sessions/id/checkpoints/request/result.json",
		"sessions/id/checkpoints/request/file_edits.json",
		"sessions/id/session.jsonl",
		"sessions/id/token_usage.json",
		"claude/projects/project/session-a.jsonl",
		"codex/sessions/2026/10/21/rollout-a.jsonl",
	}
	for _, name := range files {
		if err := writer.WriteHeader(&tar.Header{Name: name, Mode: 0600, Size: 1}); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write([]byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	if err := extractRuntimeArtifacts(&archive, root); err != nil {
		t.Fatal(err)
	}
	for _, name := range files[7:] {
		if _, err := os.Stat(filepath.Join(root, name)); err != nil {
			t.Fatalf("missing allowed evidence %s: %v", name, err)
		}
	}
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		name := strings.ToLower(entry.Name())
		if name != "token_usage.json" && (strings.Contains(name, "auth") || strings.Contains(name, "credential") || strings.Contains(name, "token")) {
			t.Errorf("secret-like file recorded: %s", path)
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if strings.Contains(relative, "/cache/") || strings.Contains(relative, "/plugin/") {
			t.Errorf("cache recorded: %s", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

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
	if err := writer.WriteHeader(&tar.Header{Name: "sessions/8d746cb1/codex/sessions/rollout-trace.jsonl", Mode: 0600, Size: int64(len(trace))}); err != nil {
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
	got, err = os.ReadFile(filepath.Join(root, "sessions", "8d746cb1", "codex", "sessions", "rollout-trace.jsonl"))
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

func TestRuntimeArtifactsCopyAllowedCorpusHardlinksAndContinue(t *testing.T) {
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	original := "sessions/drift/corpus/open@detect.json"
	linked := "sessions/incident/corpus/open@detect.json"
	for _, header := range []*tar.Header{
		{Name: original, Mode: 0600, Size: 2, Typeflag: tar.TypeReg},
		{Name: linked, Linkname: original, Typeflag: tar.TypeLink},
		{Name: "sessions/incident/model_io/request/turn.json", Mode: 0600, Size: 2, Typeflag: tar.TypeReg},
	} {
		if err := writer.WriteHeader(header); err != nil {
			t.Fatal(err)
		}
		if header.Size > 0 {
			if _, err := writer.Write([]byte("{}")); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	if err := extractRuntimeArtifacts(&archive, root); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{original, linked, "sessions/incident/model_io/request/turn.json"} {
		data, err := os.ReadFile(filepath.Join(root, name))
		if err != nil || string(data) != "{}" {
			t.Fatalf("missing evidence %s: %s, %v", name, data, err)
		}
	}
}

func TestRuntimeArtifactsRejectHardlinksToCredentials(t *testing.T) {
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	if err := writer.WriteHeader(&tar.Header{Name: "sessions/id/corpus/open.json", Typeflag: tar.TypeLink, Linkname: "sessions/id/codex/auth.json"}); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := extractRuntimeArtifacts(&archive, t.TempDir()); err == nil {
		t.Fatal("credential hardlink accepted")
	}
}
