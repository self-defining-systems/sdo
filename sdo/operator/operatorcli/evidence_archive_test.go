package operatorcli

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The exported archive holds no credential value an agent printed into its transcript.
func TestEvidenceArchiveRedactsCredentials(t *testing.T) {
	root, mounted := t.TempDir(), t.TempDir()
	secret := "synthetic-refresh-" + strings.Repeat("r", 24)
	login := `{"refresh_token":"` + secret + `"}`
	name := "sessions/s/codex/sessions/2026/rollout-a.jsonl"
	for path, text := range map[string]string{filepath.Join(mounted, "auth.json"): login, filepath.Join(root, name): "before " + secret + " after\n"} {
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	var out bytes.Buffer
	if err := RunEvidenceArchive(root, []string{mounted}, false, strings.NewReader(name+"\ngone\n"), &out); err == nil {
		t.Fatal("a file missing from a sealed archive must fail it")
	}
	out.Reset()
	if err := RunEvidenceArchive(root, []string{mounted}, true, strings.NewReader(name+"\ngone\n"), &out); err != nil {
		t.Fatal(err)
	}
	destination := t.TempDir()
	if err := extractRuntimeArtifacts(&out, destination); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(destination, name))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "before [redacted] after\n" {
		t.Fatal("the archived transcript is not the redacted one")
	}
}
