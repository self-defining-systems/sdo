package operatorcli

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/evidencesink"
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
	if err := RunEvidenceArchive(root, []string{mounted}, nil, false, strings.NewReader(name+"\ngone\n"), &out); err == nil {
		t.Fatal("a file missing from a sealed archive must fail it")
	}
	out.Reset()
	if err := RunEvidenceArchive(root, []string{mounted}, nil, true, strings.NewReader(name+"\ngone\n"), &out); err != nil {
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

// Without the credential mount the archive keeps every other file, leaves transcripts out and says so.
func TestEvidenceArchiveWithoutCredentialsWithholdsTranscripts(t *testing.T) {
	root := t.TempDir()
	transcript, kept := "sessions/s/claude/projects/p/a.jsonl", "usage/u.jsonl"
	for _, name := range []string{transcript, kept} {
		if err := os.MkdirAll(filepath.Dir(filepath.Join(root, name)), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, name), []byte("{}\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	var out bytes.Buffer
	if err := RunEvidenceArchive(root, []string{filepath.Join(root, "absent")}, nil, false, strings.NewReader(transcript+"\n"+kept+"\n"), &out); err != nil {
		t.Fatal(err)
	}
	destination := t.TempDir()
	if err := extractRuntimeArtifacts(&out, destination); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(destination, transcript)); !os.IsNotExist(err) {
		t.Fatalf("transcript exported without the credentials: %v", err)
	}
	if _, err := os.Stat(filepath.Join(destination, kept)); err != nil {
		t.Fatal(err)
	}
	gaps, err := os.ReadFile(filepath.Join(destination, evidencesink.GapFile))
	if err != nil || !strings.Contains(string(gaps), transcript) {
		t.Fatalf("gap not recorded: %v", err)
	}
}
