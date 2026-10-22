package apihistory

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCredentialCapableFieldsAreMetadataOnly(t *testing.T) {
	const canary = "KP2B_SYNTHETIC_CREDENTIAL_CANARY"
	for _, raw := range []string{
		`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"c","annotations":{"auth":"` + canary + `"},"labels":{"free":"` + canary + `"}},"data":{"certificate":"` + canary + `"},"binaryData":{"key":"` + canary + `"}}`,
		`{"apiVersion":"v1","kind":"Pod","metadata":{"name":"p"},"spec":{"containers":[{"name":"c","command":["` + canary + `"],"args":["` + canary + `"],"env":[{"name":"MODE","value":"` + canary + `"}]}]},"status":{"message":"` + canary + `"}}`,
		`{"apiVersion":"v1","kind":"Secret","metadata":{"annotations":{"auth":"` + canary + `"}},"data":{"x":"` + canary + `"},"stringData":{"x":"` + canary + `"}}`,
		`{"apiVersion":"v1","kind":"Event","message":"` + canary + `","reason":"` + canary + `"}`,
		`{"apiVersion":"v1","kind":"PodList","metadata":{"continue":"` + canary + `"},"items":[{"metadata":{"name":"p","annotations":{"auth":"` + canary + `"}},"status":{"message":"` + canary + `"}}]}`,
		"Authorization: Bearer " + canary,
		strings.Repeat("x", MaxLogBytes) + canary,
	} {
		t.Run(raw[:min(40, len(raw))], func(t *testing.T) {
			dir := t.TempDir()
			s, err := Open(dir, "synthetic", Limits{})
			if err != nil {
				t.Fatal(err)
			}
			event := Event{Phase: "end", Verb: "get"}
			if !strings.HasPrefix(raw, "{") {
				event.BodyFormat = "log_tail"
			}
			s.Record(event, []byte(raw))
			s.Close()
			h, err := Read(dir)
			if err != nil {
				t.Fatal(err)
			}
			if len(h.Events) != 1 || h.Events[0].Omission == "" {
				t.Errorf("redaction marker missing: %+v", h)
			}
			err = filepath.WalkDir(dir, func(p string, d os.DirEntry, e error) error {
				if e != nil {
					return e
				}
				if d.IsDir() {
					return nil
				}
				b, e := os.ReadFile(p)
				if bytes.Contains(b, []byte(canary)) {
					t.Errorf("credential in %s", p)
				}
				return e
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestFailedReopenInvalidatesClosedGeneration(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, "synthetic", Limits{})
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	if err := os.Remove(filepath.Join(dir, "blobs")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "blobs"), []byte("broken"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, "synthetic", Limits{}); err == nil {
		t.Fatal("expected failure")
	}
	h, err := Read(dir)
	if err != nil {
		t.Fatal(err)
	}
	if h.Manifest.Complete || h.Manifest.Dropped == 0 {
		t.Fatalf("stale generation accepted: %+v", h.Manifest)
	}
}

func TestWriterLivenessInvalidatesOldManifestWithoutAnyRecordingWrites(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, "synthetic", Limits{})
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	lease, err := BeginWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	h, err := Read(dir)
	if err != nil {
		t.Fatal(err)
	}
	if h.Manifest.Complete || h.Manifest.Dropped == 0 {
		t.Fatalf("closed prior generation accepted: %+v", h.Manifest)
	}
	lease.Close()
}

func TestFailedGenerationRemainsIncompleteAfterWriterExit(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, "synthetic", Limits{})
	if err != nil {
		t.Fatal(err)
	}
	store.Close()
	if err := os.Mkdir(filepath.Join(dir, "disabled.tmp"), 0700); err != nil {
		t.Fatal(err)
	}
	lease, err := BeginWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, "synthetic", Limits{}); err == nil {
		t.Fatal("expected marker failure")
	}
	lease.Close()
	after, err := Read(dir)
	if err != nil {
		t.Fatal(err)
	}
	if after.Manifest.Complete || after.Manifest.Dropped == 0 {
		t.Fatalf("failed generation accepted after exit: %+v", after.Manifest)
	}
}

func TestTrustedAttemptInvalidatesHistoryWhenRecorderDirectoryIsUnavailable(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "history")
	store, err := Open(dir, "synthetic", Limits{})
	if err != nil {
		t.Fatal(err)
	}
	store.Close()
	// This boundary is written by the controller before launching the sidecar,
	// independently of whether the sidecar can write its mounted history directory.
	if err := BeginGeneration(dir); err != nil {
		t.Fatal(err)
	}
	after, err := Read(dir)
	if err != nil {
		t.Fatal(err)
	}
	if after.Manifest.Complete || after.Manifest.Dropped == 0 {
		t.Fatal("old manifest certified a new attempt")
	}
}
