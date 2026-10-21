package sdktest

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCorpusProvenanceChecksCapturedDigests(t *testing.T) {
	dir := t.TempDir()
	open := []byte(`{"namespace":"app"}`)
	fixed := []byte(`{"namespace":"app","pods":[]}`)
	for name, data := range map[string][]byte{"open.json": open, "fixed.json": fixed, "healthy.json": fixed, "recordings.json": []byte(`[]`)} {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	labels := fmt.Sprintf(`{"root_cause_program_ids":["fault"],"provenance":{"incident_id":"incident","open_sha256":"%x","fixed_sha256":"%x"}}`, sha256.Sum256(open), sha256.Sum256(fixed))
	if err := os.WriteFile(filepath.Join(dir, "labels.json"), []byte(labels), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadCorpusCase(dir); err != nil {
		t.Fatal(err)
	}
	if err := ValidateCorpusProvenance(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "open.json"), []byte(`{"namespace":"forged"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := ValidateCorpusProvenance(dir); err == nil || !strings.Contains(err.Error(), "digest") {
		t.Fatalf("forged snapshot accepted: %v", err)
	}
}
