package memory

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

// Only a full 40-hex accepted commit is evidence. Every other revision is
// ignored without touching the repository; a real commit in an unreadable
// repository is an error, never an empty answer.
func TestProgramLearningEvidenceRequiresAFullAcceptedCommit(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "no-repository")
	for _, commit := range []string{"", "HEAD", "main", strings.Repeat("a", 39), strings.Repeat("a", 41), strings.Repeat("A", 40), strings.Repeat("g", 40)} {
		evidence, err := ReadProgramLearningEvidence(context.Background(), missing, commit)
		if err != nil || evidence != nil {
			t.Errorf("commit %q: want no evidence and no error, got %v, %v", commit, evidence, err)
		}
	}
	if _, err := ReadProgramLearningEvidence(context.Background(), missing, strings.Repeat("a", 40)); err == nil {
		t.Fatal("a full accepted commit in an unreadable repository must fail, not look like no evidence")
	}
}
