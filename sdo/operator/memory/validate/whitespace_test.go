package validate

import (
	"context"
	"testing"
)

// A captured patch is a diff file: its context lines start with a space and then the source's own indent
// (a tab in Go), and a blank source line is a lone space. Judging those bytes as authored text rejected
// every patch of a tab-indented file with diff.whitespace.
func TestWhitespaceDoesNotJudgeCapturedPatchBytes(t *testing.T) {
	root := t.TempDir()
	commit(t, root, "base")
	patch := "SDO-Patch: 1\nOwner: responder\nIncident: inc-1\nReason: " + gatePatchReason + "\n\n" +
		"diff --git a/main.go b/main.go\n--- a/main.go\n+++ b/main.go\n@@ -1,3 +1,4 @@\n \tkeep()\n \n+\tadded()\n \tdone()\n"
	write(t, root, gatePatchDir+"0001-x.patch", patch)

	ds, err := checkWhitespace(context.Background(), Request{Root: root, Base: "HEAD"})
	if err != nil || len(ds) != 0 {
		t.Fatalf("patch bytes judged: %v, %v", ds, err)
	}

	write(t, root, gateReadme, gateReadmeText("Recovery guidance. \n"))
	ds, err = checkWhitespace(context.Background(), Request{Root: root, Base: "HEAD"})
	if err != nil || len(ds) == 0 {
		t.Fatalf("authored text with trailing whitespace must still be reported: %v, %v", ds, err)
	}
}

func TestSecretPathScopeIsMemoryOnly(t *testing.T) {
	for path, want := range map[string]bool{
		"chart/templates/shared-secrets/job.yaml": false,
		".git/config": false,
		".sdo/diagnostics/traffic/secret-token.go":    true,
		".sdo/playbooks/base/.env":                    true,
		".sdo/deployment/chart/templates/secrets/a.y": false,
		".sdo/deployment/.git/config":                 true,
	} {
		if _, got := secretPathPart(path); got != want {
			t.Errorf("secretPathPart(%q) = %v, want %v", path, got, want)
		}
	}
}
