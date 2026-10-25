package memory

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestPlaybookActivationChangesOnlyOnLifecycleTransition(t *testing.T) {
	root := t.TempDir()
	gitCommand(t, root, "init", "-q")
	p := ".sdo/playbooks/repair/README.md"
	if err := os.MkdirAll(filepath.Dir(filepath.Join(root, p)), 0755); err != nil {
		t.Fatal(err)
	}
	commit := func(fields, body string) string {
		t.Helper()
		if err := os.WriteFile(filepath.Join(root, p), []byte("---\nschema_version: 1\n"+fields+"---\n"+body), 0644); err != nil {
			t.Fatal(err)
		}
		gitCommand(t, root, "add", ".")
		gitCommand(t, root, "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "memory")
		data, err := exec.Command("git", "-C", root, "rev-parse", "HEAD").Output()
		if err != nil {
			t.Fatal(err)
		}
		return strings.TrimSpace(string(data))
	}
	active := commit("", "Active <RESOURCE>.\n")
	prose := commit("", "Changed prose <RESOURCE>.\n")
	retired := commit("status: retired\nretired_reason: failed\n", "Changed prose <RESOURCE>.\n")
	reactivated := commit("status: active\n", "Changed prose <RESOURCE>.\n")
	for _, tc := range []struct {
		before, after string
		want          bool
	}{{active, prose, false}, {prose, retired, true}, {retired, reactivated, true}, {retired, retired, false}} {
		got, err := PlaybookActivationChanged(context.Background(), root, tc.before, tc.after, []string{p})
		if err != nil || got != tc.want {
			t.Fatalf("activation change=%v want=%v: %v", got, tc.want, err)
		}
	}
}
