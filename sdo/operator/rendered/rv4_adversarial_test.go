package rendered

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestRV4InputRelevance(t *testing.T) {
	root := t.TempDir()
	git := func(args ...string) string {
		t.Helper()
		data, err := exec.Command("git", append([]string{"-C", root}, args...)...).CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v %s", args, err, data)
		}
		return strings.TrimSpace(string(data))
	}
	git("init", "-q")
	git("config", "user.name", "Test")
	git("config", "user.email", "test@example.com")
	if err := os.WriteFile(filepath.Join(root, "app.yaml"), []byte("apiVersion: v1\nkind: Service\nmetadata: {name: api}\nspec: {ports: [{port: 80}]}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	git("add", ".")
	git("commit", "-qm", "base")
	base := git("rev-parse", "HEAD")
	source := Source{Repository: root, ManifestPaths: []string{"app.yaml"}}
	for _, p := range []string{".sdo/goal.md", ".sdo/playbooks/p/README.md", ".sdo/rendered/lock.json", "deploy/kustomization.yaml", "chart/values.yaml", ".env", "image-overrides.json", "Dockerfile", "README.md"} {
		t.Run(p, func(t *testing.T) {
			if err := os.MkdirAll(filepath.Dir(filepath.Join(root, p)), 0755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, p), []byte("changed\n"), 0600); err != nil {
				t.Fatal(err)
			}
			git("add", p)
			git("commit", "-qm", "input "+p)
			next := git("rev-parse", "HEAD")
			same, err := source.SameInputs(context.Background(), base, next)
			if err != nil {
				t.Fatal(err)
			}
			want := strings.HasPrefix(p, ".sdo/")
			if same != want {
				t.Fatalf("SameInputs(%s)=%t want %t", p, same, want)
			}
			t.Logf("input=%s relevant=%t", p, !same)
			base = next
		})
	}
}
