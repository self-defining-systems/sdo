package runtime

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/sdk"
)

func TestGitPlanSourceReadsManifestAtAcceptedCommit(t *testing.T) {
	repo := t.TempDir()
	for _, args := range [][]string{{"init"}, {"config", "user.name", "Test"}, {"config", "user.email", "test@example.com"}} {
		if output, err := exec.Command("git", append([]string{"-C", repo}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %s: %v", args, output, err)
		}
	}
	path := filepath.Join(repo, "app.yaml")
	if err := os.WriteFile(path, []byte("apiVersion: v1\nkind: Service\nmetadata:\n  name: api\n  namespace: app\nspec:\n  ports:\n  - port: 80\n"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"add", "app.yaml"}, {"commit", "-m", "fixture"}} {
		if output, err := exec.Command("git", append([]string{"-C", repo}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %s: %v", args, output, err)
		}
	}
	commit, err := exec.Command("git", "-C", repo, "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("broken"), 0600); err != nil {
		t.Fatal(err)
	}
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	object, err := (GitPlanSource{Repository: repo}).AcceptedManifest(context.Background(), strings.TrimSpace(string(commit)), ref)
	if err != nil || object.GetName() != "api" {
		t.Fatalf("accepted manifest: %+v %v", object, err)
	}
}
