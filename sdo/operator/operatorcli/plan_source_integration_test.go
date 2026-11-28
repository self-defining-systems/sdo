//go:build integration

package operatorcli

import (
	"errors"
	"google.golang.org/protobuf/types/known/anypb"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/planfile"
	"sdo.dev/operator/planfile/planfiletest"
)

func planRepo(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	testutil.InitGit(t, dir)
	if err := os.WriteFile(filepath.Join(dir, "app.txt"), []byte("app\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	testutil.Git(t, dir, "add", ".")
	testutil.Git(t, dir, "commit", "-qm", "seed")
	return dir
}

func headOf(t *testing.T, dir string) string {
	t.Helper()
	return strings.TrimSpace(testutil.Git(t, dir, "rev-parse", "HEAD"))
}

// install-existing reads the override the accepted tree carries; without one
// the source is derived from the files (nil), never refused.
func TestInstallExistingReadsTheOptionalOverride(t *testing.T) {
	repo := planRepo(t)
	if got, err := installDeploymentSource(InstallExistingConfig{Repository: repo}); err != nil || got != nil {
		t.Fatalf("no override = %v, %v", got, err)
	}
	config, err := anypb.New(&contracts.KustomizeRenderConfig{Path: "kubernetes"})
	if err != nil {
		t.Fatal(err)
	}
	source := &contracts.DeploymentSource{RendererId: "kustomize", Config: config}
	planfiletest.Write(t, repo, &contracts.DeploymentPlan{Source: source})
	got, err := installDeploymentSource(InstallExistingConfig{Repository: repo})
	if err != nil || !proto.Equal(got, source) {
		t.Fatalf("override source = %v, %v", got, err)
	}
	writeOverrideText(t, repo, "colour: red\n")
	if _, err := installDeploymentSource(InstallExistingConfig{Repository: repo}); err == nil || errors.Is(err, planfile.ErrAbsent) {
		t.Fatalf("a bad override must be refused: %v", err)
	}
}

func writeOverrideText(t *testing.T, repo, text string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(repo, ".sdo", "deployment", "plan.yaml"), []byte(text), 0o644); err != nil {
		t.Fatal(err)
	}
}
