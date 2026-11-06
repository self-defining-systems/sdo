//go:build integration

package operatorcli

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/planfile"
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

// The flag's source becomes plan.yaml in a human commit; installing again is
// idempotent, and a repository that already carries a plan needs no flag.
func TestInstallExistingRecordsTheSourceAsThePlan(t *testing.T) {
	ctx := context.Background()
	repo := planRepo(t)
	source, err := parseDeploymentSourceFlag(kustomizeSourceJSON)
	if err != nil {
		t.Fatal(err)
	}
	got, err := recordInstallDeploymentSource(ctx, InstallExistingConfig{Repository: repo, DeploymentSource: source})
	if err != nil {
		t.Fatal(err)
	}
	wantKustomizeSource(t, got)
	plan, err := planfile.Read(repo, planCatalogs())
	if err != nil {
		t.Fatal(err)
	}
	wantKustomizeSource(t, plan.GetSource())
	message := testutil.Git(t, repo, "log", "-1", "--format=%B")
	if !strings.Contains(message, "SDO-Actor: human") || testutil.Git(t, repo, "status", "--porcelain") != "" {
		t.Fatalf("plan commit %q or dirty tree", message)
	}
	head := headOf(t, repo)
	if _, err := recordInstallDeploymentSource(ctx, InstallExistingConfig{Repository: repo, DeploymentSource: source}); err != nil || headOf(t, repo) != head {
		t.Fatalf("second install committed again (%v)", err)
	}
	fromTree, err := recordInstallDeploymentSource(ctx, InstallExistingConfig{Repository: repo})
	if err != nil {
		t.Fatal(err)
	}
	wantKustomizeSource(t, fromTree)
}

func TestInstallExistingWithoutAPlanOrFlagFailsBeforeAnyClusterCall(t *testing.T) {
	_, err := recordInstallDeploymentSource(context.Background(), InstallExistingConfig{Repository: planRepo(t)})
	if !errors.Is(err, planfile.ErrAbsent) {
		t.Fatalf("err = %v", err)
	}
}

// Recording a source replaces only the source: image declarations and the
// expected workloads of the plan in the tree stay.
func TestRecordingASourceKeepsTheRestOfThePlan(t *testing.T) {
	ctx := context.Background()
	repo := planRepo(t)
	source, err := parseDeploymentSourceFlag(kustomizeSourceJSON)
	if err != nil {
		t.Fatal(err)
	}
	if err := planfile.Write(repo, &contracts.DeploymentPlan{Source: source, ExpectedWorkloadIds: []string{"Deployment/shop/web"}}); err != nil {
		t.Fatal(err)
	}
	testutil.Git(t, repo, "add", ".")
	testutil.Git(t, repo, "commit", "-qm", "plan")
	other, err := parseDeploymentSourceFlag(strings.Replace(kustomizeSourceJSON, `"kubernetes"`, `"overlays/prod"`, 1))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := recordInstallDeploymentSource(ctx, InstallExistingConfig{Repository: repo, DeploymentSource: other}); err != nil {
		t.Fatal(err)
	}
	plan, err := planfile.Read(repo, planCatalogs())
	if err != nil || !proto.Equal(plan.GetSource(), other) || len(plan.GetExpectedWorkloadIds()) != 1 {
		t.Fatalf("plan = %v, %v", plan, err)
	}
}
