package operatorcli

import (
	"context"
	"os"
	"path/filepath"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/install"
	"strings"
	"testing"
)

func TestShellHealthRefreshRequiresShellDetector(t *testing.T) {
	root := t.TempDir()
	cfg := &contracts.ControllerConfig{AppRoot: root, CodePath: &contracts.CodePathConfig{Name: "shell"}}
	need, err := shellHealthRefreshNeeded(context.Background(), cfg)
	if err != nil || !need {
		t.Fatalf("missing shell health must refresh: %v %v", need, err)
	}
	path := filepath.Join(root, ".sdo/diagnostics/detectors/health/ready/check.sh")
	if err = os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, []byte("exit 0\n"), 0755); err != nil {
		t.Fatal(err)
	}
	need, err = shellHealthRefreshNeeded(context.Background(), cfg)
	if err != nil || need {
		t.Fatalf("existing shell health: %v %v", need, err)
	}
}
func TestShellInstallExistingForwardsTypedSelection(t *testing.T) {
	args := []string{"install-existing", "--repository", "/accepted/app", "--namespace", "demo", "--application", "demo", "--controller-image", "controller:test", "--responder-image", "responder:test", "--validator-image", "validator:test", "--repository-pvc", "repository", "--credentials-secret", "credentials", "--model", "model", "--code-path", "shell"}
	launch, err := ParseInstallExistingLaunch(args)
	if err != nil {
		t.Fatal(err)
	}
	if launch.Install.CodePath.GetName() != "shell" {
		t.Fatal("selection dropped")
	}
	if got := codePathWorkerEnvironment(launch.Install.CodePath, nil); got["SDO_AGENT_GUIDANCE"] != "shell" || got["SDO_MEMORY_VALIDATION"] != "shell" {
		t.Fatal(got)
	}
	calls := 0
	validator := selectMemoryValidator(launch.Install.CodePath, func(context.Context, string, []string) error { calls++; return nil })
	if err := validator(context.Background(), t.TempDir(), nil); err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatal("shell launch ran Go validation")
	}
}

func TestShellAdoptionUsesHygieneWithoutGoWorkspace(t *testing.T) {
	root := t.TempDir()
	gitIn(t, root, "init", "-q")
	gitIn(t, root, "config", "user.name", "T")
	gitIn(t, root, "config", "user.email", "t@example.com")
	gitIn(t, root, "commit", "--allow-empty", "-qm", "source")
	path := filepath.Join(root, ".sdo/diagnostics/detectors/health/ready/check.sh")
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("exit 0\n"), 0755); err != nil {
		t.Fatal(err)
	}
	launch := InstallExistingConfig{Repository: root, OperatorRoot: "/not-a-Go-workspace", AdoptExistingMemory: true, Install: install.Config{CodePath: &contracts.CodePathConfig{Name: "shell"}}}
	commit, err := prepareInstallExistingRepository(context.Background(), launch)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(gitIn(t, root, "show", "-s", "--format=%B", commit), "SDO-Validation: skipped") {
		t.Fatal("adoption invented validation provenance")
	}
}
