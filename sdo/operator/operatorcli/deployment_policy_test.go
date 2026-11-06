package operatorcli

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"google.golang.org/protobuf/types/known/anypb"
	"k8s.io/client-go/kubernetes/fake"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/deployment"
	"sdo.dev/operator/deploymentpolicy"
	"sdo.dev/operator/internal/testutil"
)

const policyJSON = `{"allowedRegistries":["ghcr.io"],"scopeBlocking":true,"grantedNamespaces":["monitoring"],"grantedClusterKinds":["ClusterRole"]}`

func wantPolicy(t *testing.T, got *contracts.DeploymentPolicy) {
	t.Helper()
	if !slices.Equal(got.GetAllowedRegistries(), []string{"ghcr.io"}) || !got.GetScopeBlocking() ||
		!slices.Equal(got.GetGrantedNamespaces(), []string{"monitoring"}) || !slices.Equal(got.GetGrantedClusterKinds(), []string{"ClusterRole"}) {
		t.Fatalf("policy = %v", got)
	}
}

// The policy is install configuration: operator flags to the install Config,
// the Config to the operate Job and controller arguments, the arguments to the
// controller's launch config. It is never read from agent input.
func TestInstallPolicyFlagsReachTheInstallConfig(t *testing.T) {
	launch, err := ParseHostLaunch(hostArgs("--allow-registry", "ghcr.io", "--scope-blocking", "--grant-namespace", "monitoring", "--grant-cluster-kind", "ClusterRole"))
	if err != nil {
		t.Fatal(err)
	}
	policy, err := deploymentpolicy.Parse(launch.Install.DeploymentPolicy)
	if err != nil {
		t.Fatal(err)
	}
	wantPolicy(t, policy)
	plain, err := ParseHostLaunch(hostArgs())
	if err != nil || plain.Install.DeploymentPolicy != "" {
		t.Fatalf("no flags = %q, %v; want no policy (today's behavior)", plain.Install.DeploymentPolicy, err)
	}
	if _, err := ParseHostLaunch(hostArgs("--grant-namespace", "monitoring")); err == nil {
		t.Fatal("grants without --scope-blocking accepted")
	}
	existing, err := ParseInstallExistingLaunch(existingLaunchArgs("--allow-registry", "ghcr.io", "--scope-blocking", "--grant-namespace", "monitoring", "--grant-cluster-kind", "ClusterRole"))
	if err != nil {
		t.Fatal(err)
	}
	if existing.Install.DeploymentPolicy != launch.Install.DeploymentPolicy {
		t.Fatalf("install-existing policy = %q, want %q", existing.Install.DeploymentPolicy, launch.Install.DeploymentPolicy)
	}
}

func TestOperateAndControllerParseThePolicyFlag(t *testing.T) {
	operate, err := ParseOperateLaunch(operateArgs("--deployment-policy", policyJSON))
	if err != nil {
		t.Fatal(err)
	}
	policy, err := deploymentpolicy.Parse(operate.Install.DeploymentPolicy)
	if err != nil {
		t.Fatal(err)
	}
	wantPolicy(t, policy)
	controller, _, err := parseControllerLaunch(controllerLaunchArgs("--deployment-policy", policyJSON))
	if err != nil {
		t.Fatal(err)
	}
	wantPolicy(t, controller.Controller.GetDeploymentPolicy())
	plain, _, err := parseControllerLaunch(controllerLaunchArgs())
	if err != nil || plain.Controller.GetDeploymentPolicy() != nil {
		t.Fatalf("no flag = %v, %v; want none", plain.Controller.GetDeploymentPolicy(), err)
	}
	for _, bad := range []string{"{bad", `{"allowedRegistries":["GHCR.io"]}`, `{"grantedNamespaces":["x"]}`} {
		if _, err := ParseOperateLaunch(operateArgs("--deployment-policy", bad)); err == nil {
			t.Errorf("operate accepted %q", bad)
		}
		if _, _, err := parseControllerLaunch(controllerLaunchArgs("--deployment-policy", bad)); err == nil {
			t.Errorf("controller accepted %q", bad)
		}
	}
}

// The deployer both production paths build (the operate Job and the
// controller's upstream advance) refuses an off-allowlist image when the
// install policy sets an allowlist, before any build or apply: the Applier is
// the real kubectl backend, so reaching it would fail differently.
func TestProductionDeployersRefuseOffAllowlistImages(t *testing.T) {
	config := operateTestConfig("registry.local:5000")
	mount := t.TempDir()
	config.RepositoryMount, config.WorktreeRoot = mount, filepath.Join(mount, "worktrees")
	repo := filepath.Join(mount, "repo")
	if err := os.MkdirAll(filepath.Join(repo, "k8s"), 0o755); err != nil {
		t.Fatal(err)
	}
	manifest := "apiVersion: apps/v1\nkind: Deployment\nmetadata: {name: web}\nspec: {template: {spec: {containers: [{name: c, image: 'registry.synthetic.invalid/web:1'}]}}}\n" +
		"---\napiVersion: apps/v1\nkind: Deployment\nmetadata: {name: worker}\nspec: {template: {spec: {containers: [{name: c, image: 'elsewhere.invalid/job:1'}]}}}\n"
	if err := os.WriteFile(filepath.Join(repo, "k8s", "all.yaml"), []byte(manifest), 0o644); err != nil {
		t.Fatal(err)
	}
	testutil.InitGit(t, repo)
	testutil.Git(t, repo, "add", ".")
	testutil.Git(t, repo, "commit", "-qm", "synthetic")
	rawConfig, err := anypb.New(&contracts.RawRenderConfig{Paths: []string{"k8s"}})
	if err != nil {
		t.Fatal(err)
	}
	plan := &contracts.DeploymentPlan{
		Source:              &contracts.DeploymentSource{RendererId: "raw", Config: rawConfig},
		ExpectedWorkloadIds: []string{"Deployment/demo/web", "Deployment/demo/worker"},
	}
	target := deployTarget{
		Namespace: "demo", ControlNamespace: "sdo-system", AcceptedRoot: repo,
		WorkspaceRoot: config.WorktreeRoot, RepositoryMount: config.RepositoryMount, RepositoryPVC: "repo",
	}
	controller, _, err := parseControllerLaunch(controllerLaunchArgs("--deployment-policy", `{"allowedRegistries":["registry.synthetic.invalid"]}`))
	if err != nil {
		t.Fatal(err)
	}
	for name, policy := range map[string]*contracts.DeploymentPolicy{
		"operate":    mustPolicy(t, `{"allowedRegistries":["registry.synthetic.invalid"]}`),
		"controller": controller.Controller.GetDeploymentPolicy(),
	} {
		t.Run(name, func(t *testing.T) {
			deployer, err := operateIdentity(config).deployer(fake.NewSimpleClientset(), target, nil, policy)
			if err != nil {
				t.Fatal(err)
			}
			_, err = deployer.ApplyAndVerify(context.Background(), repo, plan)
			var violation *deployment.PolicyError
			if !errors.As(err, &violation) || !strings.Contains(err.Error(), "elsewhere.invalid/job:1") || strings.Contains(err.Error(), "registry.synthetic.invalid/web") {
				t.Fatalf("err = %v, want a PolicyError naming only the off-allowlist image", err)
			}
		})
	}
}

func mustPolicy(t *testing.T, encoded string) *contracts.DeploymentPolicy {
	t.Helper()
	policy, err := deploymentpolicy.Parse(encoded)
	if err != nil {
		t.Fatal(err)
	}
	return policy
}
