package install

import (
	"slices"
	"strings"
	"testing"
)

const testPolicy = `{"allowedRegistries":["ghcr.io"],"scopeBlocking":true}`

func policyConfig() Config {
	return Config{
		Namespace: "app", Application: "app", RepositoryPVC: "repo", CredentialsSecret: "creds",
		ControllerImage: "controller:test", ResponderImage: "responder:test", Model: "m", Replay: "on", NativeCommand: []string{"/usr/local/bin/sdo-operator"}, AcceptedCommit: strings.Repeat("c", 40), TimeoutSeconds: 60, VerificationTimeoutSeconds: 60,
		BuildKitImage: "buildkit@sha256:" + strings.Repeat("a", 64), RegistryPushSecret: "push",
	}
}

func argValue(args []string, flag string) (string, bool) {
	i := slices.Index(args, flag)
	if i < 0 || i+1 >= len(args) {
		return "", false
	}
	return args[i+1], true
}

// The install-time deployment policy reaches both the operate Job and the
// controller; an install without one passes neither flag (today's behavior).
func TestDeploymentPolicyReachesOperateAndController(t *testing.T) {
	cfg := policyConfig()
	cfg.DeploymentPolicy = testPolicy
	operate, err := OperateJob(cfg, "stay healthy", "op-1", 3)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := argValue(operate.Spec.Template.Spec.Containers[0].Args, "--deployment-policy"); !ok || got != testPolicy {
		t.Fatalf("operate args lack the policy: %v", operate.Spec.Template.Spec.Containers[0].Args)
	}
	controller := controllerJob(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := argValue(controller.Spec.Template.Spec.Containers[0].Args, "--deployment-policy"); !ok || got != testPolicy {
		t.Fatalf("controller args lack the policy: %v", controller.Spec.Template.Spec.Containers[0].Args)
	}

	cfg.DeploymentPolicy = ""
	operate, _ = OperateJob(cfg, "stay healthy", "op-1", 3)
	controller = controllerJob(cfg)
	for name, args := range map[string][]string{"operate": operate.Spec.Template.Spec.Containers[0].Args, "controller": controller.Spec.Template.Spec.Containers[0].Args} {
		if slices.Contains(args, "--deployment-policy") {
			t.Errorf("%s passes --deployment-policy without a policy", name)
		}
	}
}

func TestInvalidDeploymentPolicyFailsTheInstall(t *testing.T) {
	cfg := policyConfig()
	cfg.DeploymentPolicy = `{"grantedNamespaces":["x"]}`
	if err := cfg.validate(); err == nil {
		t.Fatal("grants without scope blocking were installed")
	}
}
