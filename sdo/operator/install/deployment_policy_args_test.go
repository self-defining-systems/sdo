package install

import (
	"errors"
	"slices"
	"strings"
	"testing"
)

const testPolicy = `{"allowedRegistries":["ghcr.io"],"scopeBlocking":true}`

func policyConfig() Config {
	return Config{
		Namespace: "app", Application: "app", RepositoryPVC: "repo", CredentialsSecret: "creds",
		ControllerImage: "controller:test", ResponderImage: "responder:test", Settings: testSettings(), NativeCommand: []string{"/usr/local/bin/sdo-operator"}, AcceptedCommit: strings.Repeat("c", 40),
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
	operate, err := OperateJob(cfg, "stay healthy", "op-1")
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
	operate, _ = OperateJob(cfg, "stay healthy", "op-1")
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

// Extra controller arguments are operator-supplied and go last on the command
// line, where the Go flag package lets a repeat replace an earlier value. The
// security-relevant flags have one owner, the installer's typed fields.
func TestExtraControllerArgsCannotOverrideInstallerOwnedFlags(t *testing.T) {
	for _, arg := range []string{
		"--deployment-policy={}", "-deployment-policy", "--deployment-policy", "--registry-insecure", "--registry-host=evil.example",
		"--registry-push-secret=x", "--buildkit-image=x", "--broker-mode=python", "--worker-mount-mode=rw", "--broker-arg=x",
	} {
		cfg := policyConfig()
		cfg.DeploymentPolicy = testPolicy
		cfg.ExtraControllerArgs = []string{arg}
		err := cfg.validate()
		var protected *ProtectedFlagError
		if !errors.As(err, &protected) {
			t.Errorf("%s: err = %v, want *ProtectedFlagError", arg, err)
		}
	}
	cfg := policyConfig()
	cfg.ExtraControllerArgs = []string{"--duration=1s", "--exit-after-closure", "--render-env={}"}
	if err := cfg.validate(); err != nil {
		t.Fatalf("legitimate extension refused: %v", err)
	}
}
