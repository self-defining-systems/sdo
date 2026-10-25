package operatorcli

import (
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/install"
)

func TestInstallExistingMenuEnvironmentReachesControllerPod(t *testing.T) {
	for _, value := range []string{"", "0", "1", "invalid"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("SDO_RESPONDER_MENU", value)
			args := []string{"install-existing", "--repository", "/accepted/app", "--namespace", "demo", "--application", "demo", "--controller-image", "controller:test", "--responder-image", "responder:test", "--validator-image", "validator:test", "--repository-pvc", "repository", "--credentials-secret", "credentials", "--model", "model"}
			launch, err := ParseInstallExistingLaunch(args)
			if value == "invalid" {
				if err == nil || !strings.Contains(err.Error(), "SDO_RESPONDER_MENU") {
					t.Fatalf("invalid menu input accepted: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			launch.Install.AcceptedCommit = strings.Repeat("a", 40)
			plan, err := install.Resources(launch.Install)
			if err != nil {
				t.Fatal(err)
			}
			want, actual := "0", ""
			if value != "0" {
				want = "1"
			}
			for _, environment := range plan.Job.Spec.Template.Spec.Containers[0].Env {
				if environment.Name == "SDO_RESPONDER_MENU" {
					actual = environment.Value
				}
			}
			if actual != want {
				t.Fatalf("controller menu environment=%q want %q", actual, want)
			}
			t.Logf("controller_launch_environment SDO_RESPONDER_MENU=%s", actual)
		})
	}
}

func TestParseInstallExistingUsesNativeAcceptedController(t *testing.T) {
	args := []string{
		"install-existing", "--repository", "/accepted/app", "--namespace", "demo", "--application", "demo",
		"--controller-image", "controller:test", "--responder-image", "responder:test", "--validator-image", "validator:test",
		"--repository-pvc", "repository", "--credentials-secret", "agent-credentials", "--model", "model",
		"--seed-broker-root", "/trusted/seed-broker",
		"--repair-policy", "recorded-actions", "--reflection-session", "resume", "--reflection-guidance", "generalize-spec", "--responder-env", "SDO_SUBMISSION_MODE=relay", "--deployed-manifest-path", "kubernetes", "--late-findings", "pull",
	}
	launch, err := ParseInstallExistingLaunch(args)
	if err != nil {
		t.Fatal(err)
	}
	if launch.SeedBrokerRoot != "/trusted/seed-broker" {
		t.Fatalf("trusted seed broker directory lost: %q", launch.SeedBrokerRoot)
	}
	if !launch.Install.ReuseExisting || launch.Install.RepairPolicy != "recorded-actions" || launch.Install.ReflectionSession != "resume" || launch.Install.ReflectionGuidance != "generalize-spec" || launch.Install.LateFindings != "pull" ||
		launch.Install.NativeCommand[0] != "/usr/local/bin/sdo-operator" || len(launch.Install.ExtraControllerArgs) != 2 ||
		launch.Install.ExtraControllerArgs[0] != "--responder-env=SDO_SUBMISSION_MODE=relay" || launch.Install.ExtraControllerArgs[1] != "--deployed-manifest-path=kubernetes" {
		t.Fatalf("existing installation lost native contract: %+v", launch.Install)
	}
	if launch.Install.WaitForCompletion {
		t.Fatal("persistent controller should not wait for job completion")
	}
	args = append(args, "--duration", "30s", "--exit-after-closure")
	bounded, err := ParseInstallExistingLaunch(args)
	if err != nil {
		t.Fatal(err)
	}
	if !bounded.Install.WaitForCompletion || bounded.Install.ReuseExisting || len(bounded.Install.ExtraControllerArgs) != 5 || bounded.Duration != 30*time.Second {
		t.Fatalf("bounded benchmark controller lost its completion receipt: %+v", bounded)
	}
}

func TestParseInstallExistingRejectsInvalidRepairPolicy(t *testing.T) {
	args := []string{"install-existing", "--repository", "/accepted/app", "--namespace", "demo", "--application", "demo", "--controller-image", "controller:test", "--responder-image", "responder:test", "--validator-image", "validator:test", "--repository-pvc", "repository", "--credentials-secret", "agent-credentials", "--model", "model", "--repair-policy", "unknown"}
	if _, err := ParseInstallExistingLaunch(args); err == nil {
		t.Fatal("unsupported policy accepted")
	}
}

func TestInstallExistingDefaultsToResumedReflection(t *testing.T) {
	args := []string{"install-existing", "--repository", "/accepted/app", "--namespace", "demo", "--application", "demo", "--controller-image", "controller:test", "--responder-image", "responder:test", "--validator-image", "validator:test", "--repository-pvc", "repository", "--credentials-secret", "agent-credentials", "--model", "model"}
	launch, err := ParseInstallExistingLaunch(args)
	if err != nil {
		t.Fatal(err)
	}
	if launch.Install.ReflectionSession != "resume" {
		t.Fatalf("default reflection mode: %q", launch.Install.ReflectionSession)
	}
}

func TestParseInstallExistingPermissiveResponder(t *testing.T) {
	args := []string{"install-existing", "--repository", "/accepted/app", "--namespace", "demo", "--application", "demo", "--control-namespace", "demo-sdo", "--controller-image", "controller:test", "--responder-image", "responder:test", "--validator-image", "validator:test", "--repository-pvc", "repository", "--credentials-secret", "agent-credentials", "--model", "model"}
	strict, err := ParseInstallExistingLaunch(args)
	if err != nil {
		t.Fatal(err)
	}
	if strict.Install.PermissiveResponder {
		t.Fatal("permissions defaulted on")
	}
	launch, err := ParseInstallExistingLaunch(append(args, "--permissive-responder", "--responder-namespace", "policy-system"))
	if err != nil {
		t.Fatal(err)
	}
	if !launch.Install.PermissiveResponder || len(launch.Install.ResponderNamespaces) != 1 || launch.Install.ResponderNamespaces[0] != "policy-system" {
		t.Fatalf("permissions lost: %+v", launch.Install)
	}
}

func TestGS1AutomaticRepairDefaultsAndOptOuts(t *testing.T) {
	t.Setenv("SDO_RESPONDER_MENU", "")
	args := []string{"install-existing", "--repository", "/accepted/app", "--namespace", "demo", "--application", "demo", "--controller-image", "controller:test", "--responder-image", "responder:test", "--validator-image", "validator:test", "--repository-pvc", "repository", "--credentials-secret", "credentials", "--model", "model"}
	launch, err := ParseInstallExistingLaunch(args)
	if err != nil {
		t.Fatal(err)
	}
	c := launch.Install
	if !c.ResponderMenu || c.RepairPolicy != "recorded-actions" || c.MaxResponseLoops != 10 || c.VerificationTimeoutSeconds != 300 {
		t.Fatalf("defaults: %+v", c)
	}
	disabled, err := ParseInstallExistingLaunch(append(args, "--repair-policy=commit"))
	if err != nil {
		t.Fatal(err)
	}
	disabled.Install.AcceptedCommit = strings.Repeat("a", 40)
	plan, err := install.Resources(disabled.Install)
	if err != nil {
		t.Fatal(err)
	}
	controller, _, err := ParseControllerLaunch(plan.Job.Spec.Template.Spec.Containers[0].Args)
	if err != nil || controller.Controller.GetRepairPolicy() != "commit" {
		t.Fatalf("opt-out lost: %+v %v", controller, err)
	}
}
