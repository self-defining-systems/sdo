package operatorcli

import (
	"testing"
	"time"
)

func TestParseInstallExistingUsesNativeAcceptedController(t *testing.T) {
	args := []string{
		"install-existing", "--repository", "/accepted/app", "--namespace", "demo", "--application", "demo",
		"--controller-image", "controller:test", "--responder-image", "responder:test", "--validator-image", "validator:test",
		"--repository-pvc", "repository", "--credentials-secret", "agent-credentials", "--model", "model",
		"--repair-policy", "recorded-actions", "--reflection-session", "resume", "--reflection-guidance", "generalize-spec", "--max-follow-ups", "2", "--follow-up-cooldown", "25s", "--responder-env", "SDO_SUBMISSION_MODE=relay", "--late-findings", "pull",
	}
	launch, err := ParseInstallExistingLaunch(args)
	if err != nil {
		t.Fatal(err)
	}
	if !launch.Install.ReuseExisting || launch.Install.RepairPolicy != "recorded-actions" || launch.Install.ReflectionSession != "resume" || launch.Install.ReflectionGuidance != "generalize-spec" || launch.Install.MaxFollowUps != 2 || launch.Install.FollowUpCooldownSeconds != 25 || launch.Install.LateFindings != "pull" ||
		launch.Install.NativeCommand[0] != "/usr/local/bin/sdo-operator" || len(launch.Install.ExtraControllerArgs) != 1 ||
		launch.Install.ExtraControllerArgs[0] != "--responder-env=SDO_SUBMISSION_MODE=relay" {
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
	if !bounded.Install.WaitForCompletion || bounded.Install.ReuseExisting || len(bounded.Install.ExtraControllerArgs) != 4 || bounded.Duration != 30*time.Second {
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
