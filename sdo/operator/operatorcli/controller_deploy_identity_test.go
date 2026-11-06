package operatorcli

import (
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/install"
)

// The controller parent deploys upstream advances with the operate Job's
// identity: the operate launch -> install config -> controller Job args ->
// controller launch chain must carry it unchanged.
func TestOperateIdentityReachesTheControllerParent(t *testing.T) {
	launch, err := ParseOperateLaunch(operateArgs("--registry-host", "registry.synthetic.invalid:5000", "--registry-insecure",
		"--build-timeout", "7m", "--build-budget", "11m", "--rollout-timeout", "3m"))
	if err != nil {
		t.Fatal(err)
	}
	launch.Install.AcceptedCommit = strings.Repeat("c", 40) // the installer sets it from the accepted checkout
	plan, err := install.Resources(launch.Install)
	if err != nil {
		t.Fatal(err)
	}
	args := plan.Job.Spec.Template.Spec.Containers[0].Args
	_, identity, err := parseControllerLaunch(args)
	if err != nil {
		t.Fatalf("controller Job args %v: %v", args, err)
	}
	want := deployIdentity{
		BuildKitImage: "buildkit@sha256:" + strings.Repeat("a", 64), RegistryPushSecret: "push",
		RegistryHost: "registry.synthetic.invalid:5000", RegistryInsecure: true,
		BuildTimeout: 7 * time.Minute, BuildBudget: 11 * time.Minute, RolloutTimeout: 3 * time.Minute,
	}
	if identity != want || !identity.Enabled() {
		t.Fatalf("controller identity = %+v, want %+v", identity, want)
	}
}

// Seeded and install-existing controllers have no build identity: they
// launch unchanged and do not follow.
func TestControllerWithoutIdentityDoesNotDeploy(t *testing.T) {
	_, identity, err := parseControllerLaunch(controllerLaunchArgs())
	if err != nil {
		t.Fatal(err)
	}
	if identity.Enabled() {
		t.Fatalf("identity = %+v, want disabled", identity)
	}
	for _, args := range [][]string{
		{"--buildkit-image", "buildkit@sha256:" + strings.Repeat("a", 64)},
		{"--registry-push-secret", "push"},
		{"--registry-insecure"},
		{"--build-timeout", "-1s"},
	} {
		if _, _, err := parseControllerLaunch(controllerLaunchArgs(args...)); err == nil {
			t.Fatalf("%v accepted", args)
		}
	}
}
