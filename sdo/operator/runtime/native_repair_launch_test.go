package runtime

import (
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/operatorcli/deploywire"
)

func repairLaunch() *contracts.ControllerConfig {
	return &contracts.ControllerConfig{
		Namespace: "shop", AppRoot: "/synthetic/app", BrokerWorktreeRoot: "/synthetic/repo/proposals",
		RepositoryMountPath: "/synthetic/repo", RepositoryPvc: "repo",
	}
}

// The typed launch gives the controller child the trusted repair deployer,
// built from the identity the parent put in the config.
func TestLaunchWiresTheSourceRepairDeployer(t *testing.T) {
	identity := deploywire.Identity{BuildKitImage: "buildkit@sha256:" + strings.Repeat("a", 64), RegistryPushSecret: "push", RegistryHost: "registry.synthetic.invalid:5000", RolloutTimeout: time.Minute}
	config := repairLaunch()
	config.DeployIdentity = identity.Proto()
	repairs, err := repairsFromLaunch(config, fake.NewSimpleClientset(), &broker.Broker{})
	if err != nil {
		t.Fatal(err)
	}
	repairer, ok := repairs.(DeploymentRepairer)
	if !ok || repairer.Namespace != "shop" || repairer.AppRoot != "/synthetic/app" || repairer.Deployer.Broker == nil || repairer.Deployer.Readiness == nil || repairer.Deployer.Verifier == nil {
		t.Fatalf("repairs = %+v", repairs)
	}
}

// Frozen memory publishes nothing, and shell detectors cannot judge a
// candidate checkout: neither gets a repairer. A broken identity fails the
// launch instead of deploying with another one.
func TestLaunchSourceRepairDeployerGuards(t *testing.T) {
	frozen := repairLaunch()
	frozen.FrozenMemory = true
	shell := repairLaunch()
	shell.CodePath = &contracts.CodePathConfig{Name: "shell"}
	for name, config := range map[string]*contracts.ControllerConfig{"frozen": frozen, "shell": shell} {
		if repairs, err := repairsFromLaunch(config, fake.NewSimpleClientset(), &broker.Broker{}); err != nil || repairs != nil {
			t.Fatalf("%s: repairs = %v, %v, want none", name, repairs, err)
		}
	}
	invalid := repairLaunch()
	invalid.DeployIdentity = &contracts.DeployIdentity{BuildkitImage: "x"}
	if _, err := repairsFromLaunch(invalid, fake.NewSimpleClientset(), &broker.Broker{}); err == nil {
		t.Fatal("an invalid identity was accepted")
	}
}
