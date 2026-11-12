package runtime

import (
	"errors"
	"strings"
	"testing"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/lifecycle"
)

func repairLaunch() *contracts.ControllerConfig {
	return &contracts.ControllerConfig{
		Namespace: "shop", AppRoot: "/synthetic/app", BrokerWorktreeRoot: "/synthetic/repo/proposals",
		RepositoryMountPath: "/synthetic/repo", RepositoryPvc: "repo",
	}
}

// The typed launch wraps what the composition root's factory builds, with the
// child's own broker.
func TestLaunchWiresTheSourceRepairDeployer(t *testing.T) {
	var gotConfig *contracts.ControllerConfig
	var gotRepository lifecycle.RepairBroker
	factory := func(config *contracts.ControllerConfig, _ kubernetes.Interface, repository lifecycle.RepairBroker) (lifecycle.RepairDeployer, error) {
		gotConfig, gotRepository = config, repository
		return lifecycle.RepairDeployer{Broker: repository}, nil
	}
	config := repairLaunch()
	child := &broker.Broker{}
	repairs, err := repairsFromLaunch(config, false, fake.NewSimpleClientset(), child, factory)
	if err != nil {
		t.Fatal(err)
	}
	repairer, ok := repairs.(DeploymentRepairer)
	if !ok || repairer.Namespace != "shop" || repairer.AppRoot != "/synthetic/app" || gotConfig != config || gotRepository != lifecycle.RepairBroker(child) {
		t.Fatalf("repairs = %+v", repairs)
	}
}

// Frozen memory publishes nothing, and shell detectors cannot judge a
// candidate checkout: neither gets a repairer, even without a factory. Any
// other launch without a factory fails instead of publishing repairs
// undeployed, and a factory failure fails the launch.
func TestLaunchSourceRepairDeployerGuards(t *testing.T) {
	shell := repairLaunch()
	shell.CodePath = &contracts.CodePathConfig{Name: "shell"}
	for name, launch := range map[string]struct {
		config *contracts.ControllerConfig
		frozen bool
	}{"frozen": {repairLaunch(), true}, "shell": {shell, false}} {
		repairs, err := repairsFromLaunch(launch.config, launch.frozen, fake.NewSimpleClientset(), &broker.Broker{}, nil)
		plain, ok := repairs.(PlainPublication)
		if err != nil || !ok || plain.Reason == "" {
			t.Fatalf("%s: repairs = %v, %v, want a declared plain publication", name, repairs, err)
		}
	}
	if _, err := repairsFromLaunch(repairLaunch(), false, fake.NewSimpleClientset(), &broker.Broker{}, nil); err == nil || !strings.Contains(err.Error(), "not wired") {
		t.Fatalf("a launch without a factory = %v, want a not-wired error", err)
	}
	failing := func(*contracts.ControllerConfig, kubernetes.Interface, lifecycle.RepairBroker) (lifecycle.RepairDeployer, error) {
		return lifecycle.RepairDeployer{}, errors.New("invalid identity")
	}
	if _, err := repairsFromLaunch(repairLaunch(), false, fake.NewSimpleClientset(), &broker.Broker{}, failing); err == nil || !strings.Contains(err.Error(), "invalid identity") {
		t.Fatalf("a factory failure = %v", err)
	}
}
