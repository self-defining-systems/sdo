package deploywire

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/deployment"
)

func fullIdentity() Identity {
	return Identity{
		BuildKitImage: "buildkit@sha256:" + strings.Repeat("a", 64), RegistryPushSecret: "push",
		RegistryHost: "registry.synthetic.invalid:5000", RegistryInsecure: true,
		BuildTimeout: 10 * time.Minute, BuildBudget: 30 * time.Minute, RolloutTimeout: 5 * time.Minute,
	}
}

// The controller parent's identity reaches the child through the typed launch
// config unchanged; no identity is no message.
func TestIdentitySurvivesTheLaunchConfig(t *testing.T) {
	for name, identity := range map[string]Identity{"full": fullIdentity(), "prebuilt only": {RolloutTimeout: time.Minute}} {
		t.Run(name, func(t *testing.T) {
			got, err := IdentityOf(&contracts.ControllerConfig{DeployIdentity: identity.Proto()})
			if err != nil || got != identity {
				t.Fatalf("read %+v, %v, want %+v", got, err, identity)
			}
		})
	}
	if (Identity{}).Proto() != nil {
		t.Fatal("the zero identity has a message")
	}
	if got, err := IdentityOf(&contracts.ControllerConfig{}); err != nil || got != (Identity{}) {
		t.Fatalf("an absent identity = %+v, %v, want the zero identity", got, err)
	}
}

func TestIdentityRejectsWhatTheLaunchFlagsReject(t *testing.T) {
	for name, message := range map[string]*contracts.DeployIdentity{
		"image without secret":  {BuildkitImage: "x"},
		"secret without image":  {RegistryPushSecret: "x"},
		"insecure without host": {RegistryInsecure: true},
		"negative timeout":      {BuildTimeout: durationpb.New(-time.Second)},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := IdentityOf(&contracts.ControllerConfig{DeployIdentity: message}); err == nil {
				t.Fatalf("%v accepted", message)
			}
		})
	}
}

func launch() *contracts.ControllerConfig {
	return &contracts.ControllerConfig{
		Namespace: "shop", ControlNamespace: "sdo-system", AppRoot: "/synthetic/app", BrokerWorktreeRoot: "/synthetic/repo/proposals",
		RepositoryMountPath: "/synthetic/repo", RepositoryPvc: "repo", Replay: "off", TelemetryEnvironmentFile: "/synthetic/telemetry/environment.json",
	}
}

// The repair deploys with the one trusted Deployer: every renderer, the
// deployer's image catalog, the launch's target; its verifier judges the
// deployment that same Deployer applied.
func TestRepairDeployerIsTheTrustedDeployer(t *testing.T) {
	repair, err := RepairDeployer(fullIdentity(), launch(), "/opt/sdo/sdo/operator", fake.NewSimpleClientset(), &broker.Broker{})
	if err != nil {
		t.Fatal(err)
	}
	if repair.Broker == nil {
		t.Fatal("the repair has no broker")
	}
	deployer, ok := repair.Readiness.(deployment.Deployer)
	if !ok {
		t.Fatalf("readiness = %T, want deployment.Deployer", repair.Readiness)
	}
	if got := deployer.Renderers.IDs(); !reflect.DeepEqual(got, []string{"helm", "kustomize", "raw"}) {
		t.Fatalf("renderers = %v", got)
	}
	if got := deployer.Images.IDs(); !reflect.DeepEqual(got, []string{"buildkit", "prebuilt"}) {
		t.Fatalf("image sources = %v", got)
	}
	if deployer.Namespace != "shop" || deployer.AcceptedRoot != "/synthetic/app" {
		t.Fatalf("deployer target = %+v", deployer)
	}
	verifier, ok := repair.Verifier.(deployment.IndependentHealthVerifier)
	if !ok || verifier.Readiness == nil || verifier.Readiness.Renderers != deployer.Renderers || verifier.OperatorRoot != "/opt/sdo/sdo/operator" || verifier.AcceptedRoot != "/synthetic/app" {
		t.Fatalf("verifier = %+v", repair.Verifier)
	}
	// The independent verification reads the same telemetry environment the
	// controller does, never a separate metrics URL.
	if probe, ok := verifier.Probe.(deployment.AcceptedDetectorProbe); !ok || probe.TelemetryEnvironment != "/synthetic/telemetry/environment.json" {
		t.Fatalf("probe = %+v", verifier.Probe)
	}
}

// Without a build identity only prebuilt images resolve: a repair that needs
// a build is refused by the Deployer (a hold), never built elsewhere.
func TestRepairDeployerWithoutABuildIdentityIsPrebuiltOnly(t *testing.T) {
	repair, err := RepairDeployer(Identity{}, launch(), "/opt/sdo/sdo/operator", fake.NewSimpleClientset(), &broker.Broker{})
	if err != nil {
		t.Fatal(err)
	}
	if got := repair.Readiness.(deployment.Deployer).Images.IDs(); !reflect.DeepEqual(got, []string{"prebuilt"}) {
		t.Fatalf("image sources = %v, want prebuilt only", got)
	}
}

func TestDeployerNeedsProposalsInsideTheRepositoryMount(t *testing.T) {
	config := launch()
	config.BrokerWorktreeRoot = "/elsewhere"
	if _, err := fullIdentity().Deployer(fake.NewSimpleClientset(), TargetOf(config), nil, nil); err == nil {
		t.Fatal("proposals outside the shared repository mount were accepted")
	}
	if got := ControlNamespace(&contracts.ControllerConfig{Namespace: "shop"}); got != "shop" {
		t.Fatalf("control namespace = %q", got)
	}
}

// The generated controller's factory builds the trusted repair deployer from
// the identity the parent put in the launch config, and refuses a broken one
// instead of deploying with another.
func TestRepairFromLaunch(t *testing.T) {
	launch := &contracts.ControllerConfig{Namespace: "shop", AppRoot: "/synthetic/app", BrokerWorktreeRoot: "/synthetic/repo/proposals", RepositoryMountPath: "/synthetic/repo", RepositoryPvc: "repo", DeployIdentity: fullIdentity().Proto()}
	deployer, err := RepairFromLaunch(launch, fake.NewSimpleClientset(), &broker.Broker{})
	if err != nil {
		t.Fatal(err)
	}
	if deployer.Broker == nil || deployer.Readiness == nil || deployer.Verifier == nil {
		t.Fatalf("deployer = %+v", deployer)
	}
	invalid := &contracts.ControllerConfig{DeployIdentity: &contracts.DeployIdentity{BuildkitImage: "x"}}
	if _, err := RepairFromLaunch(invalid, fake.NewSimpleClientset(), &broker.Broker{}); err == nil {
		t.Fatal("an invalid identity was accepted")
	}
}

// A responder's source repair deploys under the same install-time policy as
// onboarding and upstream advances: the policy rides the launch config.
func TestRepairDeployerCarriesTheInstallPolicy(t *testing.T) {
	config := launch()
	config.DeploymentPolicy = &contracts.DeploymentPolicy{AllowedRegistries: []string{"ghcr.io"}}
	repair, err := RepairDeployer(fullIdentity(), config, "/opt/sdo/sdo/operator", fake.NewSimpleClientset(), &broker.Broker{})
	if err != nil {
		t.Fatal(err)
	}
	deployer := repair.Readiness.(deployment.Deployer)
	if len(deployer.Policy.GetAllowedRegistries()) != 1 || deployer.Policy.GetAllowedRegistries()[0] != "ghcr.io" {
		t.Fatalf("repair deployer policy = %v", deployer.Policy)
	}
}
