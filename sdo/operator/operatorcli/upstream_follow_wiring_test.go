package operatorcli

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/broker"
	"sdo.dev/operator/deployment"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/operatorcli/catalog/upstreams"
	"sdo.dev/operator/upstream"
)

// The controller parent deploys an advance with the same Deployer as
// onboarding (every renderer, the deployer's image catalog, the production
// render environment) and verifies with that Deployer.
func TestControllerAdvancerWiresTheOnboardingDeployer(t *testing.T) {
	run, err := ParseControllerLaunch(controllerLaunchArgs("--control-namespace", testControl))
	if err != nil {
		t.Fatal(err)
	}
	identity := deployIdentity{
		BuildKitImage: "buildkit@sha256:" + strings.Repeat("a", 64), RegistryPushSecret: "push",
		RegistryHost: "registry.synthetic.invalid:5000", RolloutTimeout: time.Minute,
	}
	advancer, cfg, err := controllerAdvancer(run.Controller, "/opt/sdo/sdo/operator", identity, fake.NewSimpleClientset(), &broker.Broker{}, upstreams.Catalog())
	if err != nil {
		t.Fatal(err)
	}
	deployer, ok := advancer.Readiness.(deployment.Deployer)
	if !ok {
		t.Fatalf("readiness = %T, want deployment.Deployer", advancer.Readiness)
	}
	if got := deployer.Renderers.IDs(); !reflect.DeepEqual(got, []string{"helm", "kustomize", "raw"}) {
		t.Fatalf("renderers = %v", got)
	}
	if got := deployer.Images.IDs(); !reflect.DeepEqual(got, []string{"buildkit", "prebuilt"}) {
		t.Fatalf("image sources = %v", got)
	}
	if deployer.Environment != nil || deployer.Namespace != "shop" || deployer.AcceptedRoot != "/synthetic/app" {
		t.Fatalf("deployer target = %+v", deployer)
	}
	verifier, ok := advancer.Verifier.(deployment.IndependentHealthVerifier)
	if !ok || verifier.Readiness == nil || verifier.Readiness.Renderers != deployer.Renderers || verifier.OperatorRoot != "/opt/sdo/sdo/operator" {
		t.Fatalf("verifier = %+v", advancer.Verifier)
	}
	if cfg.Namespace != "shop" || cfg.GitDir != "/synthetic/app/.git" {
		t.Fatalf("advance config = %+v", cfg)
	}
	// Every dependency is present: the advance refuses only the empty request.
	cfg.HealthObjective = "serve requests"
	if _, err := advancer.Advance(context.Background(), cfg, upstream.Request{}); err == nil || !strings.Contains(err.Error(), "advance request needs") {
		t.Fatalf("Advance(empty request) = %v", err)
	}
	if _, ok := advancer.Activity.(controllerActivity); !ok {
		t.Fatalf("activity = %T", advancer.Activity)
	}
}

func TestAcceptedHealthObjective(t *testing.T) {
	root := t.TempDir()
	if _, err := acceptedHealthObjective(root); err == nil {
		t.Fatal("missing goal accepted")
	}
	if err := memory.WriteGoal(root, "toy", "  serve every request\n", nil); err != nil {
		t.Fatal(err)
	}
	if got, err := acceptedHealthObjective(root); err != nil || got != "serve every request" {
		t.Fatalf("objective = %q, %v", got, err)
	}
	if err := memory.WriteGoal(root, "toy", " ", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := acceptedHealthObjective(root); err == nil {
		t.Fatal("empty objective accepted")
	}
}
