package operatorcli

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/broker"
	"sdo.dev/operator/deployment"
	"sdo.dev/operator/lifecycle"
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
	advancer, cfg, _, err := controllerAdvancer(run.Controller, "/opt/sdo/sdo/operator", identity, fake.NewSimpleClientset(), &broker.Broker{}, upstreams.Catalog())
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
	applied, ok := advancer.Verifier.(lifecycle.TelemetryAppliedVerifier)
	if !ok || applied.Apply == nil {
		t.Fatalf("advance verifier = %T, want one that applies telemetry backends first", advancer.Verifier)
	}
	verifier, ok := applied.Next.(deployment.IndependentHealthVerifier)
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
	// A shape change routes to the detection designer, not to a hold.
	if designer, ok := advancer.Designer.(designerRefresh); !ok || designer.launch != run.Controller {
		t.Fatalf("designer = %#v", advancer.Designer)
	}
	// A hold the deployer owns routes to the deployment agent (E1-E3).
	if corrector, ok := advancer.Deployer.(deployerCorrection); !ok || corrector.launch != run.Controller {
		t.Fatalf("deployer correction = %#v", advancer.Deployer)
	}
}

func wiringIdentity() deployIdentity {
	return deployIdentity{
		BuildKitImage: "buildkit@sha256:" + strings.Repeat("a", 64), RegistryPushSecret: "push",
		RegistryHost: "registry.synthetic.invalid:5000", RolloutTimeout: time.Minute,
	}
}

// Production wiring pins every collaborator whose absence silently disables a
// safety feature: the settle window's clock, the maintenance clock, the
// detection designer, the deployment agent and the operability exposer.
func TestControllerAdvancerWiresEveryRequiredCollaborator(t *testing.T) {
	run, err := ParseControllerLaunch(controllerLaunchArgs("--control-namespace", testControl))
	if err != nil {
		t.Fatal(err)
	}
	advancer, _, maintenance, err := controllerAdvancer(run.Controller, "/opt/sdo/sdo/operator", wiringIdentity(), fake.NewSimpleClientset(), &broker.Broker{}, upstreams.Catalog())
	if err != nil {
		t.Fatal(err)
	}
	if advancer.Now == nil {
		t.Fatal("the advancer has no clock: the settle window and pause deadline are off")
	}
	if got := advancer.Now(); got.IsZero() {
		t.Fatal("the advancer clock reads the zero time")
	}
	if maintenance.Now == nil || advancer.Maintenance == nil {
		t.Fatalf("maintenance = %+v", maintenance)
	}
	if advancer.Designer == nil || advancer.Deployer == nil || advancer.Exposer == nil || advancer.Activity == nil {
		t.Fatalf("designer %v, deployer %v, exposer %v, activity %v", advancer.Designer, advancer.Deployer, advancer.Exposer, advancer.Activity)
	}
	if err := (advanceMaintenance{Client: fake.NewSimpleClientset(), Namespace: testControl}).validate(); err == nil {
		t.Fatal("a maintenance without a clock was accepted")
	} else {
		var missing *lifecycle.MissingCollaboratorError
		if !errors.As(err, &missing) {
			t.Fatalf("err = %T, want *lifecycle.MissingCollaboratorError", err)
		}
	}
}

// The production follower is assembled complete, and one built without a
// collaborator is refused with a typed error naming it.
func TestUpstreamFollowingIsAssembledComplete(t *testing.T) {
	run, err := ParseControllerLaunch(controllerLaunchArgs("--control-namespace", testControl))
	if err != nil {
		t.Fatal(err)
	}
	sources := func(context.Context) ([]upstream.Source, error) { return nil, nil }
	following, err := assembleUpstreamFollowing(context.Background(), run.Controller, "/opt/sdo/sdo/operator", wiringIdentity(), upstreams.Catalog(), sources, slog.Default(), fake.NewSimpleClientset(), &broker.Broker{})
	if err != nil {
		t.Fatal(err)
	}
	if err := following.validate(); err != nil {
		t.Fatal(err)
	}
	if following.Now == nil || following.SleepUntil == nil || following.Holds == nil || following.Obligations == nil || following.Refresh == nil {
		t.Fatalf("following = %+v", following)
	}
	following.Now = nil
	var missing *lifecycle.MissingCollaboratorError
	if err := following.validate(); !errors.As(err, &missing) || !slices.Equal(missing.Missing, []string{"clock"}) {
		t.Fatalf("validate = %v", err)
	}
}
