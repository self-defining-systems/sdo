package install

import (
	"slices"
	"strings"
	"testing"

	"sdo.dev/operator/telemetry"
	"sdo.dev/operator/telemetry/prometheus/recipe"
)

// prometheusProfile is the profile the operator CLI's built-in prometheus-default
// resolves to for control namespace app.
func prometheusProfile() telemetry.Profile {
	return telemetry.Profile{
		Name: "prometheus-default", Mode: telemetry.ModeOpen, Recipes: []string{"prometheus"},
		Capabilities: []telemetry.ClusterCapability{telemetry.CapPodDiscovery, telemetry.CapNodeProxy},
		Sources: []telemetry.ProfileSource{{
			ID: "prom", Adapter: "prometheus", Kinds: []telemetry.Kind{telemetry.KindMetrics},
			Endpoint: "http://sdo-prometheus.app.svc:9090", Default: true,
		}},
		MetricsPublishURL: "http://sdo-prometheus.app.svc:9090",
	}
}

func environmentConfig() Config {
	return Config{
		TelemetryProfile: prometheusProfile(),
		BackendRecipes:   []telemetry.BackendRecipe{recipe.New()},
		Namespace:        "app", Application: "app", RepositoryPVC: "repo", CredentialsSecret: "creds",
		ControllerImage: "controller:test", ResponderImage: "responder:test", Model: "m", Replay: "on",
		NativeCommand: []string{"/usr/local/bin/sdo-operator"}, AcceptedCommit: strings.Repeat("c", 40), TimeoutSeconds: 60, VerificationTimeoutSeconds: 60,
	}
}

func TestControllerReadsTheTelemetryEnvironmentFromAMountedConfigMap(t *testing.T) {
	plan, err := Resources(environmentConfig())
	if err != nil {
		t.Fatal(err)
	}
	args := plan.Job.Spec.Template.Spec.Containers[0].Args
	i := slices.Index(args, "--telemetry-environment")
	if i < 0 || args[i+1] != TelemetryEnvironmentFile() {
		t.Fatalf("controller args = %v; want --telemetry-environment %s", args, TelemetryEnvironmentFile())
	}
	for _, retired := range []string{"--metrics-url", "--metrics-publish-url", "--metrics-token-file"} {
		if slices.Contains(args, retired) {
			t.Fatalf("controller args carry the retired %s: %v", retired, args)
		}
	}
	pod := plan.Job.Spec.Template.Spec
	var mounted bool
	for _, mount := range pod.Containers[0].VolumeMounts {
		mounted = mounted || (mount.Name == "telemetry-environment" && mount.MountPath == TelemetryEnvironmentDir && mount.ReadOnly)
	}
	var fromMap bool
	for _, volume := range pod.Volumes {
		fromMap = fromMap || (volume.Name == "telemetry-environment" && volume.ConfigMap != nil && volume.ConfigMap.Name == plan.TelemetryEnvironment.Name)
	}
	if !mounted || !fromMap {
		t.Fatalf("environment ConfigMap is not mounted read-only at %s (mount %v, volume %v)", TelemetryEnvironmentDir, mounted, fromMap)
	}
	env, err := telemetry.ParseEnvironment([]byte(plan.TelemetryEnvironment.Data["environment.json"]))
	if err != nil {
		t.Fatal(err)
	}
	if plan.TelemetryEnvironment.Labels["app.kubernetes.io/managed-by"] != "sdo" {
		t.Fatalf("environment ConfigMap is not labelled as installer-owned: %v", plan.TelemetryEnvironment.Labels)
	}
	provided := env.ProvidedBindings()
	if len(provided) != 1 || provided[0].ID != "prom" || provided[0].Endpoint.URL != "http://sdo-prometheus.app.svc:9090" || !provided[0].Default {
		t.Fatalf("provided = %+v; the installed stack is the one default source", provided)
	}
	if env.MetricsPublishURL != "http://sdo-prometheus.app.svc:9090" {
		t.Fatalf("publish URL = %q", env.MetricsPublishURL)
	}
}

func TestTelemetryEnvironmentFollowsTheProfile(t *testing.T) {
	external := environmentConfig()
	external.TelemetryProfile = telemetry.Profile{Name: "external", Sources: []telemetry.ProfileSource{{
		ID: "ops", Adapter: "prometheus", Kinds: []telemetry.Kind{telemetry.KindMetrics}, Endpoint: "http://prom.ops.svc:9090", Default: true,
	}}}
	if env := external.TelemetryEnvironment(); env.Provided[0].Endpoint.URL != "http://prom.ops.svc:9090" || env.MetricsPublishURL != "" {
		t.Fatalf("an external profile provides its source and offers no publishing: %+v", env)
	}
	if env := external.TelemetryEnvironment(); env.ApplicationNamespace == "" || env.ControlNamespace == "" {
		t.Fatalf("the controller applies binding policy at open, so the environment names its namespaces: %+v", env)
	}
	none := environmentConfig()
	none.TelemetryProfile = telemetry.Profile{Name: "none"}
	if env := none.TelemetryEnvironment(); len(env.Provided) != 0 || env.MetricsPublishURL != "" {
		t.Fatalf("an empty profile provides nothing: %+v", env)
	}
	if plan, err := Resources(none); err != nil || len(plan.Backend.Deployments) != 0 {
		t.Fatalf("an empty profile applies no recipe: %v %v", plan.Backend, err)
	}
	first, err := Resources(external)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Resources(none)
	if err != nil {
		t.Fatal(err)
	}
	const hash = "sdo.dev/telemetry-environment-hash"
	a, b := first.Job.Spec.Template.Annotations[hash], second.Job.Spec.Template.Annotations[hash]
	if a == "" || a == b || strings.Contains(a, " ") {
		t.Fatalf("a changed environment must change the controller template: %q vs %q", a, b)
	}
	if first.Job.Annotations[InstallFingerprintAnnotation] == second.Job.Annotations[InstallFingerprintAnnotation] {
		t.Fatal("the install fingerprint ignores the environment document")
	}
}
