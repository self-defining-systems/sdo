package operatorcli

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"sdo.dev/operator/install"
	"sdo.dev/operator/operatorcli/catalog/telemetrysources"
	"sdo.dev/operator/telemetry"
	"sdo.dev/operator/telemetrycontext"
)

func mustBuiltin(t *testing.T, name, control string) telemetry.Profile {
	t.Helper()
	profile, ok := telemetrysources.BuiltinProfile(name, control)
	if !ok {
		t.Fatalf("no built-in profile %q", name)
	}
	return profile
}

func withSource(t *testing.T, profile telemetry.Profile, value string) telemetry.Profile {
	t.Helper()
	id, endpoint, _ := strings.Cut(value, "=")
	out, err := profile.WithSource(telemetry.ProfileSource{ID: id, Adapter: "prometheus", Kinds: []telemetry.Kind{telemetry.KindMetrics}, Endpoint: endpoint})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestTelemetryProfileDefaultsToPrometheusDefaultAndInstallsTheStack(t *testing.T) {
	launch, err := ParseHostLaunchEnv(hostArgs(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if launch.Install.TelemetryProfile.Name != telemetrysources.DefaultProfileName {
		t.Fatalf("default profile = %q", launch.Install.TelemetryProfile.Name)
	}
	launch.Install.AcceptedCommit = strings.Repeat("c", 40)
	plan, err := install.Resources(launch.Install)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Backend.Deployments) != 1 || plan.Backend.Deployments[0].Name != "sdo-prometheus" {
		t.Fatalf("the default profile installs today's Prometheus: %+v", plan.Backend)
	}
}

func TestTelemetryProfileNoneInstallsNothingAndSourcesAreProvided(t *testing.T) {
	launch, err := ParseHostLaunchEnv(hostArgs(), []string{"SDO_TELEMETRY_PROFILE=none", `SDO_TELEMETRY_SOURCES=["ops=http://prom.ops.svc:9090"]`})
	if err != nil {
		t.Fatal(err)
	}
	launch.Install.AcceptedCommit = strings.Repeat("c", 40)
	plan, err := install.Resources(launch.Install)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Backend.Deployments) != 0 {
		t.Fatalf("profile none applied a recipe: %+v", plan.Backend)
	}
	env := launch.Install.TelemetryEnvironment()
	if len(env.Provided) != 1 || env.Provided[0].ID != "ops" || !env.Provided[0].Default || env.MetricsPublishURL != "" {
		t.Fatalf("environment = %+v", env)
	}
	context, err := telemetrycontext.Build(telemetryRegistry(), env, nil)
	if err != nil || len(context.GetSources()) != 1 || context.GetSources()[0].GetEndpoint() != "http://prom.ops.svc:9090" || context.GetSources()[0].GetPublish() != nil {
		t.Fatalf("telemetry context = %v, err = %v", context, err)
	}
}

func TestTelemetryProfileFileAndTheOperateJobRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "profile.yaml")
	document := "name: lab\nmode: pinned\nsources:\n- id: lab\n  adapter: prometheus\n  kinds: [metrics]\n  endpoint: http://prom.lab.svc:9090\n  default: true\n"
	if err := os.WriteFile(path, []byte(document), 0o600); err != nil {
		t.Fatal(err)
	}
	launch, err := ParseHostLaunchEnv(hostArgs(), []string{"SDO_TELEMETRY_PROFILE=" + path})
	if err != nil {
		t.Fatal(err)
	}
	profile := launch.Install.TelemetryProfile
	if profile.Name != "lab" || profile.Mode != telemetry.ModePinned {
		t.Fatalf("profile = %+v", profile)
	}
	launch.Install.AcceptedCommit = strings.Repeat("c", 40)
	job, err := install.OperateJob(launch.Install, "healthy", "op")
	if err != nil {
		t.Fatal(err)
	}
	args := job.Spec.Template.Spec.Containers[0].Args
	i := slices.Index(args, "--telemetry-profile")
	if i < 0 {
		t.Fatalf("the operate Job does not carry the profile: %v", args)
	}
	again, err := ParseOperateLaunch(operateArgs("--telemetry-profile", args[i+1]))
	if err != nil {
		t.Fatal(err)
	}
	if again.Install.TelemetryProfile.Name != "lab" || len(again.Install.TelemetryProfile.Sources) != 1 {
		t.Fatalf("round trip = %+v", again.Install.TelemetryProfile)
	}
}

func TestRetiredTelemetryFlagsAreRefusedAndBadProfilesFail(t *testing.T) {
	for _, flag := range [][]string{{"--no-observability-stack"}, {"--metrics-url", "http://x.svc"}, {"--metrics-publish-url", "http://x.svc"}} {
		if _, err := ParseOperateLaunch(operateArgs(flag...)); err == nil {
			t.Errorf("%v accepted", flag)
		}
	}
	if _, err := ParseOperateLaunch(operateArgs("--telemetry-profile", "no-such-profile")); err == nil {
		t.Error("operate accepted an unknown profile")
	}
	for _, bad := range [][]string{{"SDO_TELEMETRY_PROFILE=no-such-profile"}, {`SDO_TELEMETRY_SOURCES=["missing-equals"]`}, {`SDO_TELEMETRY_SOURCES=["a=http://x.svc","a=http://y.svc"]`}} {
		if _, err := ParseHostLaunchEnv(hostArgs(), bad); err == nil {
			t.Errorf("%v accepted", bad)
		}
	}
}
