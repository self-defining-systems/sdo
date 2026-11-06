package renderenv_test

import (
	"reflect"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/version"
	fakediscovery "k8s.io/client-go/discovery/fake"
	clienttesting "k8s.io/client-go/testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/render"
	"sdo.dev/operator/renderenv"
)

// An unknown environment is today's default: the namespace and secret mode
// only, so evidence recorded before discovery keeps its EnvDigest.
func TestUnknownEnvironmentIsTheRendererDefault(t *testing.T) {
	for _, environment := range []*contracts.RenderEnvironment{nil, {}} {
		got := renderenv.For(environment, "demo", render.SecretsInclude)
		want := render.Env{Namespace: "demo", Secrets: render.SecretsInclude}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("For(%v) = %+v, want %+v", environment, got, want)
		}
	}
}

func TestEnvironmentReachesTheRenderEnv(t *testing.T) {
	environment := &contracts.RenderEnvironment{KubeVersion: "v1.31.2", ApiVersions: []string{"apps/v1", "v1"}}
	got := renderenv.For(environment, "demo", render.SecretsOmit)
	want := render.Env{Namespace: "demo", KubeVersion: "v1.31.2", APIVersions: []string{"apps/v1", "v1"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("For = %+v, want %+v", got, want)
	}
	got.APIVersions[0] = "mutated"
	if environment.ApiVersions[0] != "apps/v1" {
		t.Fatal("the render.Env aliases the recorded environment")
	}
}

func TestDiscoverRecordsServerVersionAndServedAPIs(t *testing.T) {
	client := &fakediscovery.FakeDiscovery{Fake: &clienttesting.Fake{Resources: []*metav1.APIResourceList{
		{GroupVersion: "v1", APIResources: []metav1.APIResource{{Name: "services", Kind: "Service"}}},
		{GroupVersion: "apps/v1", APIResources: []metav1.APIResource{{Name: "deployments", Kind: "Deployment"}, {Name: "deployments/scale", Kind: "Scale"}}},
	}}, FakedServerVersion: &version.Info{GitVersion: "v1.31.2"}}
	got, err := renderenv.Discover(client)
	if err != nil {
		t.Fatal(err)
	}
	want := &contracts.RenderEnvironment{KubeVersion: "v1.31.2", ApiVersions: []string{"apps/v1", "apps/v1/Deployment", "apps/v1/Scale", "v1", "v1/Service"}}
	if !renderenv.Same(got, want) {
		t.Fatalf("Discover = %v, want %v", got, want)
	}
}

func TestDiscoverRefusesAServerWithoutAVersion(t *testing.T) {
	client := &fakediscovery.FakeDiscovery{Fake: &clienttesting.Fake{}, FakedServerVersion: &version.Info{}}
	if _, err := renderenv.Discover(client); err == nil {
		t.Fatal("a server reporting no version was accepted")
	}
	if _, err := renderenv.Discover(nil); err == nil {
		t.Fatal("a nil discovery client was accepted")
	}
}

func TestFlagRoundTrip(t *testing.T) {
	environment := &contracts.RenderEnvironment{KubeVersion: "v1.31.2", ApiVersions: []string{"apps/v1", "v1"}}
	encoded, err := renderenv.Format(environment)
	if err != nil || strings.ContainsAny(encoded, "\n ") {
		t.Fatalf("Format = %q, %v; want compact JSON", encoded, err)
	}
	decoded, err := renderenv.Parse(encoded)
	if err != nil || !renderenv.Same(decoded, environment) {
		t.Fatalf("Parse(%q) = %v, %v", encoded, decoded, err)
	}
	if encoded, err := renderenv.Format(nil); err != nil || encoded != "" {
		t.Fatalf("Format(nil) = %q, %v", encoded, err)
	}
	if decoded, err := renderenv.Parse(""); err != nil || decoded != nil {
		t.Fatalf("Parse(\"\") = %v, %v; want unknown", decoded, err)
	}
	if _, err := renderenv.Parse(`{"kubeVersion":"v1.31.2","release":"x"}`); err == nil {
		t.Fatal("unknown field accepted")
	}
}

func TestSameTreatsNilAndEmptyAsTheUnknownEnvironment(t *testing.T) {
	if !renderenv.Same(nil, &contracts.RenderEnvironment{}) || renderenv.Same(nil, &contracts.RenderEnvironment{KubeVersion: "v1.30.0"}) {
		t.Fatal("Same disagrees about the unknown environment")
	}
}
