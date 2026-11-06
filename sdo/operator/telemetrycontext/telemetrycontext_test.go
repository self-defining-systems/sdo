package telemetrycontext

import (
	"slices"
	"testing"

	"sdo.dev/operator/telemetry"
)

func TestBuildCarriesOfferedRecipesAndGrantedEndpoints(t *testing.T) {
	env := telemetry.Environment{
		Version: telemetry.EnvironmentVersion, Mode: telemetry.ModeOpen,
		ApplicationNamespace: "shop", ControlNamespace: "sdo-system", ExtraNamespaces: []string{"obs", "shop"},
		ExternalHosts: []string{"metrics.example.com"}, Credentials: []string{"prom-token"},
		Recipes: []telemetry.Recipe{{Name: "prometheus", Declares: []telemetry.ClusterCapability{telemetry.CapPodDiscovery}}},
	}
	got, err := Build(nil, env, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Recipes) != 1 || got.Recipes[0].Name != "prometheus" || !slices.Equal(got.Recipes[0].Declares, []string{"pod-discovery"}) {
		t.Errorf("recipes = %v", got.Recipes)
	}
	if want := []string{"shop", "sdo-system", "obs"}; !slices.Equal(got.GrantedNamespaces, want) {
		t.Errorf("namespaces = %v, want %v", got.GrantedNamespaces, want)
	}
	if !slices.Equal(got.GrantedHosts, []string{"metrics.example.com"}) || !slices.Equal(got.GrantedCredentials, []string{"prom-token"}) {
		t.Errorf("hosts %v credentials %v", got.GrantedHosts, got.GrantedCredentials)
	}
}

func TestBuildOfOnlyAnOpenEnvironmentOffersNoRecipe(t *testing.T) {
	got, err := Build(nil, telemetry.Environment{Version: telemetry.EnvironmentVersion, Mode: telemetry.ModeOpen}, nil)
	if err != nil || len(got.Recipes) != 0 || len(got.GrantedNamespaces) != 0 {
		t.Fatalf("got %v err %v", got, err)
	}
}
