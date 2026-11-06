package install

import (
	"encoding/json"
	"flag"
	"os"
	"testing"

	"sdo.dev/operator/telemetry"
	"sdo.dev/operator/telemetry/prometheus/recipe"
)

var updateGolden = flag.Bool("update-golden", false, "rewrite golden files")

// The Prometheus object set is installed bytes: moving it between packages must
// not change them. The golden holds the plan's observability objects for two
// control-namespace layouts.
func TestObservabilityObjectsAreByteStable(t *testing.T) {
	for name, cfg := range map[string]Config{
		"single-namespace": {Namespace: "app"},
		"split-namespace":  {Namespace: "app", ControlNamespace: "sdo-ctl"},
	} {
		var plan Plan
		cfg.BackendRecipes = []telemetry.BackendRecipe{recipe.New()}
		if err := addObservability(&plan, cfg); err != nil {
			t.Fatal(err)
		}
		got, err := json.MarshalIndent(map[string]any{
			"config": plan.Backend.ConfigMaps[0], "service": plan.Backend.Services[0], "deployment": plan.Backend.Deployments[0],
			"serviceAccounts": plan.ServiceAccounts, "clusterRoles": plan.ClusterRoles, "clusterBindings": plan.ClusterBindings,
		}, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		path := "testdata/observability_" + name + ".golden.json"
		if *updateGolden {
			if err := os.WriteFile(path, append(got, '\n'), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		want, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if string(want) != string(got)+"\n" {
			t.Fatalf("%s: observability objects changed; review the diff or regenerate with -update-golden", name)
		}
	}
}
