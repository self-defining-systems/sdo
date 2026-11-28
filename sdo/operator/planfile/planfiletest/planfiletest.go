// Package planfiletest writes deployment overrides in tests, as a person or
// the deployer agent would commit them. Production Go never writes one.
package planfiletest

import (
	"os"
	"path/filepath"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/planfile"
)

// Write stores plan at .sdo/deployment/plan.yaml in checkout.
func Write(t testing.TB, checkout string, plan *contracts.DeploymentPlan) {
	t.Helper()
	data, err := planfile.Format(plan)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(checkout, filepath.FromSlash(layout.DeploymentPlanFile))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}
