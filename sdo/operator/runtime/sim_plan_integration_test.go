//go:build integration

package runtime

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func TestSimPlanApplyUnderServerFaults(t *testing.T) {
	sweepSimSeeds(t, func(t *testing.T, seed uint64) {
		t.Run("reconcile-normalized-quantities", func(t *testing.T) {
			simPlanRun(t, seed,
				func() *unstructured.Unstructured { return frontendDeployment("app-image:latest") },
				reconcilePlan, frontendDeployment("registry.invalid/frontend:good"))
		})
		t.Run("reconcile-defaulted", func(t *testing.T) {
			simPlanRun(t, seed,
				func() *unstructured.Unstructured { return deploymentWithDrift("registry.invalid/rate:missing") },
				reconcilePlan, deploymentWithDrift("registry.invalid/rate:good"))
		})
		t.Run("set-field-then-set-field", func(t *testing.T) {
			simPlanRun(t, seed,
				func() *unstructured.Unstructured { return deploymentWithDrift("registry.invalid/rate:missing") },
				func(ref sdk.ObjectRef) program.Plan {
					return program.Plan{Actions: []program.Action{
						program.SetField{Ref: ref, Path: "spec.replicas", Value: testPlanValue(float64(3))},
						program.SetField{Ref: ref, Path: "spec.template.spec.containers[0].image", Value: testPlanValue("registry.invalid/rate:good")},
					}}
				}, deploymentWithDrift("registry.invalid/rate:good"))
		})
	})
}
