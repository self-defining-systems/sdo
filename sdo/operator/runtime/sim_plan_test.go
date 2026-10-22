package runtime

import (
	"context"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// simPlanRun applies one correct plan to a simulated server under the seed's
// faults and checks that the executor's report matches the cluster.
func simPlanRun(t *testing.T, seed uint64, build func() *unstructured.Unstructured, plan func(sdk.ObjectRef) program.Plan, source *unstructured.Unstructured) {
	t.Helper()
	rng := newSimRNG(seed)
	faults := drawSimFaults(rng)
	kube := newSimKube(faults, build())
	var ref sdk.ObjectRef
	for ref = range kube.objects {
	}
	clock := newSimClock()
	executor := PlanExecutor{
		Client: kube, Source: planFakeSource{source: source}, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true},
		Now: clock.Now,
	}
	before := kube.objects[ref].DeepCopy()
	result, err := executor.Apply(context.Background(), IncidentRequest{RepairPolicy: "recorded-actions", SourceCommit: "accepted"}, plan(ref), false)
	image, _ := testField(kube.objects[ref], "spec.template.spec.containers.0.image")
	wantImage, _ := testField(source, "spec.template.spec.containers.0.image")
	beforeImage, _ := testField(before, "spec.template.spec.containers.0.image")

	switch {
	case err != nil && kube.timeouts == 0:
		// Churn (up to simMaxChurn bumps) is retried, never surfaced as a conflict.
		t.Fatalf("correct plan failed with no ambiguous write: %v (applied=%t rolledBack=%t)", err, result.Applied, result.RolledBack)
	case err == nil:
		// A correct plan on an object nobody else edits has no legitimate
		// reason to fail, however much the server normalizes or churns.
		if !result.Applied || result.RolledBack || !result.VerificationPassed || image != wantImage {
			t.Fatalf("success reported but cluster disagrees: image=%v want=%v applied=%t rolledBack=%t verified=%t", image, wantImage, result.Applied, result.RolledBack, result.VerificationPassed)
		}
	case strings.Contains(err.Error(), "concurrent change"):
		t.Fatalf("false concurrent change with no foreign writer: %v (churn/normalization only)", err)
	case result.RollingBack || result.RollbackError != "":
		// Compensation stopped partway and said so; the cluster may be in between.
	case result.RolledBack || !result.Applied:
		// The executor claims the cluster is back at the pre-image.
		if image != beforeImage {
			t.Fatalf("failed apply left image=%v (before=%v): %v", image, beforeImage, err)
		}
	default:
		// Applied, not rolled back, no error about it: the write must be live.
		if image != wantImage {
			t.Fatalf("result claims applied but image=%v want=%v: %v", image, wantImage, err)
		}
	}
}

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
						program.SetField{Ref: ref, Path: "spec.replicas", Value: float64(3)},
						program.SetField{Ref: ref, Path: "spec.template.spec.containers[0].image", Value: "registry.invalid/rate:good"},
					}}
				}, deploymentWithDrift("registry.invalid/rate:good"))
		})
	})
}
