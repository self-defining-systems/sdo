package runtime

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func noSleep(context.Context, time.Duration) error { return nil }

func churnExecutor(client *serverPlanClient, sleeps *[]time.Duration) PlanExecutor {
	source := planFakeSource{source: deploymentWithDrift("registry.invalid/rate:good")}
	record := func(_ context.Context, d time.Duration) error { *sleeps = append(*sleeps, d); return nil }
	return PlanExecutor{Client: client, Source: source, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}, Sleep: record}
}

func applyChurn(client *serverPlanClient, ref sdk.ObjectRef, sleeps *[]time.Duration) (PlanApplicationResult, error) {
	return churnExecutor(client, sleeps).Apply(context.Background(), IncidentRequest{RepairPolicy: "recorded-actions", SourceCommit: "accepted"}, reconcilePlan(ref), false)
}

func TestPlanExecutorSurvivesResourceVersionChurnBelowLimit(t *testing.T) {
	client, ref := newServerPlanClient(deploymentWithDrift("registry.invalid/rate:missing"))
	client.raceBeforePatch = maxApplyAttempts - 1 // the deployment controller writes status before every attempt but the last
	var sleeps []time.Duration
	result, err := applyChurn(client, ref, &sleeps)
	if err != nil || !result.Applied || result.RolledBack || !result.VerificationPassed {
		t.Fatalf("status churn failed a correct plan: %+v, %v", result, err)
	}
	image, _ := testField(client.objects[ref], "spec.template.spec.containers.0.image")
	if image != "registry.invalid/rate:good" {
		t.Fatalf("image = %v", image)
	}
	if len(sleeps) != maxApplyAttempts-1 {
		t.Fatalf("backoff sleeps = %v", sleeps)
	}
	for _, d := range sleeps {
		if d <= 0 || d > time.Second {
			t.Fatalf("backoff out of range: %v", sleeps)
		}
	}
}

func TestPlanExecutorBoundsResourceVersionChurn(t *testing.T) {
	client, ref := newServerPlanClient(deploymentWithDrift("registry.invalid/rate:missing"))
	client.raceBeforePatch = 1000
	var sleeps []time.Duration
	result, err := applyChurn(client, ref, &sleeps)
	if !apierrors.IsConflict(err) || !strings.Contains(err.Error(), "resourceVersion kept changing") {
		t.Fatalf("unbounded or unexplained churn failure: %v", err)
	}
	if attempts := 1000 - client.raceBeforePatch; attempts != maxApplyAttempts {
		t.Fatalf("attempts = %d, want %d", attempts, maxApplyAttempts)
	}
	if result.Applied || result.RolledBack {
		t.Fatalf("nothing should have been written: %+v", result)
	}
	image, _ := testField(client.objects[ref], "spec.template.spec.containers.0.image")
	if image != "registry.invalid/rate:missing" {
		t.Fatalf("image = %v", image)
	}
}

func TestPlanExecutorReportsOwnedFieldEditDuringConflict(t *testing.T) {
	client, ref := newServerPlanClient(deploymentWithDrift("registry.invalid/rate:missing"))
	client.raceBeforePatch = 1
	client.raceEdit = func(object *unstructured.Unstructured) {
		_ = program.ApplySetField(object.Object, "spec.template.spec.containers[0].image", "registry.invalid/rate:other")
	}
	var sleeps []time.Duration
	result, err := applyChurn(client, ref, &sleeps)
	want := `concurrent change before action 0: spec.template.spec.containers[0].image changed from "registry.invalid/rate:missing" to "registry.invalid/rate:other"`
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("error = %v, want %q", err, want)
	}
	if result.Applied {
		t.Fatalf("write landed over a concurrent edit: %+v", result)
	}
	image, _ := testField(client.objects[ref], "spec.template.spec.containers.0.image")
	if image != "registry.invalid/rate:other" {
		t.Fatalf("concurrent edit overwritten: %v", image)
	}
}

func TestPlanExecutorFailsHardWhenObjectIsRecreatedDuringConflict(t *testing.T) {
	client, ref := newServerPlanClient(deploymentWithDrift("registry.invalid/rate:missing"))
	client.raceBeforePatch = 1
	client.raceEdit = func(object *unstructured.Unstructured) { object.SetUID("uid-2") }
	var sleeps []time.Duration
	result, err := applyChurn(client, ref, &sleeps)
	if err == nil || !strings.Contains(err.Error(), "was recreated") {
		t.Fatalf("recreate not reported: %v", err)
	}
	if result.Applied || len(sleeps) != 0 {
		t.Fatalf("recreate retried: %+v, sleeps %v", result, sleeps)
	}
}

func TestPlanExecutorRetriesLaterActionAfterResourceVersionChurn(t *testing.T) {
	client, ref := newServerPlanClient(deploymentWithDrift("registry.invalid/rate:missing"))
	plan := program.Plan{Actions: []program.Action{
		program.SetField{Ref: ref, Path: "spec.replicas", Value: float64(3)},
		program.SetField{Ref: ref, Path: "spec.template.spec.containers[0].image", Value: "registry.invalid/rate:good"},
	}}
	client.afterPatch = func(*unstructured.Unstructured) { client.raceBeforePatch = 2 } // churn before the second write only
	var sleeps []time.Duration
	result, err := churnExecutor(client, &sleeps).Apply(context.Background(), IncidentRequest{RepairPolicy: "recorded-actions"}, plan, false)
	if err != nil || len(result.ActionsApplied) != 2 || result.RolledBack {
		t.Fatalf("second action failed on status churn: %+v, %v", result, err)
	}
	image, _ := testField(client.objects[ref], "spec.template.spec.containers.0.image")
	replicas, _ := testField(client.objects[ref], "spec.replicas")
	if image != "registry.invalid/rate:good" || fmt.Sprint(replicas) != "3" {
		t.Fatalf("image=%v replicas=%v", image, replicas)
	}
}
