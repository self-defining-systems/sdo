package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type failingBatchClient struct {
	*planFakeClient
	calls        int
	failRollback bool
}

func (c *failingBatchClient) Patch(ctx context.Context, ref sdk.ObjectRef, kind types.PatchType, patch []byte) error {
	c.calls++
	if c.calls == 2 || (c.failRollback && c.calls > 2) {
		return errors.New("admission rejected patch")
	}
	return c.planFakeClient.Patch(ctx, ref, kind, patch)
}
func TestPlanExecutorCompensatesSecondPatchFailure(t *testing.T) {
	for _, failRollback := range []bool{false, true} {
		t.Run(map[bool]string{false: "compensated", true: "rollback fails"}[failRollback], func(t *testing.T) {
			ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}
			client := &failingBatchClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: planObject(ref, map[string]any{"replicas": float64(1)})}}, failRollback: failRollback}
			executor := PlanExecutor{Client: client, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}}
			result, err := executor.Apply(context.Background(), IncidentRequest{RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.SetField{Ref: ref, Path: "spec.replicas", Value: float64(2)}, program.SetField{Ref: ref, Path: "spec.replicas", Value: float64(3)}}}, false)
			if err == nil || !result.Applied || len(result.ActionsApplied) != 1 {
				t.Fatalf("partial application lost: %+v, %v", result, err)
			}
			replicas, _ := testField(client.objects[ref], "spec.replicas")
			if !failRollback && replicas != float64(1) {
				t.Fatalf("partial write remains: %v", replicas)
			}
			if failRollback && replicas != float64(2) {
				t.Fatalf("unexpected failed rollback value: %v", replicas)
			}
		})
	}
}

type concurrentPlanClient struct {
	*planFakeClient
	intervene func()
}

func (c *concurrentPlanClient) Patch(ctx context.Context, ref sdk.ObjectRef, kind types.PatchType, patch []byte) error {
	if c.intervene != nil {
		f := c.intervene
		c.intervene = nil
		f()
	}
	var document map[string]any
	if err := json.Unmarshal(patch, &document); err != nil {
		return err
	}
	if metadata, ok := document["metadata"].(map[string]any); ok {
		current := c.objects[ref]
		if metadata["uid"] != string(current.GetUID()) || metadata["resourceVersion"] != current.GetResourceVersion() {
			return errors.New("optimistic concurrency conflict")
		}
	}
	return c.planFakeClient.Patch(ctx, ref, kind, patch)
}
func TestPlanExecutorRejectsInterveningUpdateOrRecreate(t *testing.T) {
	for _, recreate := range []bool{false, true} {
		t.Run(fmt.Sprint(recreate), func(t *testing.T) {
			ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}
			before := planObject(ref, map[string]any{"replicas": float64(1)})
			before.SetUID("original")
			before.SetResourceVersion("1")
			client := &concurrentPlanClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: before}}}
			client.intervene = func() {
				client.objects[ref].SetResourceVersion("2")
				if recreate {
					client.objects[ref].SetUID("replacement")
				}
			}
			executor := PlanExecutor{Client: client, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}}
			_, err := executor.Apply(context.Background(), IncidentRequest{RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.SetField{Ref: ref, Path: "spec.replicas", Value: float64(2)}}}, false)
			if err == nil || len(client.patches) != 0 {
				t.Fatalf("concurrent object overwritten: %v", err)
			}
		})
	}
}
func TestPlanExecutorRollbackPreservesUnrelatedFieldsAndRejectsConflicts(t *testing.T) {
	for _, mode := range []string{"unrelated", "owned", "recreated"} {
		t.Run(mode, func(t *testing.T) {
			ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}
			before := planObject(ref, map[string]any{"replicas": float64(1), "template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "api", "image": "v1", "readinessProbe": map[string]any{"periodSeconds": float64(10)}}}}}})
			before.SetUID("original")
			before.SetResourceVersion("1")
			client := &concurrentPlanClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: before}}}
			executor := PlanExecutor{Client: client, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}}
			result, err := executor.Apply(context.Background(), IncidentRequest{RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.SetField{Ref: ref, Path: "spec.template.spec.containers[0].readinessProbe.periodSeconds", Value: float64(5)}}}, false)
			if err != nil {
				t.Fatal(err)
			}
			object := client.objects[ref]
			object.SetResourceVersion("2")
			_ = program.ApplySetField(object.Object, "spec.template.spec.containers[0].image", "v2")
			if mode == "owned" {
				_ = program.ApplySetField(object.Object, "spec.template.spec.containers[0].readinessProbe.periodSeconds", float64(7))
			}
			if mode == "recreated" {
				object.SetUID("replacement")
			}
			err = executor.Rollback(context.Background(), result)
			if mode != "unrelated" {
				if err == nil {
					t.Fatal("conflicting rollback accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			image, _ := testField(client.objects[ref], "spec.template.spec.containers.0.image")
			probe, _ := testField(client.objects[ref], "spec.template.spec.containers.0.readinessProbe.periodSeconds")
			if image != "v2" || probe != float64(10) {
				t.Fatalf("rollback image=%v probe=%v", image, probe)
			}
		})
	}
}

type dryRunRejectingClient struct {
	*planFakeClient
	dryRuns int
}

func (c *dryRunRejectingClient) DryRunPatch(context.Context, sdk.ObjectRef, types.PatchType, []byte) (*unstructured.Unstructured, error) {
	c.dryRuns++
	return nil, errors.New("admission dry-run denied")
}
func TestPlanExecutorChecksAdmissionBeforeAnyWrite(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}
	for _, dryRun := range []bool{false, true} {
		client := &dryRunRejectingClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: planObject(ref, map[string]any{"replicas": float64(1)})}}}
		executor := PlanExecutor{Client: client, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}}
		_, err := executor.Apply(context.Background(), IncidentRequest{RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.SetField{Ref: ref, Path: "spec.replicas", Value: 2}}}, dryRun)
		if err == nil || client.dryRuns != 1 || len(client.patches) != 0 {
			t.Fatalf("admission bypass: dryRun=%t checks=%d writes=%d err=%v", dryRun, client.dryRuns, len(client.patches), err)
		}
	}
}
func TestResponderLoopExposesPartialApplicationWhenCompensationFails(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	client := &failingBatchClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: planObject(ref, map[string]any{"ports": []any{map[string]any{"targetPort": float64(9999)}}})}}, failRollback: true}
	loop := recoveryLoop(t, client)
	loop.plans["two-actions"] = selectedPlan{id: "two-actions", programID: "targetport", plan: program.Plan{Actions: []program.Action{program.SetField{Ref: ref, Path: "spec.ports[0].targetPort", Value: 8080}, program.SetField{Ref: ref, Path: "spec.ports[0].targetPort", Value: 8081}}}}
	response := loop.Handle(context.Background(), ToolRequest{Command: "apply", PlanIDs: []string{"two-actions"}})
	if response.Error == "" || response.ApplicationID == "" {
		t.Fatalf("lost partial rollback handle: %+v", response)
	}
	entry := loop.PlanApplications()[0]
	if !entry.Result.Applied || entry.Result.RollbackError == "" || !entry.Result.RollingBack {
		t.Fatalf("lost recovery state: %+v", entry)
	}
}

func TestPlanExecutorRetainsInProcessRollbackProgress(t *testing.T) {
	ctx := context.Background()
	ref, plan, client := repeatedReplicaPlan()
	client.rejectAttempt = 6 // Three forward writes and two reverse writes succeed.
	executor := PlanExecutor{Client: client, Rollouts: recoveryRolloutFailure{}, Verifier: planFakeVerifier{passed: true}}
	result, err := executor.Apply(ctx, IncidentRequest{RepairPolicy: "recorded-actions"}, plan, false)
	if err == nil || result.RollbackCompleted != 2 {
		t.Fatalf("in-process progress lost: %+v %v", result, err)
	}
	client.rejectAttempt = 0
	if err = executor.rollback(ctx, &result); err != nil {
		t.Fatal(err)
	}
	requireRecoveredReplicas(t, client, ref)
}
