package runtime

import (
	"context"
	"encoding/json"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"testing"
)

// Real producer: a write, an already-satisfied ensure and a successful wait
// complete three actions but produce only one mutation receipt.
func TestStagePlanCompletionIncludesObservedNoOps(t *testing.T) {
	pod := databaseAction().Target.Pod
	object := planObject(pod, map[string]any{})
	object.SetUID("pod")
	object.SetResourceVersion("1")
	c := &externalTestClient{lifecycleTestClient: &lifecycleTestClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{pod: object}}}}
	e := PlanExecutor{Client: c, Rollouts: generalNoRollouts{}, Verifier: generalHealthy{}}
	observe := program.Command{Pod: pod, Container: "db", Args: []string{"observe"}}
	ensure := program.Command{Pod: pod, Container: "db", Args: []string{"ensure"}}
	plan := program.Plan{Actions: []program.Action{
		program.PatchObject{Ref: pod, Type: "merge", Patch: json.RawMessage(`{"metadata":{"labels":{"ready":"yes"}}}`)},
		program.EnsureExec{Observe: observe, Ensure: ensure},
		program.WaitFor{Ref: pod, Path: "/metadata/labels/ready", Equals: "yes"},
	}}
	r, err := e.Apply(context.Background(), IncidentRequest{}, plan, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.ActionsApplied) != 1 {
		t.Fatalf("mutations=%d", len(r.ActionsApplied))
	}
	if !PlanActionsComplete(r) {
		t.Fatalf("real producer was incomplete: %+v", r)
	}
	encoded, err := r.Proto()
	if err != nil {
		t.Fatal(err)
	}
	if !PlanActionsComplete(planApplicationState(encoded).Result) {
		t.Fatal("receipt lost completion")
	}
	for _, report := range r.ActionReports {
		if report.Status != "applied" {
			t.Fatalf("completed action lost: %+v", report)
		}
	}
}

func TestStagePlanCompletionRejectsIncompleteReports(t *testing.T) {
	for _, status := range []string{"prepared", "partial", "unknown", "refused"} {
		r := PlanApplicationResult{Plan: program.Plan{Actions: []program.Action{program.WaitFor{}}}, ActionReports: []PlanActionReport{{ActionIndex: 0, Status: status}}}
		if PlanActionsComplete(r) {
			t.Fatalf("%s granted completion", status)
		}
	}
}
