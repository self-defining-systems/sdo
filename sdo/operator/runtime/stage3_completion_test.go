package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

type stage3NoOpPortProgram struct{ pipelinePortProgram }

func (p stage3NoOpPortProgram) Mitigate(ctx context.Context, v program.ReadView, b program.Bindings) (program.Plan, error) {
	plan, err := p.pipelinePortProgram.Mitigate(ctx, v, b)
	command := program.Command{Pod: sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "api-pod"}, Container: "app", Args: []string{"observe"}}
	ensure := command
	ensure.Args = []string{"ensure"}
	plan.Actions = append([]program.Action{program.EnsureExec{Observe: command, Ensure: ensure}}, plan.Actions...)
	plan.Actions = append(plan.Actions, program.WaitFor{Ref: b.Objects["service"], Path: "/metadata/name", Equals: "api"})
	return plan, err
}

type stage3NoOpClient struct {
	PlanObjectClient
	fail bool
}

func (c stage3NoOpClient) RunOperation(_ context.Context, op program.ExternalOperation) (int32, error) {
	if !c.fail {
		return 0, nil
	}
	if op.Exec.Args[0] == "observe" {
		return 1, nil
	}
	return 0, errors.New("synthetic operation dispatch failed")
}

// A completed source write and an unresolved readback are separate facts.
// Settlement must be able to reobserve this typed state without reapplying it.
func TestStagePlanCompletionKeepsSourceReadbackSeparate(t *testing.T) {
	ref := addedFieldsRef()
	client := reconcilingAdmissionClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: addedFieldsLive()}}, mutate: func(live *unstructured.Unstructured) {
		_ = unstructured.SetNestedSlice(live.Object, append(addedFieldsContainers(live), map[string]any{"name": "injected", "image": "proxy:1"}), "spec", "template", "spec", "containers")
	}}
	e := PlanExecutor{Client: client, Source: planFakeSource{source: addedFieldsSource()}, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}}
	r, err := e.Apply(context.Background(), IncidentRequest{Namespace: ref.Namespace, SourceCommit: "accepted", RepairPolicy: "recorded-actions"}, reconcilePlan(ref), false)
	var residual *SourceReconcilePartialError
	if !errors.As(err, &residual) || r.VerificationPassed || r.RolledBack {
		t.Fatalf("fixture lost unresolved readback: %+v %v", r, err)
	}
	if !PlanActionsComplete(r) {
		t.Fatalf("completed source write confused with convergence: %+v", r.ActionReports)
	}
	encoded, err := r.Proto()
	if err != nil {
		t.Fatal(err)
	}
	if !PlanActionsComplete(planApplicationState(encoded).Result) {
		t.Fatal("receipt restore lost execution completion")
	}
}

// Nonwriting steps may precede source writes. Mutation count is never an
// action index: every written source object still needs its direct readback.
func TestStageSourceReadbackSurvivesNonwritingActionOrder(t *testing.T) {
	for _, kind := range []string{"wait", "ensure"} {
		for _, before := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/before=%t", kind, before), func(t *testing.T) {
				ref := addedFieldsRef()
				podRef := databaseAction().Target.Pod
				pod := planObject(podRef, map[string]any{})
				pod.SetUID("pod")
				pod.SetResourceVersion("1")
				client := reconcilingAdmissionClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: addedFieldsLive(), podRef: pod}}, mutate: func(live *unstructured.Unstructured) {
					_ = unstructured.SetNestedSlice(live.Object, append(addedFieldsContainers(live), map[string]any{"name": "injected", "image": "proxy:1"}), "spec", "template", "spec", "containers")
				}}
				var noop program.Action = program.WaitFor{Ref: ref, Path: "/metadata/name", Equals: ref.Name}
				if kind == "ensure" {
					observe := program.Command{Pod: podRef, Container: "db", Args: []string{"observe"}}
					ensure := observe
					ensure.Args = []string{"ensure"}
					noop = program.EnsureExec{Observe: observe, Ensure: ensure}
				}
				actions := []program.Action{program.ReconcileFromSource{Ref: ref}, noop}
				index := 0
				if before {
					actions = []program.Action{noop, actions[0]}
					index = 1
				}
				executor := PlanExecutor{Client: stage3NoOpClient{PlanObjectClient: client}, Source: planFakeSource{source: addedFieldsSource()}, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}}
				result, err := executor.Apply(context.Background(), IncidentRequest{Namespace: ref.Namespace, SourceCommit: "accepted", RepairPolicy: "recorded-actions"}, program.Plan{Actions: actions}, false)
				var partial *SourceReconcilePartialError
				if !errors.As(err, &partial) || result.VerificationPassed || result.ActionReports[index].SourceReconcile == nil || result.ActionReports[index].SourceReconcile.Converged {
					t.Fatalf("action order skipped source readback: %+v %v", result, err)
				}
			})
		}
	}
}
