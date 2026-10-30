package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

func TestStageConsumesCompletionOfWaitAndSatisfiedEnsure(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(fmt.Sprint("partial_failure=", failed), func(t *testing.T) {
			loop, state := pipelineTestLoop(t, true)
			typed := loop.Executor.Client.(TypedPlanClient)
			pod, err := typed.Client.CoreV1().Pods("app").Get(context.Background(), "api-pod", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			pod.UID = "synthetic-pod"
			pod.ResourceVersion = "1"
			if _, err = typed.Client.CoreV1().Pods("app").Update(context.Background(), pod, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			p := stage3NoOpPortProgram{}
			registry, err := program.NewRegistry([]program.Diagnoser{p}, []program.Mitigator{p})
			if err != nil {
				t.Fatal(err)
			}
			loop.Programs = registry
			loop.Executor.Client = stage3NoOpClient{PlanObjectClient: typed, fail: failed}
			dispatcher := &startupSessionDispatcher{}
			stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}
			if err = RunResponseStages(context.Background(), state, []ResponseStage{stage, AgentResponseStage{Dispatcher: dispatcher}}); err != nil {
				t.Fatal(err)
			}
			attempt := state.PriorStages[0].Mitigations[0]
			if failed {
				if attempt.Result != "apply_error" || dispatcher.opened.IsZero() {
					t.Fatalf("partial failure did not hand off: %v", attempt)
				}
			} else {
				if attempt.Result != "applied_cleared" || len(attempt.ApplyReceipt.ActionsApplied) != 1 || len(attempt.ApplyReceipt.ActionReports) != 3 {
					t.Fatalf("valid nonwriting steps became incomplete: %v", attempt)
				}
			}
		})
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
