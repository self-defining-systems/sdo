package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type convergingSourceRollout struct {
	observations int
	persistent   bool
}

func (r *convergingSourceRollout) Wait(context.Context, []sdk.ObjectRef) error {
	r.observations++
	return nil
}
func (r *convergingSourceRollout) SourceRolloutResiduals(context.Context, *unstructured.Unstructured) ([]string, error) {
	r.observations++
	if r.persistent || r.observations < 4 {
		return []string{"rollout.pods[test/old]"}, nil
	}
	return nil, nil
}

// Run the real executor's write producer into its receipt and repair consumer.
// Sweep both document and object dispatch and a nested general-plan dispatch.
func TestSourceConvergenceBeforeTerminalReceipt(t *testing.T) {
	for _, dispatch := range []string{"document", "object", "general", "runtime"} {
		for _, persistent := range []bool{false, true} {
			t.Run(dispatch+map[bool]string{false: "/converges", true: "/bounded"}[persistent], func(t *testing.T) {
				ref := addedFieldsRef()
				actions := []program.Action{program.ReconcileFromSource{Ref: ref}}
				if dispatch == "object" {
					actions = append(actions, program.CreateFromSource{Ref: sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: ref.Namespace, Name: "created"}, Reason: "restore accepted workload"})
				}
				if dispatch == "general" {
					actions = append(actions, program.WaitFor{Ref: ref, Path: "/metadata/name", Equals: ref.Name, Timeout: time.Second, Interval: time.Millisecond})
				}
				podRef := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: ref.Namespace, Name: "synthetic"}
				if dispatch == "runtime" {
					actions = append(actions, program.Exec{Pod: podRef, Container: "app", Command: []program.Value{program.FromParam("command")}})
				}
				rollout := &convergingSourceRollout{persistent: persistent}
				client := &lifecycleTestClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: addedFieldsLive()}}}
				pod := planObject(podRef, map[string]any{})
				pod.SetUID("pod")
				pod.SetResourceVersion("1")
				client.objects[podRef] = pod
				client.objects[ref].SetUID("workload")
				client.objects[ref].SetResourceVersion("1")
				executor := PlanExecutor{Values: program.ValueInputs{Spec: program.ProgramSpec{Params: []program.ParamSpec{{Name: "command", ValueKind: program.StringValue, Default: "/bin/true"}}}}, Client: convergenceExecClient{client}, Source: convergenceTestSource{planFakeSource{source: addedFieldsSource()}}, Rollouts: rollout, Verifier: planFakeVerifier{passed: true}, RolloutTimeout: 350 * time.Millisecond}
				started := time.Now()
				result, err := executor.Apply(context.Background(), IncidentRequest{Namespace: ref.Namespace, SourceCommit: "accepted", RepairPolicy: "recorded-actions"}, program.Plan{Actions: actions}, false)
				var partial *SourceReconcilePartialError
				if persistent {
					// Waiting keeps at most a quarter of the bound for the final readbacks.
					if elapsed := time.Since(started); !errors.As(err, &partial) || result.VerificationPassed || elapsed < executor.RolloutTimeout*3/4 || elapsed > executor.RolloutTimeout+100*time.Millisecond {
						t.Fatalf("residual did not wait for bound: result=%+v observations=%d err=%v", result, rollout.observations, err)
					}
				} else if err != nil || !result.VerificationPassed || !result.ActionReports[0].SourceReconcile.Converged {
					t.Fatalf("in-flight rollout failed: result=%+v err=%v", result, err)
				}
				receipt, encodeErr := result.Proto()
				if encodeErr != nil {
					t.Fatal(encodeErr)
				}
				if err := wire.Validate(receipt); err != nil {
					t.Fatal(err)
				}
				restored := planApplicationState(receipt).Result
				repairs := ProgramRepairReceipts([]RecordedPlanApplication{{ID: "source", Result: restored, StartedAt: time.Now(), CompletedAt: time.Now()}}, nil)
				if len(repairs) == 0 || repairs[0].Success == persistent {
					t.Fatalf("terminal source receipt has wrong credit: %+v", repairs)
				}
			})
		}
	}
}

type convergenceTestSource struct{ planFakeSource }

func (s convergenceTestSource) AcceptedManifest(ctx context.Context, commit string, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	object, err := s.planFakeSource.AcceptedManifest(ctx, commit, ref)
	object.SetName(ref.Name)
	return object, err
}

func TestPendingSourceConvergenceKeepsAttributionUntilObservation(t *testing.T) {
	for _, persistent := range []bool{false, true} {
		t.Run(map[bool]string{false: "converges", true: "stalls"}[persistent], func(t *testing.T) {
			ref := addedFieldsRef()
			rollout := &convergingSourceRollout{persistent: persistent}
			executor := PlanExecutor{Client: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: addedFieldsLive()}}, Source: planFakeSource{source: addedFieldsSource()}, Rollouts: rollout, Verifier: planFakeVerifier{passed: true}, PollRollouts: true}
			incident := IncidentRequest{Namespace: ref.Namespace, SourceCommit: "accepted", RepairPolicy: "recorded-actions"}
			result, err := executor.Apply(context.Background(), incident, reconcilePlan(ref), false)
			var pending *RolloutPendingError
			if !errors.As(err, &pending) || result.RolloutPending == nil || result.ActionReports[0].Status != "applied" || result.VerificationPassed {
				t.Fatalf("write was not retained as pending: %+v %v", result, err)
			}
			encoded, err := result.Proto()
			if err != nil {
				t.Fatal(err)
			}
			if err := wire.Validate(encoded); err != nil {
				t.Fatal(err)
			}
			result = planApplicationState(encoded).Result
			repairs := ProgramRepairReceipts([]RecordedPlanApplication{{ID: "source", Result: result, Pending: true}}, nil)
			if len(repairs) == 0 || repairs[0].Success {
				t.Fatalf("pending attribution became a successful repair: %+v", repairs)
			}
			loop := &ResponderLoop{Request: incident, Executor: executor, applications: map[string]PlanApplicationResult{"source": result}, applicationsLog: []RecordedPlanApplication{{ID: "source", Result: result, Pending: true}}, pendingWrites: map[string]pendingMitigation{"source": {Refs: []sdk.ObjectRef{ref}, ReservedAt: time.Now()}}, PendingStallTimeout: time.Hour}
			rollout.persistent = true
			loop.settlePending(context.Background())
			if !planApplicationPending(loop.applications["source"]) || loop.applications["source"].VerificationPassed {
				t.Fatal("healthy sample bypassed source convergence")
			}
			rollout.persistent = persistent
			if persistent {
				loop.PendingStallTimeout = time.Nanosecond
			}
			loop.settlePending(context.Background())
			settled := loop.applications["source"]
			if persistent {
				if settled.VerificationPassed || settled.ActionReports[0].FailureReason != "source_residual" {
					t.Fatalf("stalled residual lost: %+v", settled)
				}
			} else if !settled.VerificationPassed || settled.RolloutPending != nil || !settled.ActionReports[0].SourceReconcile.Converged || loop.applicationsLog[0].Pending {
				t.Fatalf("pending application lost late credit: %+v", settled)
			}
		})
	}
}

type convergenceExecClient struct{ *lifecycleTestClient }

func (convergenceExecClient) Exec(context.Context, sdk.ObjectRef, string, []string) (int, error) {
	return 0, nil
}
