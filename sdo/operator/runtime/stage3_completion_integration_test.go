//go:build integration

package runtime

import (
	"context"
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk/program"
	"testing"
)

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

