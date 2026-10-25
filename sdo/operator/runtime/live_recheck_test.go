package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"sdo.dev/operator/runtime/internal/lifecycleio"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func restartPortLoop(original *ResponderLoop) *ResponderLoop {
	loop := NewResponderLoop(original.Request, original.Programs, original.Provider, original.Detectors, original.Executor, original.MaxBatches)
	loop.ReadView, loop.Trust = original.ReadView, original.Trust
	return loop
}

type countingPlanClient struct {
	PlanObjectClient
	writes *int
}

func (c countingPlanClient) Patch(ctx context.Context, ref sdk.ObjectRef, kind types.PatchType, patch []byte) error {
	*c.writes++
	return c.PlanObjectClient.Patch(ctx, ref, kind, patch)
}

type liveCrashClient struct{ PlanObjectClient }

func (c *liveCrashClient) Patch(ctx context.Context, ref sdk.ObjectRef, kind types.PatchType, patch []byte) error {
	if err := c.PlanObjectClient.Patch(ctx, ref, kind, patch); err != nil {
		return err
	}
	panic("crash after write")
}

type restartVerdictProgram struct {
	fastloopPortProgram
	result program.Result
	fail   bool
}

func (p restartVerdictProgram) Diagnose(context.Context, program.ReadView, program.Bindings) (program.Verdict, error) {
	if p.fail {
		return program.Verdict{}, fmt.Errorf("diagnosis unavailable")
	}
	verdict := program.Verdict{Result: p.result}
	if p.result == program.NeedsJudgement {
		verdict.Question = "is this safe?"
	}
	return verdict, nil
}
func TestResponderRestartRediagnosesWithoutUndoOrRestoredHandles(t *testing.T) {
	for _, faultPresent := range []bool{false, true} {
		t.Run(fmt.Sprint(faultPresent), func(t *testing.T) {
			ctx := context.Background()
			loop, kube := trustedPortLoop(t, false, true, true)
			dispatcher := KubernetesJobDispatcher{Client: kube, Namespace: "app"}
			attachRecoveryStore(t, dispatcher, loop)
			bindings := program.Bindings{Objects: program.ObjectBindings{"service": {APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}}}
			planned := loop.Handle(ctx, ToolRequest{Command: "plan", ProgramID: "service-targetport-mismatch", Bindings: bindings})
			if planned.Error != "" {
				t.Fatal(planned.Error)
			}
			loop.Executor.Client = &liveCrashClient{PlanObjectClient: loop.Executor.Client}
			func() {
				defer func() {
					if recover() == nil {
						t.Error("crash did not fire")
					}
				}()
				loop.Handle(ctx, ToolRequest{Command: "apply", PlanIDs: []string{planned.PlanID}})
			}()
			if faultPresent {
				service, _ := kube.CoreV1().Services("app").Get(ctx, "api", metav1.GetOptions{})
				service.Spec.Ports[0].TargetPort = intstr.FromInt32(9999)
				if _, err := kube.CoreV1().Services("app").Update(ctx, service, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			restarted := restartPortLoop(loop)
			restarted.Executor.Client = TypedPlanClient{Client: kube}
			attachRecoveryStore(t, dispatcher, restarted)
			if len(restarted.PlanApplications()) != 0 || restarted.LoopsUsed() != 1 {
				t.Fatal("restored applications or reset budget")
			}
			old := restarted.Handle(ctx, ToolRequest{Command: "apply", PlanIDs: []string{planned.PlanID}})
			if old.Error == "" {
				t.Fatal("old plan survived restart")
			}
			diagnosed := restarted.Handle(ctx, ToolRequest{Command: "diagnose", ProgramID: "service-targetport-mismatch", Bindings: bindings})
			expected := program.NoMatch
			if faultPresent {
				expected = program.Match
			}
			if diagnosed.Error != "" || len(diagnosed.ProgramRuns) != 1 || diagnosed.ProgramRuns[0].Verdict.Result != expected {
				t.Fatalf("live diagnosis: %+v", diagnosed)
			}
			if faultPresent {
				planned = restarted.Handle(ctx, ToolRequest{Command: "plan", ProgramID: "service-targetport-mismatch", Bindings: bindings})
				applied := restarted.Handle(ctx, ToolRequest{Command: "apply", PlanIDs: []string{planned.PlanID}})
				if applied.Error != "" || !applied.Healthy {
					t.Fatalf("fresh repair failed: %+v", applied)
				}
			}
			verified := restarted.Handle(ctx, ToolRequest{Command: "verify"})
			if !verified.Healthy {
				t.Fatalf("live verification: %+v", verified)
			}
			state, _, err := lifecycleio.NewConfigMapStateStore(kube, "app", IncidentJobName(loop.Request.IncidentID)+"-tools-state").Load(ctx)
			if err != nil {
				t.Fatal(err)
			}
			var facts map[string]any
			if err = json.Unmarshal(state.ResponderToolStateJson, &facts); err != nil {
				t.Fatal(err)
			}
			for _, field := range []string{"Plans", "Applications", "ApplicationsLog", "Calls", "Runs", "Findings"} {
				if _, exists := facts[field]; exists {
					t.Fatalf("persisted mutation journal: %s", field)
				}
			}
		})
	}
}

func TestResponderFailedVerificationIsRecordedAndLeftInPlace(t *testing.T) {
	for _, diagnosisFails := range []bool{false, true} {
		t.Run(fmt.Sprint(diagnosisFails), func(t *testing.T) {
			loop, kube := trustedPortLoop(t, false, true, diagnosisFails)
			if diagnosisFails {
				p := restartVerdictProgram{result: program.Match}
				registry, err := program.NewRegistry([]program.Diagnoser{p}, []program.Mitigator{p})
				if err != nil {
					t.Fatal(err)
				}
				loop.Programs = registry
			}
			bindings := program.Bindings{Objects: program.ObjectBindings{"service": {APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}}}
			planned := loop.Handle(context.Background(), ToolRequest{Command: "plan", ProgramID: "service-targetport-mismatch", Bindings: bindings})
			if planned.Error != "" {
				t.Fatal(planned.Error)
			}
			applied := loop.Handle(context.Background(), ToolRequest{Command: "apply", PlanIDs: []string{planned.PlanID}})
			if applied.Error != "" {
				t.Fatalf("apply must report, not fail: %+v", applied)
			}
			service, _ := kube.CoreV1().Services("app").Get(context.Background(), "api", metav1.GetOptions{})
			if service.Spec.Ports[0].TargetPort.IntVal == 9999 {
				t.Fatal("the responder, not apply, decides to roll back")
			}
			if apps := loop.PlanApplications(); len(apps) != 1 || apps[0].RolledBack || apps[0].Result.VerificationPassed {
				t.Fatalf("an unverified application must be recorded as such and kept: %+v", apps)
			}
		})
	}
}
