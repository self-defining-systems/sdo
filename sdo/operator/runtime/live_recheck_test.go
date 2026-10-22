package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func restartPortLoop(original *ResponderLoop) *ResponderLoop {
	loop := NewResponderLoop(original.Request, original.Programs, original.Provider, original.Detectors, original.Executor, original.MaxBatches)
	loop.ReadView, loop.Trust = original.ReadView, original.Trust
	loop.CorpusReplayPassed = true
	loop.DiagnosisSettleDelay = time.Millisecond
	return loop
}

func TestRestartFastPathRechecksLiveStateAtEveryCrashPoint(t *testing.T) {
	for _, crash := range []string{"after_write", "after_resolution", "lost_receipt"} {
		for _, liveFault := range []bool{false, true} {
			name := crash + "/fixed"
			if liveFault {
				name = crash + "/fault_present"
			}
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				loop, kube := trustedPortLoop(t, false, true, true)
				request := goldenRequest(t)
				request.Namespace, request.RepairPolicy, request.SourceCommit = "app", "recorded-actions", "tree-a"
				request.Findings = loop.Request.Findings
				loop.Request = request
				dispatcher := KubernetesJobDispatcher{Client: kube, Namespace: "app"}
				attachRecoveryStore(t, dispatcher, loop)
				if crash == "after_write" {
					loop.Executor.Client = &liveCrashClient{PlanObjectClient: loop.Executor.Client}
					func() {
						defer func() {
							if recover() == nil {
								t.Error("crash did not fire")
							}
						}()
						_, _, _, _ = dispatcher.tryFastPath(ctx, request, loop)
					}()
				} else if crash == "lost_receipt" {
					loop.Executor.Client = &recoveryOutageClient{objectClient: loop.Executor.Client, loseReceipt: true}
					_, _, _, _ = dispatcher.tryFastPath(ctx, request, loop)
				} else {
					if _, _, resolved, err := dispatcher.tryFastPath(ctx, request, loop); err != nil || !resolved {
						t.Fatalf("initial: %v %v", resolved, err)
					}
				}
				service, err := kube.CoreV1().Services("app").Get(ctx, "api", metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				if liveFault {
					service.Spec.Ports[0].TargetPort = intstr.FromInt32(9999)
					if _, err = kube.CoreV1().Services("app").Update(ctx, service, metav1.UpdateOptions{}); err != nil {
						t.Fatal(err)
					}
				} else if service.Spec.Ports[0].TargetPort.IntVal != 8080 {
					t.Fatalf("crash fixture did not leave fix: %+v", service)
				}
				restarted := restartPortLoop(loop)
				attachRecoveryStore(t, dispatcher, restarted)
				writes := 0
				restarted.Executor.Client = countingPlanClient{PlanObjectClient: TypedPlanClient{Client: kube}, writes: &writes}
				result, summary, resolved, err := dispatcher.tryFastPath(ctx, request, restarted)
				if err != nil {
					t.Fatal(err)
				}
				if !liveFault {
					if !resolved || len(result.VerificationEvidence) == 0 {
						t.Fatalf("fixed state did not verify: %v %s", resolved, summary)
					}
					if writes != 0 {
						t.Fatalf("restart mutated a correct fix: %d writes", writes)
					}
					noMatch := false
					for _, run := range result.ProgramRuns {
						if run.Verdict.Result == program.NoMatch {
							noMatch = true
						}
					}
					if !noMatch {
						t.Fatal("success without fresh NoMatch")
					}
				} else if resolved && writes == 0 {
					t.Fatal("cached success with live fault")
				}
				service, _ = kube.CoreV1().Services("app").Get(ctx, "api", metav1.GetOptions{})
				if resolved && service.Spec.Ports[0].TargetPort.IntVal != 8080 {
					t.Fatal("success while fault remains")
				}
				if restarted.LoopsUsed() < 1 {
					t.Fatal("restart reset attempt budget")
				}
			})
		}
	}
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

func TestRestartResolvedMarkerRequiresFreshIndependentVerification(t *testing.T) {
	ctx := context.Background()
	loop, kube := trustedPortLoop(t, false, true, true)
	request := goldenRequest(t)
	request.Namespace, request.RepairPolicy, request.SourceCommit = "app", "recorded-actions", "tree-a"
	request.Findings = loop.Request.Findings
	loop.Request = request
	dispatcher := KubernetesJobDispatcher{Client: kube, Namespace: "app"}
	attachRecoveryStore(t, dispatcher, loop)
	if _, _, resolved, err := dispatcher.tryFastPath(ctx, request, loop); err != nil || !resolved {
		t.Fatalf("initial: %v %v", resolved, err)
	}
	restarted := restartPortLoop(loop)
	restarted.Executor.Verifier = planFakeVerifier{passed: false}
	attachRecoveryStore(t, dispatcher, restarted)
	if _, _, resolved, err := dispatcher.tryFastPath(ctx, request, restarted); err != nil || resolved {
		t.Fatalf("stale verification accepted: %v %v", resolved, err)
	}
	service, _ := kube.CoreV1().Services("app").Get(ctx, "api", metav1.GetOptions{})
	if service.Spec.Ports[0].TargetPort.IntVal != 8080 {
		t.Fatal("verification failure undid prior fix")
	}
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
func TestRestartFastPathEscalatesInconclusiveLiveDiagnosis(t *testing.T) {
	for _, verdict := range []program.Result{program.Unknown, program.NeedsJudgement, program.Match, "error"} {
		t.Run(string(verdict), func(t *testing.T) {
			ctx := context.Background()
			loop, kube := trustedPortLoop(t, false, true, true)
			request := goldenRequest(t)
			request.Namespace, request.RepairPolicy, request.SourceCommit = "app", "recorded-actions", "tree-a"
			request.Findings = loop.Request.Findings
			loop.Request = request
			dispatcher := KubernetesJobDispatcher{Client: kube, Namespace: "app"}
			attachRecoveryStore(t, dispatcher, loop)
			if _, _, resolved, err := dispatcher.tryFastPath(ctx, request, loop); err != nil || !resolved {
				t.Fatalf("initial %v %v", resolved, err)
			}
			restarted := restartPortLoop(loop)
			p := restartVerdictProgram{result: verdict, fail: verdict == "error"}
			registry, err := program.NewRegistry([]program.Diagnoser{p}, []program.Mitigator{p})
			if err != nil {
				t.Fatal(err)
			}
			restarted.Programs = registry
			attachRecoveryStore(t, dispatcher, restarted)
			writes := 0
			restarted.Executor.Client = countingPlanClient{PlanObjectClient: restarted.Executor.Client, writes: &writes}
			if _, summary, resolved, err := dispatcher.tryFastPath(ctx, request, restarted); err != nil || resolved || summary == "" {
				t.Fatalf("inconclusive diagnosis accepted: %v %s %v", resolved, summary, err)
			}
			if writes != 0 {
				t.Fatal("restart wrote while diagnosis was inconclusive")
			}
		})
	}
}

func TestResponderRestartRediagnosesWithoutUndoOrRestoredHandles(t *testing.T) {
	for _, faultPresent := range []bool{false, true} {
		t.Run(fmt.Sprint(faultPresent), func(t *testing.T) {
			ctx := context.Background()
			loop, kube := trustedPortLoop(t, false, true, true)
			dispatcher := KubernetesJobDispatcher{Client: kube, Namespace: "app"}
			attachRecoveryStore(t, dispatcher, loop)
			bindings := program.Bindings{"service": {APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}}
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
			state, _, err := NewConfigMapStateStore(kube, "app", IncidentJobName(loop.Request.IncidentID)+"-tools-state").Load(ctx)
			if err != nil {
				t.Fatal(err)
			}
			var facts map[string]any
			if err = json.Unmarshal(state.ResponderToolStateJSON, &facts); err != nil {
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

func TestResponderFailedVerificationRollsBackWithinRun(t *testing.T) {
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
			bindings := program.Bindings{"service": {APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}}
			planned := loop.Handle(context.Background(), ToolRequest{Command: "plan", ProgramID: "service-targetport-mismatch", Bindings: bindings})
			if planned.Error != "" {
				t.Fatal(planned.Error)
			}
			applied := loop.Handle(context.Background(), ToolRequest{Command: "apply", PlanIDs: []string{planned.PlanID}})
			if applied.Error == "" || applied.Healthy {
				t.Fatalf("failed verification accepted: %+v", applied)
			}
			service, _ := kube.CoreV1().Services("app").Get(context.Background(), "api", metav1.GetOptions{})
			if service.Spec.Ports[0].TargetPort.IntVal != 9999 {
				t.Fatal("failed plan was not rolled back within process")
			}
			if apps := loop.PlanApplications(); len(apps) != 1 || !apps[0].RolledBack {
				t.Fatalf("missing rollback outcome: %+v", apps)
			}
		})
	}
}

func TestRestartLegacyMutationRecordsNeverAuthorizeWrites(t *testing.T) {
	ctx := context.Background()
	loop, kube := trustedPortLoop(t, false, true, true)
	dispatcher := KubernetesJobDispatcher{Client: kube, Namespace: "app"}
	attachRecoveryStore(t, dispatcher, loop)
	payload := []byte(`{"Version":1,"IncidentID":"fast","SourceCommit":"tree-a","Batches":2,"FailureSignals":["fast_path_failure:service-targetport-mismatch@revision-a"],"Plans":"invalid retired journal","ApplicationsLog":"invalid retired journal"}`)
	if err := loop.saveToolState(ctx, payload); err != nil {
		t.Fatal(err)
	}
	_, err := kube.CoreV1().ConfigMaps("app").Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: IncidentJobName(loop.Request.IncidentID) + "-fast-path"}, Data: map[string]string{
		"state": "failed", "program_ids": `["service-targetport-mismatch"]`, "trust_failure_signals": `["fast_path_failure:service-targetport-mismatch@revision-a"]`,
		"application": "invalid retired journal", "tool_state": "invalid retired journal", "preimages": "invalid retired journal", "result": "invalid cached success",
	}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for restart := 0; restart < 2; restart++ {
		restarted := restartPortLoop(loop)
		attachRecoveryStore(t, dispatcher, restarted)
		writes := 0
		restarted.Executor.Client = countingPlanClient{PlanObjectClient: restarted.Executor.Client, writes: &writes}
		if _, summary, resolved, err := dispatcher.tryFastPath(ctx, loop.Request, restarted); err != nil || resolved || summary == "" {
			t.Fatalf("legacy handoff: %v %s %v", resolved, summary, err)
		}
		if writes != 0 || len(restarted.PlanApplications()) != 0 || restarted.LoopsUsed() != 2 {
			t.Fatal("legacy journal mutated application or reset budget")
		}
		if !contains(restarted.FailureSignals(), "fast_path_failure:service-targetport-mismatch@revision-a") {
			t.Fatal("lost original revision failure")
		}
	}
}
