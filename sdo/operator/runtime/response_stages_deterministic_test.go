package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sdo.dev/operator/apihistory"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"strings"
)

// observedDeploymentRollout plays the deployment controller for a client-go
// fake, which never populates workload status. A completed wait records the
// observed rollout in the cluster, so the executor and the controller's
// independent closure check see the same rollout state.
type observedDeploymentRollout struct {
	client kubernetes.Interface
	waits  *int
}

func (r observedDeploymentRollout) Wait(ctx context.Context, refs []sdk.ObjectRef) error {
	if r.waits != nil {
		*r.waits++
	}
	for _, ref := range refs {
		if ref.Kind != "Deployment" {
			continue
		}
		deployment, err := r.client.AppsV1().Deployments(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		replicas := int32(1)
		if deployment.Spec.Replicas != nil {
			replicas = *deployment.Spec.Replicas
		}
		deployment.Status = appsv1.DeploymentStatus{ObservedGeneration: deployment.Generation, Replicas: replicas, UpdatedReplicas: replicas, ReadyReplicas: replicas, AvailableReplicas: replicas}
		if _, err := r.client.AppsV1().Deployments(ref.Namespace).UpdateStatus(ctx, deployment, metav1.UpdateOptions{}); err != nil {
			return err
		}
	}
	return nil
}

func pipelineTestLoop(t *testing.T, uncovered bool) (*ResponderLoop, *IncidentState) {
	t.Helper()
	return pipelineTestLoopWithServices(t, uncovered)
}

// pipelineTestLoopWithServices adds drifted Services next to the fixture's
// "api" Service, so one program matches several bindings.
func pipelineTestLoopWithServices(t *testing.T, uncovered bool, extraServices ...string) (*ResponderLoop, *IncidentState) {
	t.Helper()
	client := fastloopClient("good:v1")
	for _, name := range extraServices {
		extra := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app", UID: types.UID("synthetic-" + name), ResourceVersion: "1"}, Spec: corev1.ServiceSpec{Selector: map[string]string{"app": name}, Ports: []corev1.ServicePort{{Port: 80, TargetPort: intstr.FromInt32(9999)}}}}
		if _, err := client.CoreV1().Services("app").Create(context.Background(), extra, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	service, _ := client.CoreV1().Services("app").Get(context.Background(), "api", metav1.GetOptions{})
	service.UID = "synthetic-service"
	service.ResourceVersion = "1"
	if _, err := client.CoreV1().Services("app").Update(context.Background(), service, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	deployment, _ := client.AppsV1().Deployments("app").Get(context.Background(), "api", metav1.GetOptions{})
	deployment.UID = "synthetic-deployment"
	deployment.ResourceVersion = "1"
	if _, err := client.AppsV1().Deployments("app").Update(context.Background(), deployment, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	fastloopPodWaiting(t, context.Background(), client, "api-pod", "ImagePullBackOff")
	provider := fastloopObjects{client: client}
	detector := pipelineDetector{}
	snapshot, err := provider.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	findings, err := detector.Detect(context.Background(), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	diagnosers := []program.Diagnoser{pipelinePortProgram{}, pipelineImageProgram{}}
	mitigators := []program.Mitigator{pipelinePortProgram{}, pipelineImageProgram{}}
	if uncovered {
		diagnosers = diagnosers[:1]
		mitigators = mitigators[:1]
	}
	registry, err := program.NewRegistry(diagnosers, mitigators)
	if err != nil {
		t.Fatal(err)
	}
	request := goldenRequest(t)
	request.Namespace = "app"
	request.Findings = findings
	request.RepairPolicy = "recorded-actions"
	request.ResponseDeadline = time.Now().Add(time.Minute)
	loop := NewResponderLoop(request, registry, provider, []sdk.Detector{detector}, PlanExecutor{Client: TypedPlanClient{Client: client}, Source: pipelineAcceptedSource{client: TypedPlanClient{Client: client}}, Rollouts: observedDeploymentRollout{client: client}, Verifier: PlanHealthVerifier{Detectors: []sdk.Detector{detector}, Provider: provider}}, 5)
	loop.ReadView = func(v sdk.DetectionContext) program.ReadView {
		return &sdktest.ReplayReadView{Snapshot: v.(sdktest.Snapshot)}
	}
	request.InitialProgramRuns = loop.InitialDiagnose(context.Background())
	request.PlaybookCoverage = buildPlaybookCoverage(findings, request.InitialProgramRuns, loop.lastDiagnosisView, nil)
	loop.Request = request
	state := &IncidentState{Request: request, Findings: findings, Coverage: request.PlaybookCoverage}
	// Ensure mutation testing observes a real client write, not just state mutation.
	t.Cleanup(func() {
		service, e := client.CoreV1().Services("app").Get(context.Background(), "api", metav1.GetOptions{})
		if e != nil || service == nil {
			t.Errorf("lost real service: %v", e)
		}
	})
	return loop, state
}

func TestDeterministicResponseStageClearsBothFaultsWithoutAgent(t *testing.T) {
	loop, state := pipelineTestLoop(t, false)
	calls := 0
	stages := []ResponseStage{&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}, clearingResponseStage{"agent", &calls}}
	if err := RunResponseStages(context.Background(), state, stages); err != nil {
		t.Fatal(err)
	}
	if len(state.Findings) != 0 || calls != 0 || len(state.PriorStages) != 1 {
		t.Fatalf("state=%+v, agent=%d", state, calls)
	}
	report := state.PriorStages[0]
	if len(report.Mitigations) != 2 {
		t.Fatalf("report: %v", report)
	}
	for _, attempt := range report.Mitigations {
		if attempt.Result != "applied_cleared" || !attempt.ApplyReceipt.GetApplied() || len(attempt.ClearedFindingFingerprints) != 1 {
			t.Fatalf("attempt: %v", attempt)
		}
	}
	if len(loop.PlanApplications()) != 1 {
		t.Fatalf("real executor not used: %v", loop.PlanApplications())
	}
}

// The stage's applies are not the agent's mitigation rounds: however many
// matched plans the stage applies, the agent starts with its full budget.
func TestDeterministicResponseStageDoesNotSpendAgentRounds(t *testing.T) {
	loop, state := pipelineTestLoop(t, false)
	report, err := (&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}).Run(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Mitigations) < 2 || len(loop.PlanApplications()) == 0 {
		t.Fatalf("fixture must apply several matched plans in the stage: %v", report)
	}
	if loop.LoopsUsed() != 0 || loop.Exhausted() {
		t.Fatalf("stage applies spent the agent's rounds: used=%d of %d", loop.LoopsUsed(), loop.batchLimit())
	}
	response := loop.Handle(context.Background(), ToolRequest{Command: "status"})
	if response.MitigationRoundsUsed != 0 || response.MitigationRoundsAllowed != 5 {
		t.Fatalf("agent budget at hand-off: %+v", response)
	}
}

// Application IDs stay unique across the stage and the agent, including after a
// restart that restores the persisted state, and the agent still gets exactly
// its five rounds.
func TestStageAndAgentApplicationsShareOneIDSequence(t *testing.T) {
	loop, state := pipelineTestLoop(t, false)
	var saved []byte
	loop.saveToolState = func(_ context.Context, payload []byte) error { saved = payload; return nil }
	if _, err := (&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}).Run(context.Background(), state); err != nil {
		t.Fatal(err)
	}
	used := map[string]bool{}
	for _, application := range loop.PlanApplications() {
		used[application.ID] = true
	}
	restored := NewResponderLoop(loop.Request, loop.Programs, loop.Provider, loop.Detectors, loop.Executor, 5)
	restored.ReadView = loop.ReadView
	if err := restored.restoreToolSnapshot(saved); err != nil {
		t.Fatal(err)
	}
	if restored.LoopsUsed() != 0 || restored.applySeq != loop.applySeq {
		t.Fatalf("restored accounting differs: used=%d seq=%d want seq=%d", restored.LoopsUsed(), restored.applySeq, loop.applySeq)
	}
	service := program.Bindings{Objects: map[string]sdk.ObjectRef{"service": {APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}}}
	for round := 1; round <= 6; round++ {
		plan := restored.Handle(context.Background(), ToolRequest{Command: "plan", ProgramID: "service-targetport-mismatch", Bindings: service})
		if plan.Error != "" {
			t.Fatalf("round %d plan: %s", round, plan.Error)
		}
		applied := restored.Handle(context.Background(), ToolRequest{Command: "apply", PlanIDs: []string{plan.PlanID}})
		if round == 6 {
			if applied.Error != "response loop budget exhausted" {
				t.Fatalf("the sixth agent round must be refused: %+v", applied)
			}
			break
		}
		if applied.Error != "" || applied.ApplicationID == "" || used[applied.ApplicationID] || applied.MitigationRoundsUsed != round {
			t.Fatalf("round %d reused an application ID or miscounted: %+v (used %v)", round, applied, used)
		}
		used[applied.ApplicationID] = true
	}
	agentApplies := 0
	for _, call := range restored.ToolCalls() {
		if call.Command == "apply" && call.Error == "" && !strings.Contains(call.RequestJSON, `"stage":true`) {
			agentApplies++
		}
	}
	if agentApplies != 5 {
		t.Fatalf("agent apply records: %d", agentApplies)
	}
	for _, call := range loop.ToolCalls() {
		if call.Command == "apply" && !strings.Contains(call.RequestJSON, `"stage":true`) {
			t.Fatalf("the stage's apply record lost its stage marker: %s", call.RequestJSON)
		}
	}
	var forged ToolRequest
	if err := json.Unmarshal([]byte(`{"command":"apply","plan_ids":["x"],"stage":true}`), &forged); err != nil || forged.stage {
		t.Fatalf("a tool call must not be able to set the stage marker: %+v, %v", forged, err)
	}
}

// The stage sends all its matched plans in one apply with one rollout wait:
// one round of stage work whatever the number of bindings, each binding's plan,
// receipt range and result attributable, and the agent still has all 5 rounds.
func TestDeterministicResponseStageAppliesMatchedBindingsOnce(t *testing.T) {
	loop, state := pipelineTestLoopWithServices(t, false, "api2")
	waits := 0
	rollouts := loop.Executor.Rollouts.(observedDeploymentRollout)
	rollouts.waits = &waits
	loop.Executor.Rollouts = rollouts
	report, err := (&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}).Run(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	applications := loop.PlanApplications()
	if len(report.Mitigations) != 3 || len(applications) != 1 || waits != 1 {
		t.Fatalf("want 3 attempts, 1 application, 1 rollout wait; got %d, %d, %d: %v", len(report.Mitigations), len(applications), waits, report)
	}
	if loop.LoopsUsed() != 0 || loop.Exhausted() {
		t.Fatalf("agent rounds spent: %d", loop.LoopsUsed())
	}
	if status := loop.Handle(context.Background(), ToolRequest{Command: "status"}); status.MitigationRoundsUsed != 0 || status.MitigationRoundsAllowed != 5 {
		t.Fatalf("agent budget at hand-off: %+v", status)
	}
	planIDs, starts := map[string]bool{}, map[int32]bool{}
	for _, attempt := range report.Mitigations {
		receipt := attempt.ApplyReceipt
		if attempt.Result != "applied_cleared" || receipt.GetApplicationId() != applications[0].ID || attempt.PlanId == "" || planIDs[attempt.PlanId] || attempt.PlanActionCount != 1 || starts[attempt.PlanActionStart] {
			t.Fatalf("attempt not attributable: %v", attempt)
		}
		planIDs[attempt.PlanId], starts[attempt.PlanActionStart] = true, true
		ref, err := planActionRef(planActionState(receipt.GetPlan().GetActions()[attempt.PlanActionStart]))
		if err != nil {
			t.Fatal(err)
		}
		bound := programBindingsState(attempt.Bindings)
		if want := bound.Objects["service"]; want.Name != "" && ref != want {
			t.Fatalf("attempt %s owns action on %v, bound to %v", attempt.PlanId, ref, want)
		}
		if want := bound.Objects["deployment"]; want.Name != "" && ref != want {
			t.Fatalf("attempt %s owns action on %v, bound to %v", attempt.PlanId, ref, want)
		}
	}
	// Real producer into the real consumer: the outcome derivation.
	state.PriorStages = []*contracts.StageReport{report}
	result, err := responsePipelineResult(state, time.Now(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(result.PlanApplications) != 1 || len(result.PlanApplications[0].PlanIDs) != 3 || len(result.AppliedPlaybooks) != 3 || len(result.ConfirmedRootCauses) != 3 {
		t.Fatalf("outcome lost a binding: applications=%v playbooks=%v causes=%v", result.PlanApplications, result.AppliedPlaybooks, result.ConfirmedRootCauses)
	}
}

func TestDeterministicResponseStageHandsOffCoveredAndUncoveredEvidence(t *testing.T) {
	loop, state := pipelineTestLoop(t, true)
	report, err := (&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}).Run(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Findings) != 1 || len(report.Mitigations) != 1 || report.Mitigations[0].Result != "applied_cleared" {
		t.Fatalf("report=%v, findings=%v", report, state.Findings)
	}
	state.PriorStages = []*contracts.StageReport{report}
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	_, err = (AgentResponseStage{Dispatcher: dispatcher}).Run(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	request := <-dispatcher.requests
	if len(request.PriorStages) != 1 || len(request.Findings) != 1 || request.PriorStages[0].Mitigations[0].Result != "applied_cleared" {
		t.Fatalf("agent request: %+v", request)
	}
}

type pipelinePortProgram struct{ fastloopPortProgram }

func (pipelinePortProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "service-targetport-mismatch", Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}}}
}
func (pipelinePortProgram) Mitigate(_ context.Context, _ program.ReadView, b program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: []program.Action{program.SetField{Ref: b.Objects["service"], Path: "spec.ports[0].targetPort", Value: program.FromSource(b.Objects["service"], "spec.ports[0].targetPort"), Reason: "restore accepted port"}}}, nil
}

type pipelineImageProgram struct{ fastloopImageProgram }

func (pipelineImageProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "image-pull-failure", Params: []program.ParamSpec{{Name: "deployment", Kind: "Deployment", Required: true}}}
}
func (pipelineImageProgram) Mitigate(_ context.Context, _ program.ReadView, b program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: []program.Action{program.SetField{Ref: b.Objects["deployment"], Path: "spec.template.spec.containers[0].image", Value: program.FromSource(b.Objects["deployment"], "spec.template.spec.containers[0].image"), Reason: "restore accepted image"}}}, nil
}

type pipelineAcceptedSource struct{ client TypedPlanClient }

func (s pipelineAcceptedSource) AcceptedManifest(ctx context.Context, _ string, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	object, err := s.client.Get(ctx, ref)
	if err != nil {
		return nil, err
	}
	switch ref.Kind {
	case "Service":
		services, err := s.client.Client.CoreV1().Services(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		services.APIVersion = "v1"
		services.Kind = "Service"
		services.Spec.Ports[0].TargetPort.IntVal = 8080
		object.Object, err = runtime.DefaultUnstructuredConverter.ToUnstructured(services)
		return object, err
	case "Deployment":
		deployment, err := s.client.Client.AppsV1().Deployments(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		deployment.APIVersion = "apps/v1"
		deployment.Kind = "Deployment"
		deployment.Spec.Template.Spec.Containers[0].Image = "good:v1"
		object.Object, err = runtime.DefaultUnstructuredConverter.ToUnstructured(deployment)
		return object, err
	}
	return nil, fmt.Errorf("no synthetic accepted manifest for %v", ref)
}
func (s pipelineAcceptedSource) Revision(context.Context, sdk.ObjectRef, string) (*unstructured.Unstructured, error) {
	return nil, fmt.Errorf("no revision fixture")
}

type pipelineIneffectivePortProgram struct{ pipelinePortProgram }

func (pipelineIneffectivePortProgram) Mitigate(_ context.Context, _ program.ReadView, b program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: []program.Action{program.SetField{Ref: b.Objects["service"], Path: "spec.ports[0].targetPort", Value: program.FromObserved(b.Objects["service"], "spec.ports[0].targetPort"), Reason: "synthetic ineffective repair"}}}, nil
}

func TestDeterministicResponseStageStillMatchingIsRetainedAndReported(t *testing.T) {
	loop, state := pipelineTestLoop(t, true)
	registry, err := program.NewRegistry([]program.Diagnoser{pipelineIneffectivePortProgram{}}, []program.Mitigator{pipelineIneffectivePortProgram{}})
	if err != nil {
		t.Fatal(err)
	}
	loop.Programs = registry
	report, err := (&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}).Run(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	attempt := report.Mitigations[0]
	if attempt.Result != "applied_still_matching" || attempt.ApplyReceipt.RolledBack || attempt.PostApplyDiagnose.Verdict.Result != contracts.Verdict_RESULT_MATCH || len(state.Findings) != 2 {
		t.Fatalf("report=%v, findings=%v", report, state.Findings)
	}
	if len(loop.PlanApplications()) != 1 || loop.PlanApplications()[0].RolledBack {
		t.Fatalf("automatic rollback: %+v", loop.PlanApplications())
	}
}

type pipelineDuplicateRegistry struct{ program.ProgramRegistry }

func (r pipelineDuplicateRegistry) List() []program.ProgramSpec {
	specs := r.ProgramRegistry.List()
	return append(specs, specs[0])
}

func TestDeterministicResponseStageTypedSkipsNeverWrite(t *testing.T) {
	for _, reason := range []string{"diagnose_unavailable", "multiple_entry_points", "diagnose_no_longer_matches"} {
		t.Run(reason, func(t *testing.T) {
			loop, state := pipelineTestLoop(t, true)
			switch reason {
			case "diagnose_unavailable":
				state.Coverage.Matches[0].Bindings = nil
			case "multiple_entry_points":
				loop.Programs = pipelineDuplicateRegistry{loop.Programs}
			case "diagnose_no_longer_matches":
				client := loop.Executor.Client.(TypedPlanClient).Client
				service, err := client.CoreV1().Services("app").Get(context.Background(), "api", metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				service.Spec.Ports[0].TargetPort.IntVal = 8080
				if _, err := client.CoreV1().Services("app").Update(context.Background(), service, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			report, err := (&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}).Run(context.Background(), state)
			if err != nil {
				t.Fatal(err)
			}
			if len(report.Mitigations) != 1 || report.Mitigations[0].Result != "skipped" || report.Mitigations[0].SkipReason != reason || len(loop.PlanApplications()) != 0 {
				t.Fatalf("skip %s: %v applications=%v", reason, report, loop.PlanApplications())
			}
		})
	}
}

func TestDeterministicResponseStageRejoinDoesNotRepeatWrites(t *testing.T) {
	loop, state := pipelineTestLoop(t, true)
	var durable []byte
	loop.saveToolState = func(_ context.Context, payload []byte) error { durable = append([]byte(nil), payload...); return nil }
	stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}
	first, err := stage.Run(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	fresh := NewResponderLoop(loop.Request, loop.Programs, loop.Provider, loop.Detectors, loop.Executor, 5)
	fresh.ReadView = loop.ReadView
	if err := fresh.restoreToolSnapshot(durable); err != nil {
		t.Fatal(err)
	}
	opening := state.Request
	opening.PlaybookCoverage, opening.InitialProgramRuns = nil, nil
	replay := &IncidentState{Request: opening, Findings: append([]sdk.Finding(nil), state.Request.Findings...)}
	second, err := (&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return fresh }}).Run(context.Background(), replay)
	if err != nil {
		t.Fatal(err)
	}
	if len(fresh.PlanApplications()) != 0 || !proto.Equal(first, second) || len(replay.Findings) != 1 || !proto.Equal(state.Coverage, replay.Coverage) || len(replay.Request.InitialProgramRuns) != len(state.Request.InitialProgramRuns) {
		t.Fatalf("rejoin repeated/lost stage: first=%v second=%v applications=%v", first, second, fresh.PlanApplications())
	}
}

type pipelineDetector struct{ coverageThreeFaultDetector }

func (pipelineDetector) Spec() sdk.DetectorSpec {
	spec := (fastloopDetector{}).Spec()
	spec.Playbooks = []string{"image-pull-failure", "service-targetport-mismatch"}
	return spec
}

func TestDeterministicResponseStageInterruptedCompletionNeverReplaysMutation(t *testing.T) {
	loop, state := pipelineTestLoop(t, true)
	var durable []byte
	loop.saveToolState = func(_ context.Context, payload []byte) error {
		var progress responderToolState
		if err := json.Unmarshal(payload, &progress); err != nil {
			return err
		}
		for _, stage := range progress.StageProgress {
			for _, attempt := range stage.Report.Mitigations {
				if attempt.ApplyReceipt.GetApplied() {
					return errors.New("injected completed-checkpoint failure")
				}
			}
		}
		durable = append([]byte(nil), payload...)
		return nil
	}
	_, err := (&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}).Run(context.Background(), state)
	if err == nil {
		t.Fatal("injected checkpoint failure was ignored")
	}
	fresh := NewResponderLoop(loop.Request, loop.Programs, loop.Provider, loop.Detectors, loop.Executor, 5)
	fresh.ReadView = loop.ReadView
	if err := fresh.restoreToolSnapshot(durable); err != nil {
		t.Fatal(err)
	}
	replay := &IncidentState{Request: state.Request, Findings: append([]sdk.Finding(nil), state.Request.Findings...), Coverage: state.Coverage}
	report, err := (&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return fresh }}).Run(context.Background(), replay)
	if err != nil {
		t.Fatal(err)
	}
	if len(fresh.PlanApplications()) != 0 || len(report.Mitigations) != 1 || report.Mitigations[0].Result != "apply_error" || report.Mitigations[0].Error == "" || len(replay.Findings) != 1 {
		t.Fatalf("uncertain mutation replayed or hidden: %v, applications=%v, findings=%v", report, fresh.PlanApplications(), replay.Findings)
	}
}

func TestDeterministicResponseStageRespectsClosedIncidentMutationFence(t *testing.T) {
	loop, state := pipelineTestLoop(t, true)
	gate, err := apihistory.PrepareMutationGate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	guard, err := gate.TryObserve()
	if err != nil {
		t.Fatal(err)
	}
	if err := guard.Fence(time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := guard.Close(); err != nil {
		t.Fatal(err)
	}
	loop.MutationGate = gate
	report, err := (&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}).Run(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	if len(loop.PlanApplications()) != 0 || len(report.Mitigations) != 1 || report.Mitigations[0].Result != "apply_error" || report.Mitigations[0].Error != "incident_verified" {
		t.Fatalf("automatic apply bypassed ordinary mutation fence: %v", report)
	}
}

type pipelineDeadlineRollout struct {
	observed time.Duration
	failure  error
}

func (r *pipelineDeadlineRollout) Wait(ctx context.Context, _ []sdk.ObjectRef) error {
	deadline, ok := ctx.Deadline()
	if !ok {
		return errors.New("missing stage rollout deadline")
	}
	r.observed = time.Until(deadline)
	return r.failure
}

func TestDeterministicResponseStageRolloutResultsKeepWritesAndCapOwnWait(t *testing.T) {
	for _, pending := range []bool{false, true} {
		t.Run(fmt.Sprint(pending), func(t *testing.T) {
			loop, state := pipelineTestLoop(t, true)
			rollout := &pipelineDeadlineRollout{failure: errors.New("synthetic rollout failed")}
			want := "rollout_failed"
			if pending {
				rollout.failure = context.DeadlineExceeded
				want = "rollout_pending_at_handoff"
			}
			loop.Executor.Rollouts = rollout
			report, err := (&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}).Run(context.Background(), state)
			if err != nil {
				t.Fatal(err)
			}
			attempt := report.Mitigations[0]
			if attempt.Result != want || !attempt.ApplyReceipt.Applied || attempt.ApplyReceipt.RolledBack || attempt.PostApplyDiagnose == nil || rollout.observed <= 0 || rollout.observed > DefaultStageBudget {
				t.Fatalf("stage rollout result %s: %v, wait=%s", want, report, rollout.observed)
			}
		})
	}
}

func TestResponseStageNoMatchProofBindsReceiptRevisionAndBindings(t *testing.T) {
	loop, state := pipelineTestLoop(t, true)
	report, err := (&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}).Run(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	attempt := report.Mitigations[0]
	record := &contracts.OutcomeRecord{SourceCommit: state.Request.SourceCommit, PriorStages: []*contracts.StageReport{report}, ProgramRuns: []*contracts.ProgramRun{attempt.PostApplyDiagnose}}
	if !boundPostApplyNoMatch(record, attempt.ApplyReceipt, attempt.PlaybookId, attempt.ProgramRevision) {
		t.Fatal("controller stage proof was ignored without legacy tool transcript")
	}
	for _, mutate := range []func(*contracts.StageMitigation){func(a *contracts.StageMitigation) { a.ProgramRevision = "other" }, func(a *contracts.StageMitigation) { a.ApplyReceipt.ApplicationId = "other" }, func(a *contracts.StageMitigation) { a.Bindings = nil }, func(a *contracts.StageMitigation) {
		a.PostApplyDiagnose.Verdict.Result = contracts.Verdict_RESULT_MATCH
	}} {
		changed := proto.Clone(record).(*contracts.OutcomeRecord)
		mutate(changed.PriorStages[0].Mitigations[0])
		if boundPostApplyNoMatch(changed, attempt.ApplyReceipt, attempt.PlaybookId, attempt.ProgramRevision) {
			t.Fatalf("unbound stage proof granted credit: %v", changed.PriorStages)
		}
	}
}

func TestResponseStageFreshNoMatchRemovesStaleFindingsBeforeAgent(t *testing.T) {
	loop, state := pipelineTestLoop(t, false)
	client := loop.Executor.Client.(TypedPlanClient).Client
	service, err := client.CoreV1().Services("app").Get(context.Background(), "api", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	service.Spec.Ports[0].TargetPort.IntVal = 8080
	if _, err := client.CoreV1().Services("app").Update(context.Background(), service, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	deployment, err := client.AppsV1().Deployments("app").Get(context.Background(), "api", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	deployment.Spec.Template.Spec.Containers[0].Image = "good:v1"
	if _, err := client.AppsV1().Deployments("app").Update(context.Background(), deployment, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	calls := 0
	if err := RunResponseStages(context.Background(), state, []ResponseStage{&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}, clearingResponseStage{"agent", &calls}}); err != nil {
		t.Fatal(err)
	}
	if calls != 0 || len(state.Findings) != 0 || len(loop.PlanApplications()) != 0 || state.PriorStages[0].Mitigations[0].SkipReason != "diagnose_no_longer_matches" {
		t.Fatalf("stale work was dispatched after live NoMatch: calls=%d reports=%v", calls, state.PriorStages)
	}
}
