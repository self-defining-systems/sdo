package runtime

import (
	"context"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/requestmetrics"
	"sdo.dev/operator/sdk/sdktest"
)

// e2eChurn is routine churn: a Kubernetes condition that opens the incident
// and clears on its own when the switch is turned off.
type e2eChurn struct{ on atomic.Bool }

func (d *e2eChurn) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: memory.KubernetesConditionDetectorIDs()[0], Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge,
		Interval: time.Second, Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1},
		Batching: sdk.BatchingPolicy{Severity: sdk.SeverityCritical}, OriginatingCommit: "synthetic"}
}

func (d *e2eChurn) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	if !d.on.Load() {
		return []sdk.Finding{}, nil
	}
	finding := sdk.Finding{DetectorID: d.Spec().ID, RuleID: "workload-container-not-ready", Status: sdk.FindingActive, Severity: sdk.SeverityCritical,
		Summary: "worker not ready", Evidence: "synthetic", PrimaryResource: sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "worker-pod"}}
	finding.Fingerprint = FindingFingerprint(finding)
	return []sdk.Finding{finding}, nil
}

// e2eRoute is the real-traffic alert of the fault's own route. It fires from
// the moment the fault shows until the component's restart landed.
type e2eRoute struct{ shown atomic.Bool }

func (d *e2eRoute) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: memory.RequestMetricsDetectorID, Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge,
		Watches:  []sdk.WatchKind{{APIVersion: "apps/v1", Kind: "Deployment"}},
		Interval: time.Second, Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1},
		Batching: sdk.BatchingPolicy{Severity: sdk.SeverityWarning}, OriginatingCommit: "synthetic", Playbooks: []string{"fix"}}
}

func (d *e2eRoute) Detect(_ context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	if !d.shown.Load() {
		return []sdk.Finding{}, nil
	}
	for _, deployment := range view.Deployments() {
		if deployment.Name == "api" && deployment.Spec.Template.Annotations["kubectl.kubernetes.io/restartedAt"] == "" {
			ref := sdk.ObjectRefFrom("Deployment", "apps/v1", &deployment)
			finding := sdk.Finding{DetectorID: d.Spec().ID, RuleID: requestmetrics.RuleErrors, Status: sdk.FindingActive, Severity: sdk.SeverityWarning,
				Summary: "operation commit-file: 9 of 20 requests failed", Evidence: "synthetic", PrimaryResource: ref,
				Playbooks: []string{"fix"}, ParameterBindings: map[string]sdk.ObjectRef{ComponentRole: ref}}
			finding.Fingerprint = FindingFingerprint(finding)
			return []sdk.Finding{finding}, nil
		}
	}
	return []sdk.Finding{}, nil
}

func e2eWorld() *sameWriteWorld {
	replicas := int32(1)
	var objects []kruntime.Object
	for _, name := range []string{"api", "worker"} {
		labels := map[string]string{"app": name}
		objects = append(objects,
			&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app", UID: types.UID("synthetic-" + name), Generation: 1}, Spec: appsv1.DeploymentSpec{Replicas: &replicas, Selector: &metav1.LabelSelector{MatchLabels: labels}, Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: name, Image: name + ":v1"}}}}}, Status: appsv1.DeploymentStatus{ObservedGeneration: 1, Replicas: 1, UpdatedReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1}},
			&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name + "-pod", Namespace: "app", Labels: labels}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: name, Image: name + ":v1"}}}, Status: corev1.PodStatus{Phase: corev1.PodRunning}})
	}
	world := &sameWriteWorld{client: fake.NewSimpleClientset(objects...)}
	world.client.PrependReactor("*", "deployments", func(action ktesting.Action) (bool, kruntime.Object, error) {
		if action.GetSubresource() == "" && (action.GetVerb() == "update" || action.GetVerb() == "patch") {
			world.restarts.Add(1)
		}
		return false, nil, nil
	})
	return world
}

// A linked symptom that appears after handoff can make a previously
// non-matching program applicable. The finding still joins the alert set,
// but the responder owns the repair.
func TestLearnedPlaybookDoesNotWriteAfterHandoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	now := time.Unix(1000, 0).UTC()
	world := e2eWorld()
	churn, route := &e2eChurn{}, &e2eRoute{}
	churn.on.Store(true)
	detectors := []sdk.Detector{churn, route}
	restart := componentRestart{id: "fix"}
	registry, err := program.NewRegistry([]program.Diagnoser{restart}, []program.Mitigator{restart})
	if err != nil {
		t.Fatal(err)
	}
	cfg := testControllerConfig()
	cfg.Namespace = "app"
	cfg.ClosureStabilityWindow = 3 * time.Second
	cfg.RequestMetricsOpens = true
	dispatcher := &recordingDispatcher{}
	c, err := NewController(cfg, detectors, world, dispatcher, now)
	if err != nil {
		t.Fatal(err)
	}
	c.now = func() time.Time { return now }
	snapshot, err := world.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	churnFinding, _ := churn.Detect(ctx, snapshot)
	request := IncidentRequest{IncidentID: "synthetic-e2e", Application: "demo", Namespace: "app", SourceCommit: "synthetic", RepairPolicy: "recorded-actions",
		Findings: churnFinding, OpeningKeys: findingKeys(churnFinding), ResponseDeadline: lifecycleio.Now().Add(time.Hour)}
	executor := PlanExecutor{Client: TypedPlanClient{Client: world.client}, Source: world, Rollouts: world, Verifier: PlanHealthVerifier{Detectors: detectors, Provider: world}}
	request.InitialProgramRuns = []ProgramRun{{ProgramID: "fix", Verdict: program.Verdict{Result: program.NoMatch}}}
	request.PriorStages = []*contracts.StageReport{{Stage: "deterministic", HandoffSelected: true}}
	loop := NewResponderLoop(request, registry, world, detectors, executor, 5)
	loop.Now = c.now
	loop.ReadView = func(v sdk.DetectionContext) program.ReadView {
		return &sdktest.ReplayReadView{Snapshot: v.(sdktest.Snapshot)}
	}
	loop.saveToolState = func(context.Context, []byte) error { return nil }
	instant := newInstantRecoveryClock()
	c.tuneMidStage = func(stage *DeterministicResponseStage) {
		stage.diagnoseWait = instant.wait
		stage.RolloutTimeout = time.Minute
	}
	c.codePath = &CodePath{Selection: CodePathSelection{ResponseStages: []string{"deterministic", "agent"}}}
	c.responderLoop = loop
	c.incidentOpen, c.dispatchState = true, contracts.DispatchState_DISPATCH_STATE_RUNNING
	c.currentIncidentRequest = &request
	c.incidentFindingKeys = findingKeys(churnFinding)
	c.incidentDetectedAt, c.incidentDispatchedAt = now, now
	closed := new(IncidentClosure)
	c.OnIncidentClosed = func(closure IncidentClosure) { *closed = closure }

	step := func(count int) {
		for range count {
			now = now.Add(time.Second)
			if err := c.Step(ctx, now, nil); err != nil {
				t.Fatal(err)
			}
			c.Wait()
		}
	}
	step(3)
	// The churn alert clears on its own as the fault's symptom appears.
	churn.on.Store(false)
	route.shown.Store(true)
	step(5)
	if world.restarts.Load() != 0 {
		t.Fatalf("automatic stage wrote %d times after handoff", world.restarts.Load())
	}
	if len(c.currentIncidentRequest.JoinedKeys) != 1 {
		t.Fatalf("the symptom did not join the alert set: %v", c.currentIncidentRequest.JoinedKeys)
	}
	if closed.Result != nil || dispatcher.calls != 0 {
		t.Fatal("the responder's incident closed or dispatched another agent")
	}
}

type compositeRoute struct{ e2eRoute }

func (d *compositeRoute) Detect(_ context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	var findings []sdk.Finding
	for _, deployment := range view.Deployments() {
		if deployment.Spec.Template.Annotations["kubectl.kubernetes.io/restartedAt"] != "" {
			continue
		}
		ref := sdk.ObjectRefFrom("Deployment", "apps/v1", &deployment)
		findings = append(findings, sdk.Finding{DetectorID: d.Spec().ID, RuleID: requestmetrics.RuleErrors,
			Status: sdk.FindingActive, Severity: sdk.SeverityWarning, Summary: "route fails",
			Evidence: "synthetic", PrimaryResource: ref, ParameterBindings: map[string]sdk.ObjectRef{ComponentRole: ref}})
	}
	return findings, nil
}

type dependentRestart struct {
	componentRestart
	target  string
	blocked *atomic.Bool
	refuse  program.Action
}

func (p dependentRestart) Diagnose(ctx context.Context, view program.ReadView, bindings program.Bindings) (program.Verdict, error) {
	if bindings.Objects[ComponentRole].Name != p.target {
		return program.Verdict{Result: program.NoMatch}, nil
	}
	if p.blocked != nil && p.blocked.Load() {
		return program.Verdict{Result: program.Unknown}, nil
	}
	return p.componentRestart.Diagnose(ctx, view, bindings)
}

func (p dependentRestart) Mitigate(ctx context.Context, view program.ReadView, bindings program.Bindings) (program.Plan, error) {
	if p.refuse != nil {
		return program.Plan{Actions: []program.Action{p.refuse}}, nil
	}
	return p.componentRestart.Mitigate(ctx, view, bindings)
}

func TestCompositeAutomaticRepairAfterDependencyClears(t *testing.T) {
	for _, scenario := range []string{"opening worker alert", "joined worker alert", "worker clue", "at handoff deadline", "failed snapshot"} {
		t.Run(scenario, func(t *testing.T) {
			clearAtDeadline := scenario == "at handoff deadline"
			ctx := t.Context()
			clock := &settlingClock{now: time.Unix(1000, 0).UTC()}
			world := e2eWorld()
			dependency := &e2eChurn{}
			dependency.on.Store(true)
			detectors := []sdk.Detector{dependency, &compositeRoute{}}
			refusing := dependentRestart{componentRestart: componentRestart{id: "a-refusing"}, target: "api", refuse: program.RestartRollout{}}
			if clearAtDeadline {
				refusing.refuse = unencodableAction{}
			}
			fixAPI := dependentRestart{componentRestart: componentRestart{id: "b-api"}, target: "api"}
			fixWorker := dependentRestart{componentRestart: componentRestart{id: "c-worker"}, target: "worker", blocked: &dependency.on}
			registry, err := program.NewRegistry([]program.Diagnoser{refusing, fixAPI, fixWorker}, []program.Mitigator{refusing, fixAPI, fixWorker})
			if err != nil {
				t.Fatal(err)
			}
			cfg := testControllerConfig()
			cfg.Namespace = "app"
			cfg.RequestMetricsOpens = true
			dispatcher := &recordingDispatcher{}
			c, err := NewController(cfg, detectors, world, dispatcher, clock.Now())
			if err != nil {
				t.Fatal(err)
			}
			c.now = clock.Now
			snapshot, err := world.Snapshot(ctx)
			if err != nil {
				t.Fatal(err)
			}
			alerts, _ := detectors[1].Detect(ctx, snapshot)
			for _, finding := range alerts {
				c.tracker.Observe(clock.Now(), finding.DetectorID, []sdk.Finding{finding})
			}
			dependencyFindings, _ := dependency.Detect(ctx, snapshot)
			c.tracker.Observe(clock.Now(), dependency.Spec().ID, dependencyFindings)
			request := IncidentRequest{IncidentID: "synthetic-composite", Application: "demo", Namespace: "app", SourceCommit: "synthetic", RepairPolicy: "recorded-actions",
				Findings: alerts, OpeningKeys: findingKeys(alerts), ResponseDeadline: clock.Now().Add(time.Hour)}
			if scenario == "worker clue" || scenario == "joined worker alert" {
				for _, finding := range alerts {
					if finding.PrimaryResource.Name == "api" {
						request.OpeningKeys = []string{findingKey(finding)}
					} else if scenario == "joined worker alert" {
						request.JoinedKeys = []string{findingKey(finding)}
					}
				}
			}
			executor := PlanExecutor{Client: TypedPlanClient{Client: world.client}, Source: world, Rollouts: world, Verifier: PlanHealthVerifier{Detectors: detectors, Provider: world}}
			loop := NewResponderLoop(request, registry, world, detectors, executor, 5)
			loop.Now = c.now
			loop.ReadView = func(v sdk.DetectionContext) program.ReadView {
				return &sdktest.ReplayReadView{Snapshot: v.(sdktest.Snapshot)}
			}
			loop.saveToolState = func(context.Context, []byte) error { return nil }
			stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, KeepLoop: true}
			state := &IncidentState{Request: request, Findings: alerts}
			report, err := stage.Run(ctx, state)
			if err != nil {
				t.Fatal(err)
			}
			if world.restarts.Load() != 1 || !stagePlanRefused(report) {
				t.Fatalf("opening writes = %d, want one despite refusal: %v, diagnoses %v", world.restarts.Load(), report.GetMitigations(), report.GetDiagnoses())
			}
			if !slices.ContainsFunc(report.GetDiagnoses(), func(v *contracts.PlaybookVerdict) bool {
				return v.GetPlaybookId() == fixWorker.id && v.GetResult() == "unknown"
			}) {
				t.Fatalf("worker was not Unknown at opening: %v", report.GetDiagnoses())
			}
			state.PriorStages = []*contracts.StageReport{report}
			result, err := responsePipelineResult(state, clock.Now(), clock.Now())
			if err != nil {
				t.Fatal(err)
			}
			request = state.Request
			c.codePath = &CodePath{Selection: CodePathSelection{ResponseStages: []string{"deterministic", "agent"}}}
			c.responderLoop = loop
			c.incidentOpen, c.dispatchState = true, contracts.DispatchState_DISPATCH_STATE_RUNNING
			c.currentIncidentRequest = &request
			c.incidentFindingKeys = findingKeys(alerts)
			c.incidentDetectedAt, c.incidentDispatchedAt = clock.Now(), clock.Now()
			c.handleDispatchCompletion(dispatchCompletion{incidentID: request.IncidentID, automatic: state, result: result, priorStages: state.PriorStages}, clock.Now())

			priorCoverage := cloneCoverage(c.currentIncidentRequest.PlaybookCoverage)
			if scenario == "failed snapshot" {
				loop.Provider = &unavailableHandoffView{}
			}
			dependency.on.Store(false)
			elapsed := time.Second
			if clearAtDeadline {
				elapsed = report.GetRecoveryDeadline().AsTime().Sub(clock.Now())
			}
			_ = clock.advance(ctx, elapsed)
			if err := c.Step(ctx, clock.Now(), nil); err != nil {
				t.Fatal(err)
			}
			c.Wait()
			if clearAtDeadline {
				if world.restarts.Load() != 1 || !autoReport(c).GetHandoffSelected() {
					t.Fatalf("dependency clear postponed handoff: writes %d, report %v", world.restarts.Load(), autoReport(c))
				}
				loop.Request = *c.currentIncidentRequest
				response := loop.Handle(ctx, ToolRequest{Command: "diagnose", ProgramID: fixWorker.id,
					Bindings: program.Bindings{Objects: map[string]sdk.ObjectRef{ComponentRole: {APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "worker"}}}})
				if len(response.ProgramRuns) != 1 || response.ProgramRuns[0].Verdict.Result != program.Match {
					t.Fatalf("responder could not diagnose new match: %+v", response)
				}
				detect := loop.Handle(ctx, ToolRequest{Command: "detect"})
				if !strings.Contains(detect.Text, "match now: sdo mitigate "+fixWorker.id) {
					t.Fatalf("detect did not show the new match: %s (%s)", detect.Text, detect.Error)
				}
				return
			}
			if scenario == "failed snapshot" {
				if priorCoverage == nil || !proto.Equal(priorCoverage, c.currentIncidentRequest.PlaybookCoverage) || world.restarts.Load() != 1 {
					t.Fatalf("failed snapshot lost coverage or caused a write: %v", c.currentIncidentRequest.PlaybookCoverage)
				}
				return
			}
			if scenario == "worker clue" {
				if world.restarts.Load() != 1 {
					t.Fatalf("cleared opening alert authorized a write for a clue: %v", autoReport(c).GetMitigations())
				}
				return
			}
			if world.restarts.Load() != 2 || autoReport(c).GetHandoffSelected() {
				t.Fatalf("dependency clear should apply worker repair before handoff: writes %d, report %v", world.restarts.Load(), autoReport(c))
			}
			if scenario == "joined worker alert" {
				return
			}
			for i := range 2 * maxMidIncidentPasses {
				dependency.on.Store(i%2 == 0)
				_ = clock.advance(ctx, time.Second)
				if err := c.Step(ctx, clock.Now(), nil); err != nil {
					t.Fatal(err)
				}
				c.Wait()
			}
			if c.midStage.passes != maxMidIncidentPasses || world.restarts.Load() != 2 || len(autoReport(c).GetMitigations()) != 3 {
				t.Fatalf("flaps: %d passes, %d writes, %v attempts", c.midStage.passes, world.restarts.Load(), autoReport(c).GetMitigations())
			}
		})
	}
}
