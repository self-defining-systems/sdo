package runtime

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

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

// Routine churn opens the incident and the agent starts with nothing to apply.
// The fault's own request-metrics symptom joins the alert set afterwards: the
// learned playbook is evaluated against it, matches, applies, the alerts clear
// and the incident closes success, with the agent dispatched once and never
// re-prompted.
func TestLearnedPlaybookFixesASymptomThatJoinsAChurnIncident(t *testing.T) {
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
	loop := NewResponderLoop(request, registry, world, detectors, executor, 5)
	loop.ReadView = func(v sdk.DetectionContext) program.ReadView {
		return &sdktest.ReplayReadView{Snapshot: v.(sdktest.Snapshot)}
	}
	loop.saveToolState = func(context.Context, []byte) error { return nil }
	instant := newInstantRecoveryClock()
	c.tuneMidStage = func(stage *DeterministicResponseStage) {
		stage.recoveryNow, stage.recoveryWait = instant.now, instant.wait
		stage.Budget, stage.RolloutTimeout, stage.ObservationInterval = time.Minute, time.Minute, time.Millisecond
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
	applied := false
	for _, report := range c.currentIncidentRequest.PriorStages {
		for _, m := range report.Mitigations {
			applied = applied || m.PlaybookId == "fix" && m.Result == "applied_cleared"
		}
	}
	if !applied {
		t.Fatalf("the learned playbook was not applied and cleared: %v", c.currentIncidentRequest.PriorStages)
	}
	if world.restarts.Load() != 1 {
		t.Fatalf("the learned playbook wrote %d times, want one restart of the component", world.restarts.Load())
	}
	step(15)
	if closed.Result == nil || closed.Result.Status != IncidentCompleted {
		t.Fatalf("the incident did not close success: %+v (joined %v)", closed.Result, c.currentIncidentRequest)
	}
	if dispatcher.calls != 0 {
		t.Fatalf("the agent was dispatched %d more times", dispatcher.calls)
	}
}
