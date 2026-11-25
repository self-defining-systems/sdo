package runtime

import (
	"context"
	"slices"
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
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// componentRestart is a learned playbook with the one role a credited key binds
// through. It restarts its component, and matches until that restart landed.
type componentRestart struct{ id string }

func (p componentRestart) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: p.id, Params: []program.ParamSpec{{Name: ComponentRole, Kind: "Deployment", Required: true}}}
}

func (p componentRestart) Diagnose(_ context.Context, view program.ReadView, b program.Bindings) (program.Verdict, error) {
	for _, deployment := range view.Deployments() {
		if deployment.Name == b.Objects[ComponentRole].Name && deployment.Spec.Template.Annotations["kubectl.kubernetes.io/restartedAt"] == "" {
			return program.Verdict{Result: program.Match}, nil
		}
	}
	return program.Verdict{Result: program.NoMatch}, nil
}

func (p componentRestart) Mitigate(_ context.Context, _ program.ReadView, b program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: []program.Action{program.RestartRollout{Ref: b.Objects[ComponentRole], Reason: "restart the component"}}}, nil
}

// suspectsDetector reports one failing user test whose finding names every
// Deployment as a suspect and lists no playbook.
type suspectsDetector struct{}

func (suspectsDetector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: "traffic", Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge,
		Watches:  []sdk.WatchKind{{APIVersion: "apps/v1", Kind: "Deployment"}},
		Interval: time.Second, Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1},
		Batching: sdk.BatchingPolicy{Severity: sdk.SeverityCritical}, OriginatingCommit: "seed"}
}

func (suspectsDetector) Detect(_ context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	finding := sdk.Finding{DetectorID: "traffic", RuleID: "scenario-slo.browse", Status: sdk.FindingActive, Severity: sdk.SeverityCritical, Summary: "browse fails", Evidence: "synthetic", Fingerprint: "browse",
		PrimaryResource: sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "gateway"}}
	for _, deployment := range view.Deployments() {
		finding.RelatedResources = append(finding.RelatedResources, sdk.ObjectRefFrom("Deployment", "apps/v1", &deployment))
	}
	return []sdk.Finding{finding}, nil
}

const keyedSymptom = "traffic/browse"

func keyedStageLoop(t *testing.T, credits ProgramCredits) (*sameWriteWorld, *DeterministicResponseStage, *IncidentState) {
	t.Helper()
	replicas := int32(1)
	var objects []kruntime.Object
	for _, name := range []string{"api", "web"} {
		labels := map[string]string{"app": name}
		objects = append(objects,
			&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app", UID: types.UID("synthetic-deployment-" + name), Generation: 1}, Spec: appsv1.DeploymentSpec{Replicas: &replicas, Selector: &metav1.LabelSelector{MatchLabels: labels}, Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: name, Image: name + ":v1"}}}}}, Status: appsv1.DeploymentStatus{ObservedGeneration: 1, Replicas: 1, UpdatedReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1}},
			&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name + "-pod", Namespace: "app", Labels: labels}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: name, Image: name + ":v1"}}}, Status: corev1.PodStatus{Phase: corev1.PodRunning}})
	}
	world := &sameWriteWorld{client: fake.NewSimpleClientset(objects...)}
	world.client.PrependReactor("*", "deployments", func(action ktesting.Action) (bool, kruntime.Object, error) {
		if action.GetSubresource() == "" && (action.GetVerb() == "update" || action.GetVerb() == "patch") {
			world.restarts.Add(1)
		}
		return false, nil, nil
	})
	detector := suspectsDetector{}
	restart := componentRestart{id: "fix"}
	registry, err := program.NewRegistry([]program.Diagnoser{restart}, []program.Mitigator{restart})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := world.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	findings, err := detector.Detect(context.Background(), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	request := IncidentRequest{IncidentID: "synthetic-keyed", Namespace: "app", SourceCommit: "synthetic-accepted", Findings: findings, OpeningKeys: []string{keyedSymptom}, RepairPolicy: "recorded-actions"}
	executor := PlanExecutor{Client: TypedPlanClient{Client: world.client}, Source: world, Rollouts: world, Verifier: PlanHealthVerifier{Detectors: []sdk.Detector{detector}, Provider: world}}
	loop := NewResponderLoop(request, registry, world, []sdk.Detector{detector}, executor, 5)
	loop.Credits = credits
	loop.ReadView = func(v sdk.DetectionContext) program.ReadView {
		return &sdktest.ReplayReadView{Snapshot: v.(sdktest.Snapshot)}
	}
	loop.saveToolState = func(context.Context, []byte) error { return nil }
	instant := newInstantRecoveryClock()
	stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, KeepLoop: true, diagnoseWait: instant.wait}
	return world, stage, &IncidentState{Request: request, Findings: findings}
}

// A learned playbook that lists no detector and whose finding names two
// Deployments is applied by the stage to the one it was credited for on this
// symptom, and only that one.
func TestStageAppliesACreditedPlaybookToTheCreditedSuspectOnly(t *testing.T) {
	world, stage, state := keyedStageLoop(t, credits("fix", keyedSymptom, "api"))
	report, err := stage.Run(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	var applied []string
	for _, attempt := range report.Mitigations {
		if attempt.GetApplyReceipt().GetApplied() {
			applied = append(applied, attempt.PlaybookId+"/"+programBindingsState(attempt.Bindings).Objects[ComponentRole].Name)
		}
	}
	if len(applied) != 1 || applied[0] != "fix/api" || world.restarts.Load() != 1 {
		t.Fatalf("applied %v with %d writes, want only fix on api: %v", applied, world.restarts.Load(), report)
	}
}

// Without a credited pair the finding names two same-Kind suspects and
// nothing binds the playbook automatically.
func TestStageDoesNotBindAnAmbiguousSuspectWithoutACredit(t *testing.T) {
	world, stage, state := keyedStageLoop(t, nil)
	report, err := stage.Run(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Mitigations) != 0 || world.restarts.Load() != 0 {
		t.Fatalf("applied without a credited pair: %d writes, %v", world.restarts.Load(), report)
	}
}

// A pair credited on another symptom is not a candidate on this incident.
func TestStageIgnoresACreditForAnotherSymptom(t *testing.T) {
	world, stage, state := keyedStageLoop(t, credits("fix", "another-alert", "api"))
	report, err := stage.Run(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Mitigations) != 0 || world.restarts.Load() != 0 {
		t.Fatalf("applied on a symptom the pair was not credited for: %d writes, %v", world.restarts.Load(), report)
	}
}

// An app-wide diagnosis can still match the first suspect after its plan
// wrote. The attempted Match must still claim the credited program's slot.
type appWideRestart struct{ componentRestart }

func (appWideRestart) Diagnose(context.Context, program.ReadView, program.Bindings) (program.Verdict, error) {
	return program.Verdict{Result: program.Match}, nil
}

func TestAttemptedCreditedMatchKeepsItsSuspectSlot(t *testing.T) {
	world, stage, state := keyedStageLoop(t, nil)
	state.Request.OpeningKeys = findingKeys(state.Request.Findings)
	loop := stage.NewLoop(state.Request)
	loop.Request = state.Request
	loop.Credits = credits("fix", state.Request.OpeningKeys[0], "api", "web")
	appWide := appWideRestart{componentRestart{id: "fix"}}
	registry, err := program.NewRegistry([]program.Diagnoser{appWide}, []program.Mitigator{appWide})
	if err != nil {
		t.Fatal(err)
	}
	loop.Programs = registry
	report, err := stage.Run(t.Context(), state)
	if err != nil || world.restarts.Load() != 1 {
		t.Fatalf("opening writes=%d, report=%v, err=%v", world.restarts.Load(), report, err)
	}
	for range 2 {
		state = &IncidentState{Request: state.Request, Findings: slices.Clone(state.Request.Findings), Coverage: state.Coverage, PriorStages: []*contracts.StageReport{report}}
		report, err = RunStageAgain(t.Context(), state, stage)
		if err != nil || world.restarts.Load() != 1 {
			t.Fatalf("later pass must leave the second suspect alone: writes=%d, report=%v, err=%v", world.restarts.Load(), report, err)
		}
	}
}

// A repair whose own diagnosis stopped matching still owns its playbook while
// its write has not settled; only then may the second suspect be repaired.
func TestUnsettledCreditedRepairKeepsItsSlotAfterItStopsMatching(t *testing.T) {
	world, stage, state := keyedStageLoop(t, nil)
	world.stuck.Store(true)
	state.Request.OpeningKeys = findingKeys(state.Request.Findings)
	clock := &settlingClock{now: time.Unix(1000, 0).UTC()}
	loop := stage.NewLoop(state.Request)
	loop.Request, loop.Now = state.Request, clock.Now
	loop.Credits = credits("fix", state.Request.OpeningKeys[0], "api", "web")
	report, err := stage.Run(t.Context(), state)
	if err != nil || world.restarts.Load() != 1 {
		t.Fatalf("opening writes=%d, report=%v, err=%v", world.restarts.Load(), report, err)
	}
	again := func(want int32) {
		t.Helper()
		state = &IncidentState{Request: state.Request, Findings: slices.Clone(state.Request.Findings), Coverage: state.Coverage, PriorStages: []*contracts.StageReport{report}}
		report, err = RunStageAgain(t.Context(), state, stage)
		if err != nil || world.restarts.Load() != want {
			t.Fatalf("writes=%d, want %d: report=%v, err=%v", world.restarts.Load(), want, report, err)
		}
	}
	again(1)
	_ = clock.advance(t.Context(), DefaultMitigationRolloutTimeout+time.Second)
	again(2)
}
