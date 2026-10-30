package runtime

import (
	"context"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/servicehealth"
	"testing"
	"time"
)

// The stage consumes health findings produced from the same live snapshot as
// its repairs. Resource names deliberately differ; only typed selectors link.
func TestStageWaitsForDependentServiceAndTraffic(t *testing.T) {
	for _, unrelated := range []bool{false, true} {
		t.Run(map[bool]string{false: "dependent", true: "uncovered"}[unrelated], func(t *testing.T) {
			loop, state := pipelineTestLoop(t, false)
			client := loop.Executor.Client.(TypedPlanClient).Client
			selector := map[string]string{"app": "api"}
			if unrelated {
				selector = map[string]string{"app": "another-workload"}
			}
			_, err := client.CoreV1().Services("app").Create(context.Background(), &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "public-route", Namespace: "app", UID: "route", ResourceVersion: "1"}, Spec: corev1.ServiceSpec{Selector: selector, Ports: []corev1.ServicePort{{Port: 80, TargetPort: intstr.FromInt32(8080)}}}}, metav1.CreateOptions{})
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now()
			start := now
			health := &stage3DependentHealth{now: func() time.Time { return now }, ready: start.Add(30 * time.Second)}
			loop.Detectors = append(loop.Detectors, health)
			loop.Executor.Rollouts = &settle2DelayedRollout{base: loop.Executor.Rollouts, now: func() time.Time { return now }, ready: health.ready}
			stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, recoveryNow: func() time.Time { return now }, recoveryWait: func(context.Context, time.Duration) error { now = now.Add(15 * time.Second); return nil }}
			dispatcher := &startupSessionDispatcher{}
			if err := RunResponseStages(context.Background(), state, []ResponseStage{stage, AgentResponseStage{Dispatcher: dispatcher}}); err != nil {
				t.Fatal(err)
			}
			wantReason := contracts.RecoveryWait_EXIT_REASON_CLEARED
			if unrelated {
				wantReason = contracts.RecoveryWait_EXIT_REASON_UNCOVERED
			}
			assertStage3Wait(t, loop, state.PriorStages[0], wantReason, now.Sub(start))
			if unrelated {
				if dispatcher.opened.IsZero() || !now.Equal(start) {
					t.Fatalf("uncovered work did not hand off promptly: opened=%v wait=%s", dispatcher.opened, now.Sub(start))
				}
			} else if !dispatcher.opened.IsZero() || len(state.Findings) != 0 || now.Sub(start) != 30*time.Second {
				t.Fatalf("dependent symptoms dispatched responder: opened=%v wait=%s findings=%v report=%v", dispatcher.opened, now.Sub(start), state.Findings, state.PriorStages)
			}
		})
	}
}

type stage3DependentHealth struct {
	now   func() time.Time
	ready time.Time
}

func (*stage3DependentHealth) TrafficWorkloads() []string { return []string{"synthetic-journey"} }

func (*stage3DependentHealth) Spec() sdk.DetectorSpec {
	s := incidentSpec(podWatch)
	s.ID = "synthetic-dependent-health"
	s.Class = sdk.DetectorClassHealth
	return s
}
func (d *stage3DependentHealth) Detect(_ context.Context, v sdk.DetectionContext) ([]sdk.Finding, error) {
	if !d.now().Before(d.ready) {
		return nil, nil
	}
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: v.Namespace(), Name: "public-route"}
	return []sdk.Finding{
		{DetectorID: d.Spec().ID, RuleID: servicehealth.RuleNoReadyEndpoints, Status: sdk.FindingActive, Severity: sdk.SeverityCritical, PrimaryResource: ref, Summary: "Synthetic endpoints settling", Evidence: "synthetic"},
		{DetectorID: d.Spec().ID, RuleID: "traffic", Status: sdk.FindingActive, Severity: sdk.SeverityCritical, PrimaryResource: sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: v.Namespace(), Name: "journey-entry"}, RelatedResources: []sdk.ObjectRef{ref}, Summary: "Synthetic journey settling", Evidence: "synthetic"},
	}, nil
}

// Pass-4 shape: writes/rollout finish first; dependent health clears seven
// seconds later. The health wait must use the same injected recovery clock.
func TestStageHealthClearsSevenSecondsAfterApplyWithoutResponderClaim(t *testing.T) {
	loop, state := pipelineTestLoop(t, false)
	now := time.Now()
	start := now
	health := &stage3DependentHealth{now: func() time.Time { return now }, ready: start.Add(7 * time.Second)}
	client := loop.Executor.Client.(TypedPlanClient).Client
	_, err := client.CoreV1().Services("app").Create(context.Background(), &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "public-route", Namespace: "app", UID: "route", ResourceVersion: "1"}, Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "api"}, Ports: []corev1.ServicePort{{Port: 80, TargetPort: intstr.FromInt32(8080)}}}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	loop.Detectors = append(loop.Detectors, health)
	stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, recoveryNow: func() time.Time { return now }, ObservationInterval: time.Second, recoveryWait: func(_ context.Context, d time.Duration) error { now = now.Add(d); return nil }}
	dispatcher := &startupSessionDispatcher{}
	if err := RunResponseStages(context.Background(), state, []ResponseStage{stage, AgentResponseStage{Dispatcher: dispatcher}}); err != nil {
		t.Fatal(err)
	}
	if !dispatcher.opened.IsZero() || now.Sub(start) != 7*time.Second {
		t.Fatalf("claim after applied repair: claimed=%v recovery=%s reports=%v", dispatcher.opened, now.Sub(start), state.PriorStages)
	}
	assertStage3Wait(t, loop, state.PriorStages[0], contracts.RecoveryWait_EXIT_REASON_CLEARED, 7*time.Second)
}

// A Service configuration fault is not a rollout symptom merely because it
// selects the Deployment being repaired. Role and relation are both required.
func TestStageDoesNotWaitForUncoveredServiceConfiguration(t *testing.T) {
	loop, state := pipelineTestLoop(t, false)
	p := pipelineImageProgram{}
	registry, err := program.NewRegistry([]program.Diagnoser{p}, []program.Mitigator{p})
	if err != nil {
		t.Fatal(err)
	}
	loop.Programs = registry
	state.Request.InitialProgramRuns = loop.InitialDiagnose(context.Background())
	state.Coverage = buildPlaybookCoverage(state.Findings, state.Request.InitialProgramRuns, loop.lastDiagnosisView, nil)
	state.Request.PlaybookCoverage = state.Coverage
	loop.Request = state.Request
	now := time.Now()
	waits := 0
	stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, recoveryNow: func() time.Time { return now }, recoveryWait: func(_ context.Context, d time.Duration) error { waits++; now = now.Add(d); return nil }}
	report, err := stage.Run(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	if waits != 0 || report.GetRecoveryWait().GetExitReason() != contracts.RecoveryWait_EXIT_REASON_UNCOVERED {
		t.Fatalf("configuration borrowed rollout dependency: waits=%d report=%v", waits, report)
	}
}
