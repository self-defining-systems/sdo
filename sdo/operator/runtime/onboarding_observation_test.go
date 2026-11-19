package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/onboarding"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/traffic"
)

func TestOnboardingIncompleteHealthIsNotContradiction(t *testing.T) {
	d := controllerDetector("health-objective", time.Second, sdk.Finding{}, sdk.Finding{RuleID: "unhealthy", Status: sdk.FindingActive})
	d.spec.Class = sdk.DetectorClassHealth
	d.spec.Owner = sdk.DetectorOwnerHealthJudge
	d.errors[0] = errors.New("observation unavailable")
	c := &Controller{onboardingReachability: &onboardingReachabilityState{}, detectors: map[string]sdk.Detector{d.spec.ID: d}}
	if c.checkOnboardingHealth(context.Background(), sdktest.Snapshot{}) {
		t.Fatal("unavailable health authorized onboarding")
	}
	if c.onboardingReachability.invalid {
		t.Fatal("incomplete observation permanently rejected onboarding")
	}
	if !strings.Contains(c.onboardingReachability.waitReason, "observation unavailable") {
		t.Fatal("missing pending evidence")
	}
	if c.checkOnboardingHealth(context.Background(), sdktest.Snapshot{}) {
		t.Fatal("unhealthy observation authorized onboarding")
	}
	if c.onboardingReachability.invalid {
		t.Fatal("the first warm evaluation of a finding rejected the install")
	}
	c.evaluationIteration++
	if c.checkOnboardingHealth(context.Background(), sdktest.Snapshot{}) {
		t.Fatal("unhealthy observation authorized onboarding")
	}
	if !c.onboardingReachability.invalid || !strings.Contains(c.onboardingReachability.invalidReason, "health-objective") {
		t.Fatal("persistent contradiction was not retained")
	}
}

// An informational finding (request metrics that have no series yet, too few
// samples) is an incomplete observation: the install waits, however many
// evaluations repeat it, and the window's expiry bounds the wait.
func TestOnboardingWaitsOnUnobservedFindings(t *testing.T) {
	d := &warmingDetector{sequenceDetector: controllerDetector("request-metrics", time.Second), warm: true}
	d.spec.Class = sdk.DetectorClassHealth
	d.spec.Owner = sdk.DetectorOwnerHealthJudge
	d.always = sdk.Finding{DetectorID: "request-metrics", RuleID: "request-metrics.not-instrumented", Status: sdk.FindingActive, Severity: sdk.SeverityInfo, Summary: "http_requests_total has no series"}
	c := &Controller{onboardingReachability: &onboardingReachabilityState{}, detectors: map[string]sdk.Detector{d.spec.ID: d}}
	for i := 0; i < 3; i++ {
		c.evaluationIteration++
		if c.checkOnboardingHealth(context.Background(), sdktest.Snapshot{}) {
			t.Fatal("an unobserved finding authorized onboarding")
		}
		if c.onboardingReachability.invalid {
			t.Fatalf("an unobserved finding contradicted the install: %s", c.onboardingReachability.invalidReason)
		}
	}
	if !strings.Contains(c.onboardingReachability.waitReason, "has no series") {
		t.Fatalf("wait reason does not carry the observation gap: %q", c.onboardingReachability.waitReason)
	}
}

// A warning is a contradiction only once traffic warm-up has settled: a
// detector still warming (its confirmation window incomplete) waits, however
// long the finding repeats.
func TestOnboardingWarningWaitsForWarmUp(t *testing.T) {
	d := &warmingDetector{sequenceDetector: controllerDetector("synthetic", time.Second)}
	d.spec.Class = sdk.DetectorClassHealth
	d.spec.Owner = sdk.DetectorOwnerHealthJudge
	d.always = sdk.Finding{DetectorID: "synthetic", RuleID: "slo", Status: sdk.FindingActive, Severity: sdk.SeverityWarning}
	c := &Controller{onboardingReachability: &onboardingReachabilityState{}, detectors: map[string]sdk.Detector{d.spec.ID: d}}
	for i := 0; i < 3; i++ {
		c.evaluationIteration++
		if c.checkOnboardingHealth(context.Background(), sdktest.Snapshot{}) || c.onboardingReachability.invalid {
			t.Fatal("a warming detector's finding rejected or authorized the install")
		}
	}
	d.warm = true
	for i := 0; i < 2; i++ {
		c.evaluationIteration++
		c.checkOnboardingHealth(context.Background(), sdktest.Snapshot{})
	}
	if !c.onboardingReachability.invalid {
		t.Fatal("a warning that persisted after warm-up did not contradict the install")
	}
}

type warmingDetector struct {
	*sequenceDetector
	always sdk.Finding
	warm   bool
}

func (d *warmingDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	return []sdk.Finding{d.always}, nil
}
func (d *warmingDetector) Confirmed(sdk.DetectionContext) bool { return d.warm }

type slowWindowDetector struct{ *sequenceDetector }

func (slowWindowDetector) EvaluationWindow(sdk.DetectionContext) time.Duration {
	return 90 * time.Second
}

func TestOnboardingExpiryAllowsConsumerWindowButRemainsBounded(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	d := slowWindowDetector{controllerDetector("health-objective", time.Second, sdk.Finding{})}
	d.spec.Class = sdk.DetectorClassHealth
	d.spec.Owner = sdk.DetectorOwnerHealthJudge
	c := &Controller{pendingOnboarding: &contracts.NamespaceOnboarding{}, onboardingReachability: &onboardingReachabilityState{startedAt: start}, detectors: map[string]sdk.Detector{d.spec.ID: d}}
	if c.onboardingConfirmationExpired(start.Add(40*time.Second), sdktest.Snapshot{}) {
		t.Fatal("expired before consumer could observe its window")
	}
	if !c.onboardingConfirmationExpired(start.Add(6*time.Minute), sdktest.Snapshot{}) {
		t.Fatal("incomplete onboarding can wait forever")
	}
}

func TestOnboardingClaimAlwaysReachesTerminalRecord(t *testing.T) {
	for _, scenario := range []string{"unhealthy", "interrupted", "rejection-write-fails"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			now := time.Unix(1000, 0)
			client := fake.NewSimpleClientset()
			d := controllerDetector("health", time.Second, sdk.Finding{})
			d.spec.Class = sdk.DetectorClassHealth
			d.spec.Owner = sdk.DetectorOwnerHealthJudge
			cfg := testControllerConfig()
			cfg.SourceCommit = "accepted"
			c := &Controller{config: cfg, detectors: map[string]sdk.Detector{d.spec.ID: d}, healthDetectorIDs: []string{d.spec.ID}}
			c.onboardingClient, c.onboardingControl = client, "control"
			record := &contracts.NamespaceOnboarding{AcceptedCommit: "accepted"}
			if _, err := onboarding.Claim(ctx, client, "control"); err != nil {
				t.Fatal(err)
			}
			if scenario == "rejection-write-fails" {
				client.PrependReactor("create", "configmaps", func(ktesting.Action) (bool, kruntime.Object, error) { return true, nil, errors.New("API unavailable") })
				if err := c.finishOnboarding(ctx, record, []sdk.Finding{{Status: sdk.FindingActive}}, false, now); err == nil {
					t.Fatal("rejection write failure lost")
				}
				if c.pendingOnboarding == nil {
					t.Fatal("rejection write failure discarded pending evaluation")
				}
				client.Fake.ReactionChain = client.Fake.ReactionChain[1:]
				c.beginOnboarding(ctx, []sdk.Detector{d}, now)
			} else if scenario == "unhealthy" {
				c.finishOnboarding(ctx, record, []sdk.Finding{{Status: sdk.FindingActive}}, false, now)
			} else {
				c.pendingOnboarding = record
				c.beginOnboarding(ctx, []sdk.Detector{d}, now)
			}
			rejected, err := onboarding.Marked(ctx, client, "control", onboarding.RejectedConfigMapName)
			if err != nil || !rejected {
				t.Fatalf("consumed attempt left indefinitely pending: rejected=%v err=%v", rejected, err)
			}
			if c.namespaceOnboarding != nil {
				t.Fatal("failed attempt granted authority")
			}
		})
	}
}

// A firing health detector while a workload rolls out is the rollout, not a
// contradiction of the install window: the install waits for the rollout.
func TestOnboardingWaitsForARolloutInsteadOfRejecting(t *testing.T) {
	d := controllerDetector("workload-health", time.Second, sdk.Finding{RuleID: "workload-deployment-unavailable", Status: sdk.FindingActive})
	d.spec.Class = sdk.DetectorClassHealth
	d.spec.Owner = sdk.DetectorOwnerHealthJudge
	c := &Controller{onboardingReachability: &onboardingReachabilityState{}, detectors: map[string]sdk.Detector{d.spec.ID: d}}
	one := int32(1)
	rolling := appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "web", Generation: 2},
		Spec:   appsv1.DeploymentSpec{Replicas: &one},
		Status: appsv1.DeploymentStatus{ObservedGeneration: 2, Replicas: 2, UpdatedReplicas: 1}}
	if c.checkOnboardingHealth(context.Background(), sdktest.Snapshot{DeploymentList: []appsv1.Deployment{rolling}}) {
		t.Fatal("a rolling workload authorized onboarding")
	}
	if c.onboardingReachability.invalid {
		t.Fatalf("a rollout rejected the install window: %s", c.onboardingReachability.invalidReason)
	}
	if !strings.Contains(c.onboardingReachability.waitReason, "Deployment/web") {
		t.Fatalf("wait reason does not name the rollout: %q", c.onboardingReachability.waitReason)
	}
	settled := rolling
	settled.Status.Replicas = 1
	if c.checkOnboardingHealth(context.Background(), sdktest.Snapshot{DeploymentList: []appsv1.Deployment{settled}}) {
		t.Fatal("unhealthy settled workload authorized onboarding")
	}
	c.evaluationIteration++
	c.checkOnboardingHealth(context.Background(), sdktest.Snapshot{DeploymentList: []appsv1.Deployment{settled}})
	if !c.onboardingReachability.invalid {
		t.Fatal("a firing detector outside any rollout must still contradict the install")
	}
}

// No health detector ID is reserved for the human goal: the shared check
// installs the generic health detectors and the judge may author others. A
// baseline is authorized by any independently evaluated health detector, and
// by none when no health detector exists (only learned detectors, or none).
func TestHealthyBaselineGoalNeedsAHealthDetectorNotAReservedID(t *testing.T) {
	health := controllerDetector("workload-health", time.Second, sdk.Finding{})
	health.spec.Class = sdk.DetectorClassHealth
	health.spec.Owner = sdk.DetectorOwnerHealthJudge
	if ok, err := healthyBaselineGoal(context.Background(), []sdk.Detector{health}, sdktest.Snapshot{}); err != nil || !ok {
		t.Fatalf("generic health detector did not authorize the baseline: %v", err)
	}
	incident := controllerDetector("crash", time.Second, sdk.Finding{})
	if ok, err := healthyBaselineGoal(context.Background(), []sdk.Detector{incident}, sdktest.Snapshot{}); err == nil || ok {
		t.Fatal("a learned detector authorized the baseline")
	}
	if ok, err := healthyBaselineGoal(context.Background(), nil, sdktest.Snapshot{}); err == nil || ok {
		t.Fatal("no detectors authorized the baseline")
	}
}

// The create-once install evaluation is claimed only when the install's own
// health check holds: a claimed evaluation that meets an unobserved finding is
// rejected for good, so the finding is met before the claim and waits instead.
func TestOnboardingIsNotClaimedOnUnobservedHealth(t *testing.T) {
	d := &warmingDetector{sequenceDetector: controllerDetector("request-metrics", time.Second), warm: true}
	d.spec.Class = sdk.DetectorClassHealth
	d.spec.Owner = sdk.DetectorOwnerHealthJudge
	d.always = sdk.Finding{DetectorID: "request-metrics", RuleID: "request-metrics.not-instrumented", Status: sdk.FindingActive, Severity: sdk.SeverityInfo, Summary: "no series"}
	c := &Controller{pendingOnboarding: &contracts.NamespaceOnboarding{}, onboardingReachability: &onboardingReachabilityState{}, detectors: map[string]sdk.Detector{d.spec.ID: d}}
	if c.onboardingHealthSettled(context.Background(), sdktest.Snapshot{}) {
		t.Fatal("the install evaluation would be claimed over an unobserved finding")
	}
	c.detectors = map[string]sdk.Detector{d.spec.ID: &emptyDetector{d.sequenceDetector}}
	if !c.onboardingHealthSettled(context.Background(), sdktest.Snapshot{}) {
		t.Fatal("a quiet install was held back")
	}
}

type emptyDetector struct{ *sequenceDetector }

func (emptyDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	return nil, nil
}

// remoteConsumer is a traffic consumer as the memory host registers it: it
// has no Confirmed method of its own; its probe workload's samples confirm it.
type remoteConsumer struct{ *sequenceDetector }

func (remoteConsumer) TrafficWorkloads() []string { return []string{"probe"} }

// warmingTraffic serves a probe window that holds no samples until warm, then
// three fresh successes: a probe sampled slower than start-up takes.
type warmingTraffic struct{ warm *bool }

func (p warmingTraffic) Snapshot(context.Context) (sdk.DetectionContext, error) {
	workload := traffic.Workload{APIVersion: traffic.APIVersion, Kind: traffic.WorkloadKind, Name: "probe", Purpose: traffic.PurposeHealthProbe, Scenarios: []traffic.WorkloadScenario{{ID: "call"}}}.WithDefaults()
	at := time.Unix(2000, 0).UTC()
	observed := traffic.ScenarioObservations{Scenario: traffic.Descriptor{ID: "call"}}
	if *p.warm {
		observed.Qualified = true
		for i := 0; i < 3; i++ {
			observed.Samples = append(observed.Samples, traffic.Sample{At: at.Add(time.Duration(i-3) * 10 * time.Second), Outcome: traffic.OutcomeOK, Latency: time.Millisecond})
		}
	}
	return sdktest.Snapshot{Traffic: map[string]traffic.Window{"probe": {Workload: workload, ObservedAt: at, Scenarios: []traffic.ScenarioObservations{observed}}}}, nil
}

// A probe that warms up after start-up's full batch must still let the install
// be claimed once it is warm, whatever batch the step that finds it warm
// selects. In production every detector runs behind the memory host, so none
// carries a Confirmed method of its own; the install sweep must not depend on one.
func TestInstallIsClaimedWhenProbesWarmAfterTheFirstBatch(t *testing.T) {
	start := time.Unix(1000, 0).UTC()
	cfg := testControllerConfig()
	cfg.SourceCommit = strings.Repeat("a", 40)
	probe := remoteConsumer{quietHealth()}
	probe.spec.ID, probe.spec.Interval = "probe", 30*time.Second
	health := quietHealth()
	health.spec.Interval = 5 * time.Second
	warm := false
	c, err := NewController(cfg, []sdk.Detector{probe, health}, warmingTraffic{&warm}, &recordingDispatcher{requests: make(chan IncidentRequest, 4)}, start)
	if err != nil {
		t.Fatal(err)
	}
	c.onboardingClient, c.onboardingControl = fake.NewSimpleClientset(), "control"
	c.onboardingReachability = &onboardingReachabilityState{}
	c.pendingOnboarding = &contracts.NamespaceOnboarding{Namespace: cfg.Namespace, AcceptedCommit: cfg.SourceCommit, EstablishedAt: timestamppb.New(start)}
	if err := c.Step(context.Background(), start, nil); err != nil {
		t.Fatal(err)
	}
	if c.namespaceOnboarding != nil {
		t.Fatal("a cold probe let start-up's batch claim the install")
	}
	warm = true
	if err := c.Step(context.Background(), start.Add(5*time.Second), nil); err != nil {
		t.Fatal(err)
	}
	if c.namespaceOnboarding == nil {
		t.Fatalf("the install was not claimed once the probe was warm; stale pending reason %q", c.onboardingReachability.waitReason)
	}
}
