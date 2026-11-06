package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/onboarding"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
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
	if !c.onboardingReachability.invalid || !strings.Contains(c.onboardingReachability.invalidReason, "health-objective") {
		t.Fatal("confirmed contradiction was not retained")
	}
}

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
	if !c.onboardingReachability.invalid {
		t.Fatal("a firing detector outside any rollout must still contradict the install")
	}
}
