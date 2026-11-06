package lifecycle_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/lifecycle"
)

func providedPlans(t *testing.T) map[string]*contracts.DeploymentPlan {
	t.Helper()
	raw, err := anypb.New(&contracts.RawRenderConfig{Paths: []string{"k8s"}})
	if err != nil {
		t.Fatal(err)
	}
	helm, err := anypb.New(&contracts.HelmRenderConfig{ChartPath: "chart"})
	if err != nil {
		t.Fatal(err)
	}
	images := []*contracts.ImageSource{{Image: "registry.local/app:v1"}}
	return map[string]*contracts.DeploymentPlan{
		"raw":  {Source: &contracts.DeploymentSource{RendererId: "raw", Config: raw}, Images: images, ExpectedWorkloadIds: []string{"Deployment/ns/app"}},
		"helm": {Source: &contracts.DeploymentSource{RendererId: "helm", Config: helm}, Images: images, ExpectedWorkloadIds: []string{"Deployment/ns/app"}},
	}
}

func providedConfig(plan *contracts.DeploymentPlan) lifecycle.Config {
	cfg := config()
	cfg.ProvidedPlan = plan
	return cfg
}

func TestProvidedPlanIsDeployedAndRecordedWithoutTheDeployerAgent(t *testing.T) {
	for name, plan := range providedPlans(t) {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, &fakeVerifier{verdicts: []lifecycle.HealthVerdict{clear}})
			fake := h.coordinator.Broker.(*fakeBroker)
			result, err := h.coordinator.Run(context.Background(), providedConfig(plan))
			if err != nil {
				t.Fatal(err)
			}
			if h.workers.deployerAttempts() != 0 {
				t.Fatalf("the deployer agent ran %d times for a provided plan", h.workers.deployerAttempts())
			}
			if len(h.readiness.plans) != 1 || !proto.Equal(h.readiness.plans[0], plan) {
				t.Fatalf("trusted deployer saw %v, want the provided plan unchanged", h.readiness.plans)
			}
			evidence, ok := fake.evidence["op-source-1"]
			if !ok || !proto.Equal(evidence.Plan, plan) || evidence.DeploymentSource.GetRendererId() != name {
				t.Fatalf("broker evidence = %+v, want the provided plan recorded", evidence)
			}
			if result.SourceCommit == "" || result.JudgeCommit == "" || h.installer.installs != 1 || h.workers.judgeRuns != 1 {
				t.Fatalf("result %+v installs=%d judge runs=%d: everything after the deploy must run as usual", result, h.installer.installs, h.workers.judgeRuns)
			}
		})
	}
}

func TestProvidedPlanThatFailsAdmissionIsTerminal(t *testing.T) {
	legacy, _ := anypb.New(&contracts.RawRenderConfig{Paths: []string{"k8s"}})
	for name, plan := range map[string]*contracts.DeploymentPlan{
		"no source":     {ExpectedWorkloadIds: []string{"Deployment/ns/app"}},
		"strategy only": {StrategyId: "raw-manifests-v1", StrategyConfig: legacy},
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, &fakeVerifier{verdicts: []lifecycle.HealthVerdict{clear}})
			_, err := h.coordinator.Run(context.Background(), providedConfig(plan))
			var terminal *lifecycle.ProvidedPlanError
			if !errors.As(err, &terminal) || terminal.Stage != "deploy" || !strings.Contains(err.Error(), "source") {
				t.Fatalf("error = %v, want a deploy-stage ProvidedPlanError naming the missing source", err)
			}
			if h.readiness.calls != 0 || h.workers.deployerAttempts() != 0 || h.installer.installs != 0 {
				t.Fatalf("deployer calls=%d agent attempts=%d installs=%d", h.readiness.calls, h.workers.deployerAttempts(), h.installer.installs)
			}
		})
	}
}

func TestProvidedPlanThatFailsToDeployIsTerminalWithoutAgentRetries(t *testing.T) {
	h := newHarness(t, &fakeVerifier{verdicts: []lifecycle.HealthVerdict{clear}})
	h.readiness.errors = []error{errors.New("chart values: unknown key")}
	_, err := h.coordinator.Run(context.Background(), providedConfig(providedPlans(t)["helm"]))
	var terminal *lifecycle.ProvidedPlanError
	if !errors.As(err, &terminal) || terminal.Stage != "deploy" || !strings.Contains(err.Error(), "unknown key") {
		t.Fatalf("error = %v, want a deploy-stage ProvidedPlanError with the deployer's reason", err)
	}
	if h.readiness.calls != 1 || h.workers.deployerAttempts() != 0 {
		t.Fatalf("deploy calls=%d agent attempts=%d, want one deploy and no agent", h.readiness.calls, h.workers.deployerAttempts())
	}
}

func TestProvidedPlanThatVerifiesUnhealthyIsTerminal(t *testing.T) {
	h := newHarness(t, &fakeVerifier{verdicts: []lifecycle.HealthVerdict{firing}})
	_, err := h.coordinator.Run(context.Background(), providedConfig(providedPlans(t)["raw"]))
	var terminal *lifecycle.ProvidedPlanError
	if !errors.As(err, &terminal) || terminal.Stage != "verify" || !strings.Contains(err.Error(), "frontend-available") {
		t.Fatalf("error = %v, want a verify-stage ProvidedPlanError naming the detector", err)
	}
	if h.readiness.calls != 1 || h.workers.deployerAttempts() != 0 || h.installer.installs != 0 {
		t.Fatalf("deploy calls=%d agent attempts=%d installs=%d", h.readiness.calls, h.workers.deployerAttempts(), h.installer.installs)
	}
}
