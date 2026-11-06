package lifecycle_test

import (
	"context"
	"slices"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/install"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/renderenv"
)

func rawSourcePlan(t *testing.T, paths ...string) *contracts.DeploymentPlan {
	t.Helper()
	config, err := anypb.New(&contracts.RawRenderConfig{Paths: paths})
	if err != nil {
		t.Fatal(err)
	}
	build, err := anypb.New(&contracts.BuildKitImageConfig{ContextPath: "app", DockerfilePath: "app/Dockerfile"})
	if err != nil {
		t.Fatal(err)
	}
	return &contracts.DeploymentPlan{
		Source:              &contracts.DeploymentSource{RendererId: "raw", Config: config},
		Images:              []*contracts.ImageSource{{Image: "registry.local/app:dev", SourceId: "buildkit", Config: build}},
		ExpectedWorkloadIds: []string{"Deployment/ns/app"},
	}
}

func acceptedEvidence(t *testing.T, h *harness) broker.SourceEvidence {
	t.Helper()
	evidence, found, err := h.coordinator.Broker.SourceEvidence(context.Background(), "op-source-1")
	if err != nil || !found {
		t.Fatalf("accepted source evidence: found=%v err=%v", found, err)
	}
	return evidence
}

// Accepted evidence binds the source the deployer rendered and its digests;
// the admitted plan (image declarations and expected workloads) is written by
// Go into the proposal as .sdo/deployment/plan.yaml.
func TestAcceptedEvidenceRecordsTheSourceAndDigestsAndGoWritesThePlan(t *testing.T) {
	h := newHarness(t, &fakeVerifier{verdicts: []lifecycle.HealthVerdict{clear}})
	plan := rawSourcePlan(t, "k8s")
	h.workers.plan = plan
	if _, err := h.coordinator.Run(context.Background(), config()); err != nil {
		t.Fatal(err)
	}
	evidence := acceptedEvidence(t, h)
	if !rendered.SameDeploymentSource(evidence.DeploymentSource, plan.GetSource()) {
		t.Fatalf("evidence source %v, want %v", evidence.DeploymentSource, plan.GetSource())
	}
	if written := h.coordinator.Broker.(*fakeBroker).plan(t, "op-source-1"); !proto.Equal(written, plan) {
		t.Fatalf("plan.yaml of the proposal = %v, want the admitted plan %v", written, plan)
	}
	if evidence.BaseTree != "tree" || evidence.EffectiveTree != "effective" || evidence.PatchesDigest != "patches" || evidence.RenderDigest != "render" || evidence.EnvDigest != "env" {
		t.Fatalf("evidence = %+v", evidence)
	}
}

func TestReceiptThatDoesNotBindTheProposedSourceIsNotAccepted(t *testing.T) {
	for name, adjust := range map[string]func(*lifecycle.DeploymentReceipt){
		"another source":    func(r *lifecycle.DeploymentReceipt) { r.Source = rawSourcePlan(t, "other").GetSource() },
		"no source":         func(r *lifecycle.DeploymentReceipt) { r.Source = nil },
		"no render digest":  func(r *lifecycle.DeploymentReceipt) { r.RenderDigest = "" },
		"no env digest":     func(r *lifecycle.DeploymentReceipt) { r.EnvDigest = "" },
		"no base tree":      func(r *lifecycle.DeploymentReceipt) { r.BaseTree = "" },
		"no effective tree": func(r *lifecycle.DeploymentReceipt) { r.EffectiveTree = "" },
		"no patches digest": func(r *lifecycle.DeploymentReceipt) { r.PatchesDigest = "" },
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, &fakeVerifier{verdicts: []lifecycle.HealthVerdict{clear}})
			h.workers.plan = rawSourcePlan(t, "k8s")
			h.readiness.receipt = adjust
			if _, err := h.coordinator.Run(context.Background(), config()); err == nil {
				t.Fatal("run accepted a receipt that does not bind the proposed source")
			}
			if _, found, _ := h.coordinator.Broker.SourceEvidence(context.Background(), "op-source-1"); found {
				t.Fatal("broker accepted the source")
			}
			if feedback := h.workers.deployer[1].GetCorrectionFeedback(); !strings.Contains(feedback, "independent") {
				t.Fatalf("feedback = %q", feedback)
			}
		})
	}
}

// After a restart, re-verification must reproduce the recorded source and
// digests; a changed digest is not the same deployment.
func TestRestartRequiresTheRecordedSourceAndDigests(t *testing.T) {
	for name, adjust := range map[string]func(*lifecycle.DeploymentReceipt){
		"render digest": func(r *lifecycle.DeploymentReceipt) { r.RenderDigest = "changed" },
		"env digest":    func(r *lifecycle.DeploymentReceipt) { r.EnvDigest = "changed" },
		"source":        func(r *lifecycle.DeploymentReceipt) { r.Source = rawSourcePlan(t, "other").GetSource() },
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, &fakeVerifier{verdicts: []lifecycle.HealthVerdict{firing, clear}})
			h.workers.plan = rawSourcePlan(t, "k8s")
			first := h.coordinator
			// The first run accepts the source and stops at the firing verdict
			// with one attempt; the next run re-verifies the accepted source.
			cfg := config()
			cfg.MaxAttempts = 1
			if _, err := first.Run(context.Background(), cfg); err == nil {
				t.Fatal("first run should exhaust its attempt on the firing verdict")
			}
			h.readiness.reverify = adjust
			if _, err := h.coordinator.Run(context.Background(), cfg); err == nil || !strings.Contains(err.Error(), "changed deployment evidence") {
				t.Fatalf("err = %v", err)
			}
		})
	}
}

type capturingStrategy struct {
	config      install.Config
	verified    *contracts.DeploymentSource
	environment *contracts.RenderEnvironment
}

func (s *capturingStrategy) Install(_ context.Context, config install.Config, _ install.RepositorySeeder) (install.Result, error) {
	s.config = config
	return install.Result{}, nil
}

func (s *capturingStrategy) WithVerifiedDeployment(_, _, _ string, source *contracts.DeploymentSource, environment *contracts.RenderEnvironment) lifecycle.InstallStrategy {
	s.verified, s.environment = source, environment
	return s
}

// The installed controller reads the deployed source from the accepted tree's
// plan.yaml: no source or manifest path flag is forwarded, and onboarding
// stages the receipt's source.
func TestInstallDeploymentStagesTheDeploymentSourceAndForwardsNoFlag(t *testing.T) {
	strategy := &capturingStrategy{}
	adapter := lifecycle.NativeInstallAdapter{Strategy: strategy, AcceptedRepository: t.TempDir(), Config: install.Config{ExtraControllerArgs: []string{"--duration=1s"}}}
	source := rawSourcePlan(t, "k8s").GetSource()
	commit := strings.Repeat("a", 40)
	if err := adapter.InstallDeployment(context.Background(), commit, "ns", lifecycle.DeploymentReceipt{Source: source}); err != nil {
		t.Fatal(err)
	}
	if want := []string{"--duration=1s"}; !slices.Equal(strategy.config.ExtraControllerArgs, want) {
		t.Fatalf("controller args = %v, want %v: the controller reads the source from plan.yaml", strategy.config.ExtraControllerArgs, want)
	}
	if strategy.verified != source {
		t.Fatalf("onboarding staged %v, want the receipt's source", strategy.verified)
	}
	if err := adapter.InstallDeployment(context.Background(), commit, "ns", lifecycle.DeploymentReceipt{}); err == nil {
		t.Fatal("install without a deployment source succeeded")
	}
}

// The environment the deployment was rendered with reaches the installed
// controller (--render-env) and onboarding, so reads render what deploy did.
// Evidence without one (recorded before discovery) adds no flag.
func TestInstallDeploymentForwardsTheRenderEnvironment(t *testing.T) {
	strategy := &capturingStrategy{}
	adapter := lifecycle.NativeInstallAdapter{Strategy: strategy, AcceptedRepository: t.TempDir()}
	source := rawSourcePlan(t, "k8s").GetSource()
	commit := strings.Repeat("a", 40)
	environment := &contracts.RenderEnvironment{KubeVersion: "v1.31.2", ApiVersions: []string{"apps/v1", "v1"}}
	if err := adapter.InstallDeployment(context.Background(), commit, "ns", lifecycle.DeploymentReceipt{Source: source, Environment: environment}); err != nil {
		t.Fatal(err)
	}
	encoded, err := renderenv.Format(environment)
	if err != nil {
		t.Fatal(err)
	}
	if got := strategy.config.ExtraControllerArgs; len(got) != 1 || got[0] != "--render-env="+encoded {
		t.Fatalf("controller args = %v, want --render-env=%s", got, encoded)
	}
	if !renderenv.Same(strategy.environment, environment) {
		t.Fatalf("onboarding renders with %v, want the receipt's environment", strategy.environment)
	}
	strategy = &capturingStrategy{}
	adapter.Strategy = strategy
	if err := adapter.InstallDeployment(context.Background(), commit, "ns", lifecycle.DeploymentReceipt{Source: source}); err != nil {
		t.Fatal(err)
	}
	for _, arg := range strategy.config.ExtraControllerArgs {
		if strings.HasPrefix(arg, "--render-env") {
			t.Fatalf("unknown environment forwarded as %q", arg)
		}
	}
}
