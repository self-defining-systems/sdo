package lifecycle_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/operability"
	"sdo.dev/operator/worker_registry"
	"sdo.dev/operator/worker_registry/registrytest"
)

// treeReadiness gates on the proposal checkout's files, as the real deployer
// does offline: a checkout without deployment files fails O1.render with the
// render's own "no deployment files" error, before any cluster or build.
type treeReadiness struct {
	*fakeReadiness
	offline int
}

const deploymentFile = "kustomization.yaml"

func (r *treeReadiness) CheckOffline(_ context.Context, checkout string, _ *contracts.DeploymentPlan) (lifecycle.OfflineChecks, error) {
	r.offline++
	if _, err := os.Stat(filepath.Join(checkout, deploymentFile)); err != nil {
		return lifecycle.OfflineChecks{BaseTree: "tree", Checks: failing(ck("O1.render", operability.StatusFail, operability.CauseRender, "the tree has no deployment files: no Helm chart (Chart.yaml), no kustomization.yaml and no Kubernetes manifest"))}, nil
	}
	return lifecycle.OfflineChecks{Checks: passing(), BaseTree: "tree", EffectiveTree: "effective", RenderDigest: "render", EnvDigest: "env"}, nil
}

// committingWorkers is the deployer agent of case A: it commits a
// kustomization into its proposal checkout, like an engineer.
type committingWorkers struct {
	*fakeWorkers
	t *testing.T
}

func (w *committingWorkers) Execute(ctx context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
	if task.GetRequest().MessageIs(&contracts.SourceDeploymentRequest{}) {
		checkout := task.GetWorkspace().GetCheckoutPath()
		if err := os.MkdirAll(checkout, 0o755); err != nil {
			w.t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(checkout, deploymentFile), []byte("resources: [app.yaml]\n"), 0o644); err != nil {
			w.t.Fatal(err)
		}
	}
	return w.fakeWorkers.Execute(ctx, task)
}

// Case B: the application ships its own deployment files. Go deploys the
// commit as it is and publishes it; no deployer task runs.
func TestCaseBDeploysTheCommitWithZeroDeployerTasks(t *testing.T) {
	h := newHarness(t, &fakeVerifier{verdicts: []lifecycle.HealthVerdict{clear}})
	r := &treeReadiness{fakeReadiness: h.readiness}
	h.coordinator.Readiness = r
	fake := h.coordinator.Broker.(*fakeBroker)
	cfg := config()
	if err := os.MkdirAll(filepath.Join(fake.root, cfg.OperationID+"-source"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fake.root, cfg.OperationID+"-source", deploymentFile), []byte("resources: []\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	result, err := h.coordinator.Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if h.workers.deployerAttempts() != 0 {
		t.Fatalf("deployer tasks = %d, want none for an application that ships its deployment", h.workers.deployerAttempts())
	}
	if h.readiness.calls != 1 || r.offline != 1 || h.readiness.plans[0] != nil {
		t.Fatalf("deploys=%d offline=%d plan=%v: want one deploy of the commit with no override", h.readiness.calls, r.offline, h.readiness.plans)
	}
	if _, ok := fake.evidence[cfg.OperationID+"-source"]; !ok || result.SourceCommit == "" || h.installer.installs != 1 {
		t.Fatalf("result %+v installs=%d: the commit must be published and installed", result, h.installer.installs)
	}
}

// Case A: the tree has no deployment files. The first deploy fails offline
// (no apply), the deployer gets that failing step, commits files, and Go
// deploys its commit.
func TestCaseAGivesTheDeployerTheMissingFilesAndDeploysItsCommit(t *testing.T) {
	h := newHarness(t, &fakeVerifier{verdicts: []lifecycle.HealthVerdict{clear}})
	r := &treeReadiness{fakeReadiness: h.readiness}
	h.coordinator.Readiness = r
	h.coordinator.Workers = &committingWorkers{fakeWorkers: h.workers, t: t}
	result, err := h.coordinator.Run(context.Background(), config())
	if err != nil {
		t.Fatal(err)
	}
	if h.workers.deployerAttempts() != 1 {
		t.Fatalf("deployer tasks = %d, want one", h.workers.deployerAttempts())
	}
	if feedback := h.workers.deployer[0].GetCorrectionFeedback(); !strings.Contains(feedback, "no deployment files") || !strings.Contains(feedback, "O1.render") {
		t.Fatalf("the deployer's first task must carry the failing step, got %q", feedback)
	}
	if r.offline != 2 || h.readiness.calls != 1 {
		t.Fatalf("offline=%d deploys=%d: the first round must not touch the cluster", r.offline, h.readiness.calls)
	}
	if result.SourceCommit == "" || h.installer.installs != 1 {
		t.Fatalf("result %+v installs=%d", result, h.installer.installs)
	}
}

// A failed deploy with no deployer agent registered is for a person: a
// DeploymentHoldError carrying the report, and no task.
func TestNoRegisteredDeployerMakesAFailureAHumanHold(t *testing.T) {
	h := newHarness(t, &fakeVerifier{verdicts: []lifecycle.HealthVerdict{clear}})
	h.coordinator.Registry = registryWithout(t, "source-deployment.json")
	h.coordinator.Readiness = &treeReadiness{fakeReadiness: h.readiness}
	_, err := h.coordinator.Run(context.Background(), config())
	var held *lifecycle.DeploymentHoldError
	if !errors.As(err, &held) || held.Stage != "deploy" || held.Report == nil || !strings.Contains(err.Error(), "no deployment files") {
		t.Fatalf("error = %v, want a deploy-stage DeploymentHoldError with the report", err)
	}
	if h.workers.deployerAttempts() != 0 || h.readiness.calls != 0 || h.installer.installs != 0 {
		t.Fatalf("tasks=%d deploys=%d installs=%d", h.workers.deployerAttempts(), h.readiness.calls, h.installer.installs)
	}
}

// A deploy failure after apply, with no deployer, is also a human hold.
func TestNoRegisteredDeployerHoldsAFailedApply(t *testing.T) {
	h := newHarness(t, &fakeVerifier{verdicts: []lifecycle.HealthVerdict{clear}})
	h.coordinator.Registry = registryWithout(t, "source-deployment.json")
	h.readiness.errors = []error{errors.New("chart values: unknown key")}
	_, err := h.coordinator.Run(context.Background(), config())
	var held *lifecycle.DeploymentHoldError
	if !errors.As(err, &held) || held.Stage != "deploy" || !strings.Contains(err.Error(), "unknown key") {
		t.Fatalf("error = %v, want a deploy-stage DeploymentHoldError", err)
	}
	if h.readiness.calls != 1 || h.workers.deployerAttempts() != 0 {
		t.Fatalf("deploys=%d tasks=%d", h.readiness.calls, h.workers.deployerAttempts())
	}
}

// The deployer's corrections are bounded by MaxAttempts.
func TestDeployerCorrectionsAreBounded(t *testing.T) {
	h := newHarness(t, &fakeVerifier{verdicts: []lifecycle.HealthVerdict{clear}})
	h.readiness.errors = []error{errors.New("rollout stuck: a"), errors.New("rollout stuck: b"), errors.New("rollout stuck: c"), errors.New("rollout stuck: d")}
	_, err := h.coordinator.Run(context.Background(), config())
	var stuck *lifecycle.AgentNotConvergingError
	if !errors.As(err, &stuck) || h.workers.deployerAttempts() != 3 || h.readiness.calls != 4 {
		t.Fatalf("err=%v tasks=%d deploys=%d, want three tasks after four failed deploys", err, h.workers.deployerAttempts(), h.readiness.calls)
	}
}

func registryWithout(t *testing.T, manifests ...string) *worker_registry.Registry {
	t.Helper()
	dir := t.TempDir()
	entries, err := os.ReadDir(registrytest.Directory())
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		skip := false
		for _, name := range manifests {
			skip = skip || entry.Name() == name
		}
		if skip || entry.IsDir() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(registrytest.Directory(), entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, entry.Name()), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	registry, err := worker_registry.LoadDirectory(dir)
	if err != nil {
		t.Fatal(err)
	}
	return registry
}
