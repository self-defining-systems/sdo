//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sdo.dev/operator/contracts/settings"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/anypb"

	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/deployment"
	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/operability"
	"sdo.dev/operator/operatorcli/catalog/renderers"
	"sdo.dev/operator/operatorcli/catalog/telemetrysources"
	"sdo.dev/operator/operatorcli/catalog/upstreams"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/upstream"
)

// An advance recreates a finished Job whose template changed (#1025); the new
// Job's command can never succeed on the retained state. The real
// CommandBackend observes it (its kubectl replaced by a synthetic API server
// that serves the stamped Job with failing status) on an injected clock: the
// deploy must end in the typed rollout failure naming the Job, its last exit
// and attempts, and the advance must route it to the responder (E7) instead
// of waiting silently.

type failingJobCluster struct {
	*immutableCluster
	failing bool
	// reportFailed makes the Job report the Failed condition (backoffLimit);
	// otherwise its attempts keep failing under the limit.
	reportFailed bool
	now          time.Time
	waits        int
	progress     []string
}

func (c *failingJobCluster) backend() deployment.CommandBackend {
	return deployment.CommandBackend{
		ObserveLimit: 10 * time.Minute,
		Now:          func() time.Time { return c.now },
		Wait:         func(_ context.Context, d time.Duration) error { c.waits++; c.now = c.now.Add(d); return nil },
		Progress:     func(line string) { c.progress = append(c.progress, line) },
		Run: func(_ context.Context, _ string, args []string, _ []byte) ([]byte, error) {
			if len(args) > 1 && args[1] == "pods" {
				return []byte(`{"items":[{"metadata":{"name":"seed-x","creationTimestamp":"2026-11-05T00:00:00Z"},"status":{"phase":"Failed","containerStatuses":[{"name":"seed","state":{"terminated":{"exitCode":1,"reason":"Error","message":"bucket already exists"}}}]}}]}`), nil
			}
			doc := c.objects["Job/seed"].doc
			encoded, _ := json.Marshal(doc)
			var job map[string]any
			_ = json.Unmarshal(encoded, &job)
			meta := job["metadata"].(map[string]any)
			meta["namespace"], meta["uid"] = advChartNamespace, c.objects["Job/seed"].uid
			spec := job["spec"].(map[string]any)
			spec["selector"] = map[string]any{"matchLabels": map[string]any{"controller-uid": "x"}}
			status := map[string]any{"active": 1, "failed": 3}
			if c.reportFailed {
				status = map[string]any{"failed": 7, "conditions": []any{map[string]any{"type": "Failed", "status": "True", "reason": "BackoffLimitExceeded", "message": "Job has reached the specified backoff limit"}}}
			}
			job["status"] = status
			return json.Marshal(job)
		},
	}
}

// failureClassifyingReadiness classifies a deployment error with the
// production live checks (the shared stub passes everything).
type failureClassifyingReadiness struct{ advReadiness }

func (r failureClassifyingReadiness) CheckLive(ctx context.Context, path string, plan *contracts.DeploymentPlan, receipt lifecycle.DeploymentReceipt, err error) ([]operability.Check, error) {
	if err != nil {
		return deployment.LiveChecks(deployment.LiveInput{Err: err}), nil
	}
	return r.advReadiness.CheckLive(ctx, path, plan, receipt, nil)
}

func (c *failingJobCluster) Observe(ctx context.Context, id string) (deployment.WorkloadObservation, error) {
	if c.failing && strings.HasPrefix(id, "Job/") {
		return c.backend().Observe(ctx, id)
	}
	return c.immutableCluster.Observe(ctx, id)
}

// Each case onboards once (about half of the cost of a run is the broker's
// git work), so the two live in separate tests instead of one test with two
// onboardings.
func TestAdvanceRoutesARecreatedJobThatReportsFailedToTheResponder(t *testing.T) {
	advanceARecreatedJobThatCannotSucceed(t, true)
}

func TestAdvanceRoutesARecreatedJobWhoseAttemptsFailUntilTheDeadlineToTheResponder(t *testing.T) {
	advanceARecreatedJobThatCannotSucceed(t, false)
}

func advanceARecreatedJobThatCannotSucceed(t *testing.T, reportFailed bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	upstreamDir := filepath.Join(t.TempDir(), "upstream")
	if err := os.MkdirAll(upstreamDir, 0o755); err != nil {
		t.Fatal(err)
	}
	testutil.InitGit(t, upstreamDir)
	testutil.Git(t, upstreamDir, "checkout", "-q", "-B", "main")
	advWrite(t, upstreamDir, recreateChartFiles("1", "one"))
	testutil.Git(t, upstreamDir, "add", ".")
	testutil.Git(t, upstreamDir, "commit", "-qm", "release 1")

	config, err := anypb.New(&contracts.HelmRenderConfig{ChartPath: "chart"})
	if err != nil {
		t.Fatal(err)
	}
	source := &contracts.DeploymentSource{RendererId: "helm", Config: config}
	plan := advPlan(t, upstreamDir, source)

	cluster := &failingJobCluster{immutableCluster: &immutableCluster{}, reportFailed: reportFailed, now: time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC)}
	var deployer deployment.Deployer
	var accepted lifecycle.SourceReadiness
	o := onboardWith(t, advChartApp, mustBuiltin(t, telemetrysources.NoneProfileName, onbControl), onbOptions{
		prepare: func(t *testing.T, root string) {
			testutil.Git(t, filepath.Dir(root), "clone", "-q", upstreamDir, root)
			testutil.ConfigureGit(t, root)
			testutil.Git(t, root, "remote", "remove", "origin")
			deployer = deployment.Deployer{
				Namespace: advChartNamespace, Renderers: renderers.Catalog(), Images: advImages(t), Applier: cluster,
				TreeDigest: broker.ProposalTreeDigest, AcceptedRoot: root,
			}
			accepted = advReadiness{deployer}
		},
		plan:      plan,
		readiness: lazyReadiness{&accepted},
		sources:   []layout.Source{{Path: ".", Kind: "git", Config: map[string]any{"repository": upstreamDir, "ref": "main"}}},
	}, advChartApp.goodProposal())
	if o.err != nil {
		t.Fatalf("onboarding the chart: %v", o.err)
	}

	cluster.failing = true // from here the recreated Job can never succeed
	advWrite(t, upstreamDir, recreateChartFiles("2", "one"))
	testutil.Git(t, upstreamDir, "commit", "-qam", "release 2: new seed image")
	release2 := strings.TrimSpace(testutil.Git(t, upstreamDir, "rev-parse", "HEAD"))

	catalog := upstreams.Catalog()
	sources, err := memory.ReadSources(o.accepted, catalog)
	if err != nil || len(sources) != 1 {
		t.Fatalf("accepted sources = %v, %v", sources, err)
	}
	prev, _, err := o.repository.LastUpstream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	latest, err := catalog.Latest(ctx, sources[0].Kind, sources[0].Config)
	if err != nil || latest.ID != release2 {
		t.Fatalf("latest = %+v, %v", latest, err)
	}
	advClock := &advTestClock{at: time.Unix(1_800_000_000, 0)}
	advancer := lifecycle.Advancer{
		Upstreams: catalog, Broker: o.repository, Readiness: failureClassifyingReadiness{advReadiness{deployer}},
		Verifier: onbVerifier{},
		Shape: func(ctx context.Context, root, revision string, source *contracts.DeploymentSource) (string, error) {
			return rendered.Source{Deployment: source, Catalog: renderers.Catalog(), Repository: root}.ShapeDigest(ctx, revision, advChartNamespace)
		},
		Maintenance: advMaintenance{}, Activity: advMaintenance{},
		Wait: advClock.wait, Now: advClock.now,
	}
	result, err := advancer.Advance(ctx, lifecycle.AdvanceConfig{SettingsDigest: settings.Default().DetectionDigest(),
		Namespace: advChartNamespace, HealthObjective: "every synthetic workload stays available", GitDir: filepath.Join(o.accepted, ".git"),
		PendingEvaluations: 1, InfrastructureTries: 1,
	}, upstream.Request{Source: sources[0], Revision: latest, Prev: prev})
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	if result.Outcome != lifecycle.AdvanceHeld || result.Hold != lifecycle.HoldRollout || result.Cause != operability.CauseRollout || result.Owner != operability.OwnerResponder {
		t.Fatalf("advance = %s hold %s cause %s owner %s: %s", result.Outcome, result.Hold, result.Cause, result.Owner, result.Detail)
	}
	for _, want := range []string{"Job/", "seed", "failed attempts", "exit code 1"} {
		if !strings.Contains(result.Detail, want) {
			t.Errorf("hold detail lacks %q: %s", want, result.Detail)
		}
	}
	// Bounded by the rollout deadline on the injected clock.
	if waited := cluster.now.Sub(time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC)); waited > 10*time.Minute+time.Minute {
		t.Errorf("waited %s, past the 10m rollout deadline", waited)
	}
	if !reportFailed && len(cluster.progress) == 0 {
		t.Error("a wait on a failing Job logged no progress")
	}
}
