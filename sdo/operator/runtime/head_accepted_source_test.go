package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/broker"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type cachedRevisionSource map[string]*unstructured.Unstructured

func (s cachedRevisionSource) AcceptedManifest(_ context.Context, revision string, _ sdk.ObjectRef) (*unstructured.Unstructured, error) {
	return s[revision].DeepCopy(), nil
}

func (s cachedRevisionSource) AcceptedManifests(_ context.Context, revision, _ string) ([]*unstructured.Unstructured, error) {
	if s[revision] == nil {
		return nil, errors.New("unexpected manifest revision: " + revision)
	}
	return []*unstructured.Unstructured{s[revision].DeepCopy()}, nil
}

func (s cachedRevisionSource) Revision(ctx context.Context, ref sdk.ObjectRef, revision string) (*unstructured.Unstructured, error) {
	return s.AcceptedManifest(ctx, revision, ref)
}

type headReceiptReader struct {
	lifecycle.HeadBroker
	revision string
	reads    int
	err      error
	pollErr  error
}

func (b *headReceiptReader) HeadToDeploy(context.Context) (broker.HeadChange, bool, error) {
	return broker.HeadChange{}, false, b.pollErr
}

func (b *headReceiptReader) TryAcceptedDeploymentRevision(context.Context) (string, error) {
	b.reads++
	return b.revision, b.err
}

func TestHeadAcceptanceRebindsSourceRepairWithoutRestart(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0)
	config := testControllerConfig()
	config.SourceCommit, config.DeployedCommit = "memory", "startup"
	config.ExternalIncidentsOnly = true
	c, err := NewController(config, nil, staticProvider{}, &recordingDispatcher{}, now)
	if err != nil {
		t.Fatal(err)
	}
	ledger := &headReceiptReader{revision: "v1"}
	c.SetHeadPoller(&HeadPoller{Deployer: lifecycle.HeadDeployer{Broker: ledger}})
	c.head.nextAt = now.Add(time.Hour)
	var invalidations int
	c.invalidateSourceCache = func() { invalidations++ }
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: config.Namespace, Name: "app"}
	manifest := func(image, field string) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": ref.APIVersion, "kind": ref.Kind,
			"metadata": map[string]any{"namespace": ref.Namespace, "name": ref.Name},
			"data":     map[string]any{"release": image, "configuration": field},
		}}
	}
	source := cachedRevisionSource{"v1": manifest("old", "v1-setting"), "v2": manifest("new", "v2-setting")}
	c.acceptDeploymentLocked(c.readAcceptedDeployment(ctx))
	if c.CurrentAcceptedCommit() != "memory" {
		t.Fatalf("startup deployment lookup changed accepted memory to %q", c.CurrentAcceptedCommit())
	}
	warm, err := source.AcceptedManifest(ctx, c.config.DeployedCommit, ref)
	if err != nil || warm.Object["data"].(map[string]any)["release"] != "old" {
		t.Fatalf("initial source: %v %v", warm, err)
	}
	// Head polling published the v2 deployment while this controller stayed up.
	c.handleHeadCompletion(reflectionCompletion{head: &lifecycle.HeadResult{Outcome: lifecycle.HeadDeployed, Commit: "v2"}})
	c.config.SourceCommit = "memory-after-v2"
	if err := c.StepEvents(ctx, now.Add(time.Second), nil); err != nil {
		t.Fatal(err)
	}
	request := c.incidentRequest(now, nil)
	for _, completion := range []reflectionCompletion{
		{head: &lifecycle.HeadResult{Outcome: lifecycle.HeadHeld, Commit: "v3"}},
		{head: &lifecycle.HeadResult{Outcome: lifecycle.HeadDeferred, Commit: "v3"}},
		{head: &lifecycle.HeadResult{Commit: "v3"}, err: errors.New("deploy refused or rolled back")},
	} {
		c.handleHeadCompletion(completion)
	}
	if ledger.reads != 1 || c.CurrentRenderedRevision() != "v2" {
		t.Fatalf("idle/refused poll changed the accepted render or read the broker: %q reads=%d", c.CurrentRenderedRevision(), ledger.reads)
	}
	observed := manifest("new", "drifted")
	snapshot := sdktest.Snapshot{NamespaceName: config.Namespace, ObjectSnapshotVersion: sdktest.ObjectsOnlySnapshotVersion,
		Objects: []unstructured.Unstructured{*observed}}
	view := LiveReadView{Carrier: inputs.New(snapshot), Source: source, RenderRevision: c.CurrentRenderedRevision}
	diff, err := view.DiffAgainstSource(ref)
	if err != nil {
		t.Fatal(err)
	}
	release, err := diff.Field("data.release")
	if err != nil || release.Differs() || release.Accepted != "new" {
		t.Fatalf("diagnose read stale source: %+v, %v", release, err)
	}
	c.ConfigureDrift(DriftControllerConfig{Inventory: true, Source: source})
	if err := c.prepareSourceInventory(ctx); err != nil || c.sourceInventory == nil || c.sourceInventory.Revision != "v2" {
		t.Fatalf("inventory read stale source: %v, %v", c.sourceInventory, err)
	}
	c.recordAdoptionError(now, ErrAcceptanceRecordUnavailable)
	if c.ownershipAdoption.SourceCommit != "v2" {
		t.Fatalf("adoption recorded memory as the source: %+v", c.ownershipAdoption)
	}
	executor := PlanExecutor{Source: source}
	loop := ResponderLoop{Request: request, Executor: executor, Now: func() time.Time { return now }}
	resp := loop.Handle(ctx, ToolRequest{Command: "source", Ref: &ref})
	if resp.Error != "" || !planValuesEqual(resp.Source, source["v2"].Object) {
		t.Fatalf("sdo source read stale source: %v, %v", resp.Source, resp.Error)
	}
	fromSource, err := executor.valueInputs(ctx, request).Source(ref)
	if err != nil || !planValuesEqual(fromSource, source["v2"].Object) {
		t.Fatalf("FromSource read stale source: %v, %v", fromSource, err)
	}
	next, err := executor.prepareAction(ctx, request, program.ReconcileFromSource{Ref: ref}, observed)
	if err != nil {
		t.Fatal(err)
	}
	if invalidations != 2 {
		t.Fatalf("startup and accepted deployment invalidated pinned renders %d times, want 2", invalidations)
	}
	if request.SourceCommit != "memory-after-v2" || request.RepositoryBaseCommit != "memory-after-v2" || request.DeployedCommit != "v2" || !planValuesEqual(next.Object, source["v2"].Object) {
		t.Fatalf("source %q deployment %q repaired to %v, want v2", request.SourceCommit, request.DeployedCommit, next.Object)
	}
	previous, err := executor.prepareAction(ctx, request, program.Rollback{Ref: ref, ToRevision: "v1"}, next)
	if err != nil || !planValuesEqual(previous.Object, source["v1"].Object) {
		t.Fatalf("explicit rollback lost its own target: %v %v", previous, err)
	}
}

func TestHeadReceiptInvalidatesRenderOfSameRevision(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0)
	config := testControllerConfig()
	config.SourceCommit, config.DeployedCommit = "v2", "v2"
	c, err := NewController(config, nil, staticProvider{}, &recordingDispatcher{}, now)
	if err != nil {
		t.Fatal(err)
	}
	ledger := &headReceiptReader{err: errors.New("ledger unavailable")}
	c.SetHeadPoller(&HeadPoller{Deployer: lifecycle.HeadDeployer{Broker: ledger}})
	invalidations := 0
	c.invalidateSourceCache = func() { invalidations++ }
	c.acceptDeploymentLocked(c.readAcceptedDeployment(ctx))
	if invalidations != 0 {
		t.Fatalf("before acceptance: %d invalidations", invalidations)
	}
	ledger.revision, ledger.err = "v2", nil
	retryAt := c.head.receiptRetryAt
	c.syncDeploymentBeforeOpening(ctx, retryAt.Add(-time.Second))
	if ledger.reads != 1 {
		t.Fatal("read the ledger before the retry was due")
	}
	c.syncDeploymentBeforeOpening(ctx, retryAt)
	c.syncDeploymentBeforeOpening(ctx, retryAt.Add(time.Minute))
	if invalidations != 1 || ledger.reads != 2 || !c.head.receiptRetryAt.IsZero() {
		t.Fatalf("same-revision receipt invalidated %d times, want 1", invalidations)
	}
}

func TestLiveMemoryReadsIgnoreRenderRevision(t *testing.T) {
	const architecture = "accepted architecture\n<!-- sdo:application-map:begin -->\n```yaml\nversion: 1\n```\n<!-- sdo:application-map:end -->\n"
	source := GitPlanSource{Repository: "/synthetic", Git: func(_ context.Context, _ string, args ...string) ([]byte, error) {
		if args[0] == "show" && len(args[1]) > 7 && args[1][:7] == "memory:" {
			return []byte(architecture), nil
		}
		return nil, errors.New("expected memory revision, got " + args[1])
	}}
	view := LiveReadView{Source: source, SourceCommit: "memory", RenderRevision: func() string { return "v1" }}
	if _, err := view.AppMap(); err != nil {
		t.Fatalf("AppMap read followed deployment revision: %v", err)
	}
	if text, err := view.ArchitectureText(); err != nil || text != architecture {
		t.Fatalf("memory read followed deployment revision: %q %v", text, err)
	}
}

func TestOpeningWaitsForRepairCompletionOrItsBound(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0)
	for _, path := range []string{"external", "page", "detector"} {
		t.Run(path, func(t *testing.T) {
			cfg := testControllerConfig()
			cfg.SourceCommit, cfg.DeployedCommit = "memory", "v1"
			cfg.ExternalIncidentsOnly = path == "external"
			c, err := NewController(cfg, nil, staticProvider{}, &recordingDispatcher{}, now)
			if err != nil {
				t.Fatal(err)
			}
			ledger := &headReceiptReader{revision: "v2"}
			c.SetHeadPoller(&HeadPoller{Deployer: lifecycle.HeadDeployer{Broker: ledger}})
			c.reflections.running = "repair"
			c.head.openingBound = now.Add(deploymentOpeningBound)
			alert := ExternalIncident{ID: "test", Namespace: cfg.Namespace, Message: "check"}
			open := func(at time.Time) {
				t.Helper()
				var err error
				switch path {
				case "external":
					err = c.OpenExternalIncident(ctx, at, alert)
				case "page":
					err = c.admitPage(ctx, at, sdktest.Snapshot{NamespaceName: cfg.Namespace}, alert)
				case "detector":
					err = c.dispatchReady(ctx, at, sdktest.Snapshot{NamespaceName: cfg.Namespace})
				}
				if err != nil && !errors.Is(err, ErrExternalIncidentBusy) {
					t.Fatal(err)
				}
			}
			c.batcher.AddAt(sdk.Finding{RuleID: "test", Status: sdk.FindingActive}, now)
			open(now.Add(time.Minute))
			if c.incidentOpen {
				t.Fatal("opened against a deployment awaiting completion")
			}
			c.syncDeploymentBeforeOpening(ctx, c.head.openingBound)
			open(now.Add(deploymentOpeningBound))
			if !c.incidentOpen || c.currentIncidentRequest.DeployedCommit != "v2" {
				t.Fatalf("after bound, expected incident on recorded v2: %+v", c.currentIncidentRequest)
			}
		})
	}
}

func TestHeadPollReadsReceiptOnErrorAndCurrent(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0)
	for _, pollErr := range []error{nil, errors.New("deployment operation failed after acceptance")} {
		ledger := &headReceiptReader{revision: "v2", pollErr: pollErr}
		cfg := testControllerConfig()
		cfg.SourceCommit, cfg.DeployedCommit = "memory", "v1"
		c, err := NewController(cfg, nil, staticProvider{}, &recordingDispatcher{}, now)
		if err != nil {
			t.Fatal(err)
		}
		c.SetHeadPoller(&HeadPoller{
			Config: func() (lifecycle.RepairConfig, error) {
				return lifecycle.RepairConfig{Namespace: cfg.Namespace, HealthObjective: "health"}, nil
			},
			Deployer: lifecycle.HeadDeployer{Broker: ledger,
				Deployer: lifecycle.RepairDeployer{Readiness: unusedReadiness{}, Verifier: unusedVerifier{}}},
		})
		c.driveHeadPoll(ctx, now)
		c.Wait()
		c.processBrokerCompletions()
		if ledger.reads != 1 || c.CurrentRenderedRevision() != "v2" || c.CurrentAcceptedCommit() != "memory" {
			t.Fatalf("poll %v ignored receipt: reads=%d render=%s memory=%s", pollErr, ledger.reads, c.CurrentRenderedRevision(), c.CurrentAcceptedCommit())
		}
	}
}
