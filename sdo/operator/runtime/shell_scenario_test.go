package runtime

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/shellpath"
	"sdo.dev/operator/worker_registry"
)

type shellScenarioClock struct{ nanos atomic.Int64 }

func (c *shellScenarioClock) Now() time.Time   { return time.Unix(0, c.nanos.Load()).UTC() }
func (c *shellScenarioClock) Set(at time.Time) { c.nanos.Store(at.UnixNano()) }

type shellScenarioReader struct {
	service *shellpath.Service
	clock   *shellScenarioClock
	reports *shellpath.ReportStore
}

func (r shellScenarioReader) Read(ctx context.Context) (shellpath.Report, error) {
	if err := r.service.Step(ctx, r.clock.Now(), false); err != nil {
		return shellpath.Report{}, err
	}
	return r.reports.Get(), nil
}

type shellScenarioDispatcher struct {
	preparer KubernetesJobDispatcher
	fastloopDispatch
}

func (d shellScenarioDispatcher) PrepareDispatch(ctx context.Context, r IncidentRequest) (IncidentRequest, error) {
	return d.preparer.PrepareDispatch(ctx, r)
}
func (d shellScenarioDispatcher) Dispatch(ctx context.Context, r IncidentRequest) (IncidentResult, error) {
	if r.PreparedAgentTask == "" {
		var err error
		r, err = d.preparer.PrepareDispatch(ctx, r)
		if err != nil {
			return IncidentResult{}, err
		}
	}

	task := &contracts.AgentTask{}
	if err := wire.Unmarshal([]byte(r.PreparedAgentTask), task); err != nil {
		return IncidentResult{}, err
	}
	request := &contracts.IncidentRequest{}
	if err := task.Request.UnmarshalTo(request); err != nil {
		return IncidentResult{}, err
	}
	if len(request.GetMemorySnapshot().GetDetectors()) == 0 || !strings.Contains(request.GetMemorySnapshot().GetDetectors()[0].GetDescription(), "check.sh") {
		return IncidentResult{}, fmt.Errorf("shell responder did not receive accepted scripts")
	}
	return d.fastloopDispatch.Dispatch(ctx, r)
}

// This scenario uses real bash, runner state, the real controller's independent
// closure, the production outcome deriver, and real broker publication. Only
// namespace observations and agent decisions are scripted; no cluster is needed.
func TestShellControllerClosureReflectionAndActivation(t *testing.T) {
	ctx := context.Background()
	root := nativeRepository(t)
	write := func(base, path, body string, mode os.FileMode) {
		t.Helper()
		absolute := filepath.Join(base, path)
		if err := os.MkdirAll(filepath.Dir(absolute), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(absolute, []byte(body), mode); err != nil {
			t.Fatal(err)
		}
	}
	write(root, ".sdo/goal.md", "Keep the application available.\n", 0644)
	write(root, ".sdo/arch.md", "One application namespace.\n", 0644)
	write(root, ".sdo/diagnostics/detectors/health/ready/check.sh", "#!/usr/bin/env bash\ntest ! -e \"$SDO_TEST_FAULT\"\n", 0755)
	nativeGit(t, root, "add", ".")
	nativeGit(t, root, "commit", "-qm", "accepted shell health")
	accepted := nativeGit(t, root, "rev-parse", "HEAD")
	fault := filepath.Join(t.TempDir(), "fault")
	if err := os.WriteFile(fault, []byte("active"), 0600); err != nil {
		t.Fatal(err)
	}
	start := time.Now().UTC()
	clock := &shellScenarioClock{}
	clock.Set(start)
	reports := &shellpath.ReportStore{}
	service := &shellpath.Service{Repository: &shellpath.GitRepository{Root: root, Scratch: t.TempDir()}, Reports: reports, Executor: shellpath.BashExecutor{Now: clock.Now, Env: append(os.Environ(), "SDO_TEST_FAULT="+fault)}}
	reader := shellScenarioReader{service: service, clock: clock, reports: reports}
	detector := &shellpath.Detector{Reader: reader, Namespace: "app", Commit: accepted}
	selected, err := NewCodePathRegistry().Resolve(CodePathConfig{Name: "shell"})
	if err != nil {
		t.Fatal(err)
	}
	repository, err := broker.New(root, filepath.Join(t.TempDir(), "proposals"), selected.Memory.Validate, broker.ObservationPolicy{CodePath: "shell"})
	if err != nil {
		t.Fatal(err)
	}
	native := NativeIncidentBroker{Repository: repository, LedgerRoot: filepath.Join(t.TempDir(), "ledger"), CodePath: CodePathConfig{Name: "shell"}, ShellReader: reader, Now: clock.Now, Deriver: ProtoOutcomeDeriver{}, ResponderBackend: "scripted", ResponderModel: "none"}
	reflections := 0
	native.Reflector = ReflectionWorkerFunc(func(_ context.Context, closure IncidentClosure, workspace IncidentWorkspace, outcome *contracts.OutcomeRecord, _, _ string, _ ReflectionFeedback) (ReflectionDecision, error) {
		reflections++
		if outcome.GetIncidentId() != closure.Request.IncidentID {
			t.Fatal("reflection lost outcome identity")
		}
		write(workspace.Worktree, ".sdo/diagnostics/detectors/incidents/learned/check.sh", "#!/usr/bin/env bash\necho learned\nexit 0\n", 0755)
		write(workspace.Worktree, ".sdo/playbooks/recovery/README.md", "Inspect detector evidence; remove the application fault.\n", 0644)
		return ReflectionDecision{LearningDecision: "updated", PlaybookIDs: []string{"recovery"}}, nil
	})
	registry, err := worker_registry.LoadDirectory(filepath.Join("..", "..", "agents", "registrations"))
	if err != nil {
		t.Fatal(err)
	}
	dispatcher := shellScenarioDispatcher{preparer: KubernetesJobDispatcher{Registry: registry, AgentID: "sdo-responder", Catalog: memory.OpenShellCatalogReader(root), RequireMemorySnapshot: true, CoverageProvider: selected.Coverage}, fastloopDispatch: fastloopDispatch{requests: make(chan IncidentRequest, 1), results: make(chan IncidentResult, 1)}}
	config := ControllerConfig{CodePath: CodePathConfig{Name: "shell"}, Application: "app", Namespace: "app", SourceCommit: accepted, DeployedCommit: accepted, RepositoryWorktree: root, ResponseTimeout: 3 * time.Minute, VerificationTimeout: time.Minute, FiringThreshold: 1, ClearThreshold: 1, RepairPolicy: "recorded-actions"}
	controller, err := NewController(config, []sdk.Detector{detector}, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}, dispatcher, start)
	if err != nil {
		t.Fatal(err)
	}
	controller.now = clock.Now
	if err := controller.SetIncidentBroker(native); err != nil {
		t.Fatal(err)
	}
	closed := make(chan IncidentClosure, 1)
	controller.OnIncidentClosed = func(c IncidentClosure) { closed <- c }
	step := func(offset time.Duration) {
		t.Helper()
		clock.Set(start.Add(offset))
		if err := controller.Step(ctx, clock.Now(), nil); err != nil {
			t.Fatal(err)
		}
	}
	step(0)
	if _, ok := controller.PendingWorkspaceEffect(); ok {
		t.Fatal("single nonzero result fired")
	}
	step(30 * time.Second)
	effect, ok := controller.PendingWorkspaceEffect()
	if !ok {
		t.Fatalf("confirmed script did not open incident: %+v", exportState(t, controller))
	}
	if err := controller.ExecuteWorkspaceEffect(ctx, effect); err != nil {
		t.Fatal(err)
	}
	awaitBrokerResult(t, controller.workspaceResults)
	step(30*time.Second + time.Millisecond)
	executePendingEffect(t, controller)
	var request IncidentRequest
	select {
	case request = <-dispatcher.requests:
	case result := <-controller.results:
		t.Fatalf("dispatch failed: %v", result.err)
	case <-time.After(2 * time.Second):
		t.Fatal("no shell dispatch")
	}

	if len(request.Findings) != 1 || !strings.Contains(request.Findings[0].Evidence, "check.sh") {
		t.Fatalf("script evidence lost: %+v", request.Findings)
	}
	if err := os.Remove(fault); err != nil {
		t.Fatal(err)
	}
	result := completedResult(request.IncidentID)
	result.ResponderSessionID = "scripted-shell-session"
	result.Timing.StartedAt = start.Add(31 * time.Second)
	result.Timing.CompletedAt = start.Add(32 * time.Second)
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Namespace", Name: "app"}
	result.RepairActions = []RepairActionReceipt{{ActionID: "repair", Kind: "script", Target: "app", Resources: []sdk.ObjectRef{ref}, Summary: "removed application fault", Details: "removed the fault fixture", StartedAt: result.Timing.StartedAt, CompletedAt: result.Timing.CompletedAt, Success: true}}
	result.ConfirmedRootCauses = []ConfirmedRootCause{{Summary: "application fault", Resources: []sdk.ObjectRef{ref}, ExplainedDetectors: []string{"shell.detectors"}, Evidence: []RootCauseEvidence{{Kind: "live-observation", Source: "shell detector", Observation: "nonzero before repair and zero after fault removal"}}}}
	dispatcher.results <- result
	awaitDispatchCompletionQueued(t, controller)
	step(60 * time.Second)
	select {
	case <-closed:
		t.Fatal("shell incident closed before the 15s stability window")
	default:
	}
	step(74 * time.Second)
	select {
	case <-closed:
		t.Fatal("shell incident closed before the 15s stability window")
	default:
	}
	step(75 * time.Second)
	var closure IncidentClosure
	select {
	case closure = <-closed:
	case <-time.After(time.Second):
		t.Fatal("stable script recovery did not close")
	}
	if closure.ClosureStabilityWindow != 15*time.Second {
		t.Fatalf("stability evidence lost: %s", closure.ClosureStabilityWindow)
	}
	receipt, err := native.ProcessClosure(ctx, closure)
	if err != nil {
		t.Fatal(err)
	}
	if reflections != 1 || receipt.ReflectionCommit == "" {
		t.Fatalf("reflection not published: %+v", receipt)
	}
	active, err := reader.Read(ctx)
	if err != nil || active.AcceptedCommit != receipt.ReflectionCommit || len(active.Results) != 2 {
		t.Fatalf("accepted scripts not activated: %#v %v", active, err)
	}
	ledger, err := native.readLedger(request.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	if ledger.RolloutExpectation != nil || ledger.ClosurePhaseTimestamps["shell_runner_activated"] == nil {
		t.Fatal("shell activation required rebuild or lost activation time")
	}
	stored, err := memory.OpenReader(root).Get(ctx, request.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Record.GetCodePath().GetName() != "shell" {
		t.Fatal("outcome lost code-path selection")
	}
	if err := native.AcknowledgeClosure(ctx, receipt); err != nil {
		t.Fatal(err)
	}
	historical, err := memory.SnapshotAt(ctx, memory.OpenShellCatalogReader(root), accepted)
	if err != nil || len(historical.GetDetectors()) != 1 {
		t.Fatalf("proposal snapshot moved with accepted head: %v %v", historical, err)
	}
}
