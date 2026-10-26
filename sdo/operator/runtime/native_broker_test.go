package runtime

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
)

func testDerivedOutcome(id, classification string, reflect bool) DerivedOutcome {
	return DerivedOutcome{Record: &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: id, Classification: classification}, RequiresReflection: reflect}
}

type rejectingLatePull struct{}

func (rejectingLatePull) VerifyPull(context.Context, string, uint32, string, []string, []string) error {
	return errors.New("stale report digest")
}

func TestNativeBrokerRejectsUnverifiedLatePullBeforeLedger(t *testing.T) {
	t.Parallel()
	target := nativeRepository(t)
	repository, err := broker.New(target, filepath.Join(t.TempDir(), "proposals"), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	workspace, err := repository.CreateProposal(context.Background(), "incident-1")
	if err != nil {
		t.Fatal(err)
	}
	closure := nativeClosure(IncidentWorkspace{IncidentID: "incident-1", Worktree: workspace.Path, BaseCommit: workspace.BaseCommit})
	closure.Result.LateFindingsPull = &LateFindingsPull{IncidentID: "incident-1", PullCount: 1, ReportDigest: strings.Repeat("a", 64)}
	root := filepath.Join(t.TempDir(), "ledger")
	native := NativeIncidentBroker{Repository: repository, LedgerRoot: root, Deriver: ProtoOutcomeDeriver{}, ResponderBackend: "test", ResponderModel: "test", LateFindingsVerifier: rejectingLatePull{}}
	if _, err := native.ProcessClosure(context.Background(), closure); err == nil || !strings.Contains(err.Error(), "stale report digest") {
		t.Fatalf("unverified pull accepted: %v", err)
	}
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatalf("broker wrote ledger before verifying pull: %v", err)
	}
}

func nativeGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	return testutil.Git(t, dir, args...)
}

func nativeRepository(t *testing.T) string {
	t.Helper()
	root := filepath.Join(t.TempDir(), "accepted")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	testutil.InitGit(t, root)
	nativeGit(t, root, "config", "user.name", "Test")
	nativeGit(t, root, "config", "user.email", "test@example.com")
	if err := os.WriteFile(filepath.Join(root, "app.txt"), []byte("base\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	nativeGit(t, root, "add", ".")
	nativeGit(t, root, "commit", "-qm", "base")
	return root
}

func nativeMemoryFixture(t *testing.T, root string) {
	t.Helper()
	files := map[string]string{
		".sdo/schema-version":           "3\n",
		".sdo/goal.md":                  "---\nschema_version: 1\nowner: human\napplication: test\n---\nGoal\n",
		".sdo/arch.md":                  "---\nschema_version: 1\ngenerated_at_commit: base\ngenerated_at: 2026-01-01T00:00:00Z\napplication: test\ntopology_fingerprint: abc\n---\nArchitecture\n",
		".sdo/playbooks/README.md":      "# Playbooks\n\n- [base](base/README.md)\n",
		".sdo/playbooks/base/README.md": "---\nschema_version: 1\nowner: responder\nfault_class: base\noriginating_incident: initial\n---\nRepair <RESOURCE_NAME>\n",
		".sdo/diagnostics/detectors/health/health/manifest.yaml": nativeDetectorManifest("health", "health", "initial"),
		".sdo/diagnostics/detectors/health/health/detector.go":   "package objective\n",
	}
	for name, content := range files {
		path := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	nativeGit(t, root, "add", ".")
	nativeGit(t, root, "commit", "-qm", "initial memory")
}

func nativeClosure(workspace IncidentWorkspace) IncidentClosure {
	now := time.Now().UTC()
	return IncidentClosure{
		Request: IncidentRequest{
			SchemaVersion: ProtocolSchemaVersion, Application: "app", Namespace: "default", IncidentID: workspace.IncidentID,
			Findings:        []sdk.Finding{stateFinding("health")},
			DetectorHistory: []DetectorEvaluation{{DetectorID: "health", EvaluatedAt: now, Status: DetectorEvaluationFiring}}, SourceCommit: workspace.BaseCommit, DeployedCommit: workspace.BaseCommit,
			RepositoryWorktree: workspace.Worktree, RepositoryBaseCommit: workspace.BaseCommit,
			ResponseDeadline: now.Add(time.Minute), CancellationToken: "token", RepairPolicy: "commit",
		},
		Result: &IncidentResult{
			SchemaVersion: ProtocolSchemaVersion, IncidentID: workspace.IncidentID, Status: IncidentCompleted,
			Timing: TimingMetrics{StartedAt: now, CompletedAt: now.Add(time.Second)},
		},
		FinalDetectorStates: []DetectorEvaluation{{DetectorID: "health", EvaluatedAt: now.Add(time.Second), Status: DetectorEvaluationClear}},
		DetectedAt:          now, DispatchedAt: now, ResponderCompletedAt: now.Add(time.Second), VerifiedAt: now.Add(time.Second),
	}
}

func TestNativeBrokerRecoversCommittedPhasesAndRequiresReflection(t *testing.T) {
	t.Parallel()
	target := nativeRepository(t)
	repositoryBroker, err := broker.New(target, filepath.Join(t.TempDir(), "proposals"), func(_ context.Context, _ string, _ []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	ledgerRoot := filepath.Join(t.TempDir(), "ledger")
	derivations := 0
	deriver := OutcomeDeriverFunc(func(_ context.Context, closure IncidentClosure, _, _, _ string) (DerivedOutcome, error) {
		derivations++
		return testDerivedOutcome(closure.Request.IncidentID, "success", true), nil
	})
	native := NativeIncidentBroker{Repository: repositoryBroker, LedgerRoot: ledgerRoot, Deriver: deriver, ResponderBackend: "test", ResponderModel: "test"}
	sessions := t.TempDir()
	native.SessionRoot = sessions
	native.SessionRetention = time.Hour
	if _, err := memory.PrepareIncidentSession(sessions, "", "inc-native"); err != nil {
		t.Fatal(err)
	}
	workspace, err := native.PrepareIncident(context.Background(), "inc-native")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace.Worktree, "app.txt"), []byte("repair\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	closure := nativeClosure(workspace)
	if _, err := native.ProcessClosure(context.Background(), closure); err == nil {
		t.Fatal("success acknowledged without reflection worker")
	}
	if nativeGit(t, target, "log", "--oneline") == "" {
		t.Fatal("proposal/outcome were not persisted before retry")
	}
	native.Reflector = ReflectionWorkerFunc(func(_ context.Context, _ IncidentClosure, reflection IncidentWorkspace, _ *contracts.OutcomeRecord, _, _ string, _ ReflectionFeedback) (ReflectionDecision, error) {
		if reflection.BaseCommit == workspace.BaseCommit {
			t.Fatal("reflection was not based on committed outcome")
		}
		return ReflectionDecision{LearningDecision: "no_change", NoChangeReason: "already learned"}, nil
	})
	receipt, err := native.ProcessClosure(context.Background(), closure)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.ProposalCommit == "" || receipt.OutcomeCommit == "" || receipt.AckToken == "" {
		t.Fatalf("incomplete receipt: %+v", receipt)
	}
	if derivations != 1 {
		t.Fatalf("outcome was re-derived across restart: %d", derivations)
	}
	// Simulate a crash after Git commits and before the ledger stored their
	// receipts. Attribution recovers both commits without publishing again.
	ledger, err := native.readLedger(workspace.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	ledger.ProposalCommit, ledger.OutcomeCommit = "", ""
	ledger.ReflectionProcessed = false
	ledger.Receipt = nil
	if err := native.saveLedger(ledger); err != nil {
		t.Fatal(err)
	}
	commitCount := nativeGit(t, target, "rev-list", "--count", "HEAD")
	other := native
	again, err := other.ProcessClosure(context.Background(), closure)
	if err != nil || again != receipt {
		t.Fatalf("retry returned %+v, %v; want %+v", again, err, receipt)
	}
	if nativeGit(t, target, "rev-list", "--count", "HEAD") != commitCount {
		t.Fatal("recovery duplicated an accepted commit")
	}
	if err := other.AcknowledgeClosure(context.Background(), receipt); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(sessions, ".sdo-runtime", "sessions")); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(filepath.Join(sessions, ".sdo-runtime", "sessions"))
	if err != nil || len(entries) != 1 {
		t.Fatalf("acknowledged incident session was lost before evidence export: %+v %v", entries, err)
	}
	openSession, err := memory.PrepareIncidentSession(sessions, "", "inc-open")
	if err != nil {
		t.Fatal(err)
	}
	ledger, err = native.readLedger(workspace.IncidentID)
	if err != nil || ledger.AcknowledgedAt == nil {
		t.Fatalf("acknowledgment timestamp not durable: %+v %v", ledger, err)
	}
	// A queued reflection resumes this session, so it outlives retention.
	ledger.ReflectionQueued, ledger.ReflectionProcessed = true, false
	if err := native.saveLedger(ledger); err != nil {
		t.Fatal(err)
	}
	if err := native.PruneIncidentSessions(ledger.AcknowledgedAt.AsTime().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(sessions, ".sdo-runtime", "sessions", entries[0].Name())); err != nil {
		t.Fatalf("session pruned while its reflection is queued: %v", err)
	}
	ledger.ReflectionQueued, ledger.ReflectionProcessed = false, true
	if err := native.saveLedger(ledger); err != nil {
		t.Fatal(err)
	}
	if err := native.PruneIncidentSessions(ledger.AcknowledgedAt.AsTime().Add(59 * time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(sessions, ".sdo-runtime", "sessions", entries[0].Name())); err != nil {
		t.Fatalf("session pruned before retention expiry: %v", err)
	}
	if err := native.PruneIncidentSessions(ledger.AcknowledgedAt.AsTime().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	entries, err = os.ReadDir(filepath.Join(sessions, ".sdo-runtime", "sessions"))
	if err != nil || len(entries) != 1 {
		t.Fatalf("expired session was not pruned or open session was lost: %+v %v", entries, err)
	}
	if entries[0].Name() != filepath.Base(openSession) {
		t.Fatalf("remaining session does not belong to open incident: %s", entries[0].Name())
	}
	if err := other.AcknowledgeClosure(context.Background(), receipt); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(workspace.Worktree); !os.IsNotExist(err) {
		t.Fatalf("proposal still mounted after ack: %v", err)
	}
}

func TestNativeClosureRecordsMonotonicPhaseTimestamps(t *testing.T) {
	target := nativeRepository(t)
	validations := 0
	var phaseLog bytes.Buffer
	repository, err := broker.New(target, filepath.Join(t.TempDir(), "proposals"), func(context.Context, string, []string) error {
		validations++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	native := NativeIncidentBroker{
		Repository: repository, LedgerRoot: filepath.Join(t.TempDir(), "ledger"),
		Deriver: OutcomeDeriverFunc(func(_ context.Context, closure IncidentClosure, _, _, _ string) (DerivedOutcome, error) {
			return testDerivedOutcome(closure.Request.IncidentID, "success", true), nil
		}),
		Reflector: ReflectionWorkerFunc(func(context.Context, IncidentClosure, IncidentWorkspace, *contracts.OutcomeRecord, string, string, ReflectionFeedback) (ReflectionDecision, error) {
			return ReflectionDecision{LearningDecision: "no_change", NoChangeReason: "already accepted"}, nil
		}),
		ResponderBackend: "test", ResponderModel: "test", PhaseLog: &phaseLog,
	}
	workspace, err := native.PrepareIncident(context.Background(), "timed")
	if err != nil {
		t.Fatal(err)
	}
	closure := nativeClosure(workspace)
	closure.Request.RepairPolicy = "recorded-actions"
	receipt, err := native.ProcessClosure(context.Background(), closure)
	if err != nil {
		t.Fatal(err)
	}
	if err := native.AcknowledgeClosure(context.Background(), receipt); err != nil {
		t.Fatal(err)
	}
	if validations != 0 {
		t.Fatalf("no-change reflection launched %d validator runs", validations)
	}
	ledger, err := native.readLedger("timed")
	if err != nil {
		t.Fatal(err)
	}
	var previous time.Time
	for _, phase := range []string{"closure_start", "outcome_write", "broker_commit", "reflection_dispatch", "worker_result_picked_up", "closure_ack"} {
		at := controllerTime(ledger.ClosurePhaseTimestamps[phase])
		if at.IsZero() || (!previous.IsZero() && at.Before(previous)) {
			t.Fatalf("phase %s is missing or out of order: %+v", phase, ledger.ClosurePhaseTimestamps)
		}
		previous = at
		if !strings.Contains(phaseLog.String(), `"closure_phase":"`+phase+`"`) {
			t.Fatalf("phase %s missing from the phase log: %s", phase, phaseLog.String())
		}
	}
}

type priorOutcomeReader struct{ outcomes []memory.Outcome }

func (r priorOutcomeReader) List(context.Context) ([]memory.Outcome, error) { return r.outcomes, nil }
func (r priorOutcomeReader) Get(context.Context, string) (memory.Outcome, error) {
	return memory.Outcome{}, memory.ErrNotFound
}

func TestNativeBrokerCompletesClosureAfterTypedReflectionFailure(t *testing.T) {
	accepted := nativeRepository(t)
	repository, err := broker.New(accepted, filepath.Join(t.TempDir(), "proposals"), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	native := NativeIncidentBroker{
		Repository: repository, LedgerRoot: filepath.Join(t.TempDir(), "ledger"),
		ResponderBackend: "test", ResponderModel: "test",
		Deriver: OutcomeDeriverFunc(func(_ context.Context, closure IncidentClosure, _, _, _ string) (DerivedOutcome, error) {
			return testDerivedOutcome(closure.Request.IncidentID, "success", true), nil
		}),
		Reflector: ReflectionWorkerFunc(func(_ context.Context, _ IncidentClosure, _ IncidentWorkspace, _ *contracts.OutcomeRecord, _, _ string, _ ReflectionFeedback) (ReflectionDecision, error) {
			calls++
			return ReflectionDecision{LearningDecision: "failed", Error: "watch namespace is not a proto field"}, nil
		}),
	}
	workspace, err := native.PrepareIncident(context.Background(), "inc-reflection-failed")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace.Worktree, "app.txt"), []byte("repair\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	closure := nativeClosure(workspace)
	first, err := native.ProcessClosure(context.Background(), closure)
	if err != nil {
		t.Fatal(err)
	}
	second, err := native.ProcessClosure(context.Background(), closure)
	if err != nil || first != second || calls != 1 {
		t.Fatalf("failed reflection retried: first=%+v second=%+v calls=%d err=%v", first, second, calls, err)
	}
	ledger, err := native.readLedger(workspace.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	if !ledger.ReflectionProcessed || ledger.ReflectionDecision == nil || ledger.ReflectionDecision.Error != "watch namespace is not a proto field" || ledger.ReflectionCommit != "" {
		t.Fatalf("reflection failure not recorded: %+v", ledger)
	}
}

func TestNativeBrokerPersistsSDKGapForVerifiedIncident(t *testing.T) {
	t.Parallel()
	target := nativeRepository(t)
	repository, err := broker.New(target, filepath.Join(t.TempDir(), "proposals"), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	native := NativeIncidentBroker{
		Repository: repository, LedgerRoot: filepath.Join(t.TempDir(), "ledger"),
		Deriver: OutcomeDeriverFunc(func(_ context.Context, closure IncidentClosure, _, _, _ string) (DerivedOutcome, error) {
			return testDerivedOutcome(closure.Request.IncidentID, "success", true), nil
		}),
		ResponderBackend: "test", ResponderModel: "test",
		Reflector: ReflectionWorkerFunc(func(_ context.Context, _ IncidentClosure, _ IncidentWorkspace, _ *contracts.OutcomeRecord, _, _ string, _ ReflectionFeedback) (ReflectionDecision, error) {
			return ReflectionDecision{LearningDecision: "sdk_gap", NoChangeReason: "UDP health check needed", SDKGaps: []*contracts.SdkGap{{FaultClass: "service-port-mismatch", Primitive: "ProbeUDP", Reason: "UDP health check needed", ScratchScript: "kubectl run probe"}}}, nil
		}),
	}
	workspace, err := native.PrepareIncident(context.Background(), "sdk-gap-incident")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace.Worktree, "app.txt"), []byte("repair\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	closure := nativeClosure(workspace)
	if _, err := native.ProcessClosure(context.Background(), closure); err != nil {
		t.Fatal(err)
	}
	ledger, err := native.readLedger(workspace.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	if !ledger.ReflectionProcessed || ledger.ReflectionDecision == nil || len(ledger.ReflectionDecision.SdkGaps) != 1 || ledger.ReflectionDecision.SdkGaps[0].Primitive != "ProbeUDP" {
		t.Fatalf("SDK gap missing from incident ledger: %+v", ledger.ReflectionDecision)
	}
	outcome, err := memory.NewJSONStore(target).Get(context.Background(), workspace.IncidentID)
	if err != nil || len(outcome.Record.GetSdkGaps()) != 1 || outcome.Record.SdkGaps[0].ScratchScript != "kubectl run probe" {
		t.Fatalf("SDK gap missing from accepted outcome: %+v %v", outcome, err)
	}
}

func TestNativeBrokerCompletesAfterDeterministicReflectionRejectionAndRunsNextIncident(t *testing.T) {
	t.Parallel()
	target := nativeRepository(t)
	nativeMemoryFixture(t, target)
	repositoryBroker, err := broker.New(target, filepath.Join(t.TempDir(), "proposals"), func(_ context.Context, _ string, _ []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	native := NativeIncidentBroker{
		Repository: repositoryBroker, LedgerRoot: filepath.Join(t.TempDir(), "ledger"), ResponderBackend: "test", ResponderModel: "test",
		Deriver: OutcomeDeriverFunc(func(_ context.Context, c IncidentClosure, _, _, _ string) (DerivedOutcome, error) {
			return testDerivedOutcome(c.Request.IncidentID, "success", true), nil
		}),
		Reflector: ReflectionWorkerFunc(func(_ context.Context, _ IncidentClosure, w IncidentWorkspace, _ *contracts.OutcomeRecord, _, _ string, _ ReflectionFeedback) (ReflectionDecision, error) {
			calls++
			if w.IncidentID != "inc-reject" {
				return ReflectionDecision{LearningDecision: "no_change", NoChangeReason: "already learned"}, nil
			}
			return ReflectionDecision{LearningDecision: "updated", PlaybookIDs: []string{"base"}}, os.WriteFile(filepath.Join(w.Worktree, ".sdo", "playbooks", "base", "README.md"), []byte("bad playbook\n"), 0o644)
		}),
	}
	workspace, err := native.PrepareIncident(context.Background(), "inc-reject")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace.Worktree, "app.txt"), []byte("repair\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	closure := nativeClosure(workspace)
	if _, err := native.ProcessClosure(context.Background(), closure); err != nil {
		t.Fatalf("invalid learning blocked verified closure: %v", err)
	}
	if _, err := repositoryBroker.ProposalInfo("inc-reject", broker.ReflectionPhase); err == nil {
		t.Fatal("rejected reflection checkout remained")
	}
	if _, err := native.ProcessClosure(context.Background(), closure); err != nil {
		t.Fatal(err)
	}
	ledger, err := native.readLedger("inc-reject")
	if err != nil || !ledger.ReflectionProcessed || !ledger.ReflectionRejected || len(ledger.ReflectionRejections) != broker.MaxValidationRetries+1 || calls != broker.MaxValidationRetries+1 {
		t.Fatalf("rejection was retried or lost: ledger=%+v calls=%d err=%v", ledger, calls, err)
	}
	next, err := native.PrepareIncident(context.Background(), "inc-next")
	if err != nil {
		t.Fatal(err)
	}
	nextClosure := nativeClosure(next)
	nextClosure.Request.RepairPolicy = "recorded-actions"
	if _, err := native.ProcessClosure(context.Background(), nextClosure); err != nil {
		t.Fatalf("next incident blocked by failed learning: %v", err)
	}
	if got := nativeGit(t, target, "show", "HEAD:.sdo/playbooks/base/README.md"); strings.Contains(got, "bad playbook") {
		t.Fatal("rejected reflection reached accepted state")
	}
}

func TestNativeBrokerRecordedActionsDoNotCreateEmptyProposalCommit(t *testing.T) {
	t.Parallel()
	target := nativeRepository(t)
	repositoryBroker, err := broker.New(target, filepath.Join(t.TempDir(), "proposals"), nil)
	if err != nil {
		t.Fatal(err)
	}
	native := NativeIncidentBroker{
		Repository: repositoryBroker, LedgerRoot: filepath.Join(t.TempDir(), "ledger"), ResponderBackend: "test", ResponderModel: "test",
		Deriver: OutcomeDeriverFunc(func(_ context.Context, c IncidentClosure, _, _, _ string) (DerivedOutcome, error) {
			return testDerivedOutcome(c.Request.IncidentID, "cleared_without_action", false), nil
		}),
	}
	w, err := native.PrepareIncident(context.Background(), "inc-live-only")
	if err != nil {
		t.Fatal(err)
	}
	closure := nativeClosure(w)
	closure.Request.RepairPolicy = "recorded-actions"
	receipt, err := native.ProcessClosure(context.Background(), closure)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.ProposalCommit != "" || receipt.OutcomeCommit == "" {
		t.Fatalf("unexpected commits: %+v", receipt)
	}
	if nativeGit(t, target, "rev-list", "--count", "HEAD") != "2" {
		t.Fatal("recorded action path created an extra empty commit")
	}
}

func TestNativeBrokerNeverPublishesRepairOfTimedOutClosure(t *testing.T) {
	t.Parallel()
	target := nativeRepository(t)
	repositoryBroker, err := broker.New(target, filepath.Join(t.TempDir(), "proposals"), nil)
	if err != nil {
		t.Fatal(err)
	}
	native := NativeIncidentBroker{
		Repository: repositoryBroker, LedgerRoot: filepath.Join(t.TempDir(), "ledger"), ResponderBackend: "test", ResponderModel: "test",
		Deriver: OutcomeDeriverFunc(func(_ context.Context, c IncidentClosure, _, _, _ string) (DerivedOutcome, error) {
			return testDerivedOutcome(c.Request.IncidentID, "failed", false), nil
		}),
	}
	w, err := native.PrepareIncident(context.Background(), "inc-timeout")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(w.Worktree, "unverified.txt"), []byte("repair\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	closure := nativeClosure(w)
	closure.Request.RepairPolicy = "commit"
	closure.FinalDetectorStates = nil
	receipt, err := native.ProcessClosure(context.Background(), closure)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.ProposalCommit != "" || receipt.OutcomeCommit == "" {
		t.Fatalf("timed-out closure published its repair: %+v", receipt)
	}
}

func TestProtoOutcomeDeriverAcceptsRuntimeClosure(t *testing.T) {
	closure := nativeClosure(IncidentWorkspace{IncidentID: "inc-proto", Worktree: "/proposal", BaseCommit: "base"})
	derived, err := (ProtoOutcomeDeriver{}).Derive(context.Background(), closure, "", "test", "test")
	if err != nil {
		t.Fatal(err)
	}
	if derived.Record.GetIncidentId() != "inc-proto" {
		t.Fatalf("wrong outcome: %+v", derived.Record)
	}
}

func TestNativeBrokerRequiresVerifiedDetectorRolloutBeforeAck(t *testing.T) {
	t.Parallel()
	target := nativeRepository(t)
	nativeMemoryFixture(t, target)
	fixture := "../../../tests/fixtures/sdo/playbook_program_app/.sdo"
	module, err := os.ReadFile(filepath.Join(fixture, "diagnostics/go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(target, ".sdo/diagnostics/go.mod"), module, 0644); err != nil {
		t.Fatal(err)
	}
	testutil.CopyPortProgram(t, target, fixture)
	// This scenario publishes a new detector, not a provenance rewrite of the
	// detector now included by the shared accepted-program fixture.
	if err := os.RemoveAll(filepath.Join(target, ".sdo/diagnostics/detectors/incidents/routing-relations")); err != nil {
		t.Fatal(err)
	}

	copyFastloopTree(t, filepath.Join(fixture, "corpus"), filepath.Join(target, ".sdo/corpus"))
	index := filepath.Join(target, ".sdo/playbooks/README.md")
	data, _ := os.ReadFile(index)
	if err := os.WriteFile(index, append(data, []byte("\n- [service-targetport-mismatch](service-targetport-mismatch/README.md)\n")...), 0644); err != nil {
		t.Fatal(err)
	}
	nativeGit(t, target, "add", ".")
	nativeGit(t, target, "commit", "-qm", "accepted port program")
	repositoryBroker, err := broker.New(target, filepath.Join(t.TempDir(), "proposals"), func(_ context.Context, _ string, _ []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	native := NativeIncidentBroker{
		Repository: repositoryBroker, LedgerRoot: filepath.Join(t.TempDir(), "ledger"), SessionRoot: t.TempDir(), ResponderBackend: "test", ResponderModel: "test",
		Deriver: OutcomeDeriverFunc(func(_ context.Context, closure IncidentClosure, _, _, _ string) (DerivedOutcome, error) {
			outcome := testDerivedOutcome(closure.Request.IncidentID, "success", true)
			ref := &contracts.ObjectRef{ApiVersion: "v1", Kind: "Service", Namespace: "demo", Name: "front"}
			outcome.Record.ConfirmedRootCauses = []*contracts.ConfirmedRootCause{{Summary: "verified service port repair", Resources: []*contracts.ObjectRef{ref}}}
			outcome.Record.Findings = []*contracts.Finding{{DetectorId: "routing-relations", RuleId: "targetport", Status: "active", Severity: "warn", Summary: "target port mismatch", Evidence: "selected pod has a different port", Fingerprint: "port-mismatch", PrimaryResource: ref}}
			outcome.Record.RepairActions = []*contracts.RepairActionReceipt{{ActionId: "repair-port", Kind: "kubectl", Target: "Service/demo/front", Summary: "restore target port", Details: "target port restored and read back", Success: true, Resources: []*contracts.ObjectRef{ref}, StartedAt: timestamppb.New(closure.DetectedAt), CompletedAt: timestamppb.New(closure.ResponderCompletedAt)}}
			outcome.Record.DetectorHistory = []*contracts.DetectorEvaluation{{DetectorId: "routing-relations", Status: "clear", EvaluatedAt: timestamppb.New(closure.VerifiedAt)}}
			return outcome, nil
		}),
		Reflector: ReflectionWorkerFunc(func(_ context.Context, _ IncidentClosure, workspace IncidentWorkspace, _ *contracts.OutcomeRecord, _, _ string, _ ReflectionFeedback) (ReflectionDecision, error) {
			fastloopWriteDetector(t, workspace.Worktree, "inc-rollout")
			return ReflectionDecision{LearningDecision: "updated"}, nil
		}),
	}
	capture, err := memory.IncidentCorpusDir(native.SessionRoot, "inc-rollout")
	if err != nil {
		t.Fatal(err)
	}
	healthy := filepath.Join(capture, "healthy")
	if err := os.MkdirAll(healthy, 0755); err != nil {
		t.Fatal(err)
	}
	baseline, err := os.ReadFile(filepath.Join(fixture, "corpus/targetport-mismatch/healthy.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(healthy, "healthy.json"), baseline, 0644); err != nil {
		t.Fatal(err)
	}
	workspace, err := native.PrepareIncident(context.Background(), "inc-rollout")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace.Worktree, "app.txt"), []byte("repair\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	closure := nativeClosure(workspace)
	closure.Result.ResponderSessionID = "rollout-session"
	receipt, err := native.ProcessClosure(context.Background(), closure)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.ReflectionCommit == "" {
		ledger, err := native.readLedger(workspace.IncidentID)
		t.Fatalf("reflection produced no detector commit: ledger=%v err=%v", ledger, err)
	}
	expectation, pending, err := native.PendingControllerRollout(receipt.ReflectionCommit)
	if err != nil || !pending || expectation.AfterDetectorFingerprint == expectation.BeforeDetectorFingerprint {
		t.Fatalf("rollout = %+v, %v, %v", expectation, pending, err)
	}
	if err := native.AcknowledgeClosure(context.Background(), receipt); err == nil {
		t.Fatal("ack before rollout verification")
	}
	recovered := native
	handoffs, err := recovered.PendingControllerRollouts()
	if err != nil || len(handoffs) != 1 || handoffs[0].Receipt != receipt || handoffs[0].Expectation != expectation {
		t.Fatalf("restart handoff = %+v, %v", handoffs, err)
	}
	bad := ControllerRolloutReceipt{Expectation: expectation, Success: true, BuildArtifact: "image", ControllerRevision: receipt.ReflectionCommit, ObservedDetectorFingerprint: "wrong", VerifiedAt: time.Now()}
	if err := native.RecordControllerRollout(bad); err == nil {
		t.Fatal("mismatched detector revision accepted")
	}
	good := bad
	good.ObservedDetectorFingerprint = expectation.AfterDetectorFingerprint
	if err := native.RecordControllerRollout(good); err != nil {
		t.Fatal(err)
	}
	if _, pending, err := native.PendingControllerRollout(receipt.ReflectionCommit); err != nil || pending {
		t.Fatalf("rollout remains pending: %v, %v", pending, err)
	}
	if handoffs, err := recovered.PendingControllerRollouts(); err != nil || len(handoffs) != 0 {
		t.Fatalf("completed rollout still requires handoff: %+v, %v", handoffs, err)
	}
	if err := native.AcknowledgeClosure(context.Background(), receipt); err != nil {
		t.Fatal(err)
	}
}

func nativeDetectorManifest(class, id, incident string) string {
	owner, origin := "health_judge", ""
	if class == "incident" {
		owner, origin = "responder", "originatingIncident: "+incident+"\n"
	}
	return "apiVersion: sdo.dev/v1alpha1\nkind: DetectorManifest\nsdkVersion: v0.1\nid: " + id + "\nconstructor: New\nclass: " + class + "\nowner: " + owner + "\nwatches:\n  - apiVersion: v1\n    kind: Pod\ninterval: 30s\npersistence:\n  firing: 1\n  clearing: 1\nbatching:\n  severity: critical\n  debounce: 500ms\npossiblePlaybooks:\n  - base\n" + origin + "originatingCommit: initial\n"
}
