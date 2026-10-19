package runtime

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
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
	out, err := exec.Command("git", append([]string{"-C", dir}, args...)...).CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v: %s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

func nativeRepository(t *testing.T) string {
	t.Helper()
	root := filepath.Join(t.TempDir(), "accepted")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	nativeGit(t, root, "init", "-q")
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
		DetectedAt: now, DispatchedAt: now, ResponderCompletedAt: now.Add(time.Second), VerifiedAt: now.Add(time.Second),
	}
}

func TestNativeBrokerRecoversCommittedPhasesAndRequiresReflection(t *testing.T) {
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
	native.Reflector = ReflectionWorkerFunc(func(_ context.Context, _ IncidentClosure, reflection IncidentWorkspace, _ *contracts.OutcomeRecord, _, _ string) (ReflectionDecision, error) {
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

func TestNativeBrokerRejectsReflectionAndRecreatesItsCheckout(t *testing.T) {
	target := nativeRepository(t)
	nativeMemoryFixture(t, target)
	repositoryBroker, err := broker.New(target, filepath.Join(t.TempDir(), "proposals"), func(_ context.Context, _ string, _ []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	invalid := true
	native := NativeIncidentBroker{
		Repository: repositoryBroker, LedgerRoot: filepath.Join(t.TempDir(), "ledger"), ResponderBackend: "test", ResponderModel: "test",
		Deriver: OutcomeDeriverFunc(func(_ context.Context, c IncidentClosure, _, _, _ string) (DerivedOutcome, error) {
			return testDerivedOutcome(c.Request.IncidentID, "success", true), nil
		}),
		Reflector: ReflectionWorkerFunc(func(_ context.Context, _ IncidentClosure, w IncidentWorkspace, _ *contracts.OutcomeRecord, _, _ string) (ReflectionDecision, error) {
			if !invalid {
				return ReflectionDecision{LearningDecision: "no_change", NoChangeReason: "already learned"}, nil
			}
			return ReflectionDecision{LearningDecision: "updated"}, os.WriteFile(filepath.Join(w.Worktree, ".sdo", "playbooks", "base", "README.md"), []byte("bad playbook\n"), 0o644)
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
	if _, err := native.ProcessClosure(context.Background(), closure); err == nil {
		t.Fatal("invalid reflection accepted")
	}
	if _, err := repositoryBroker.ProposalInfo("inc-reject", broker.ReflectionPhase); err == nil {
		t.Fatal("rejected reflection checkout remained")
	}
	invalid = false
	if _, err := native.ProcessClosure(context.Background(), closure); err != nil {
		t.Fatal(err)
	}
	if got := nativeGit(t, target, "show", "HEAD:.sdo/playbooks/base/README.md"); strings.Contains(got, "bad playbook") {
		t.Fatal("rejected reflection reached accepted state")
	}
}

func TestNativeBrokerReleasesSupersededProposal(t *testing.T) {
	target := nativeRepository(t)
	repositoryBroker, err := broker.New(target, filepath.Join(t.TempDir(), "proposals"), nil)
	if err != nil {
		t.Fatal(err)
	}
	native := NativeIncidentBroker{Repository: repositoryBroker, LedgerRoot: filepath.Join(t.TempDir(), "ledger")}
	w, err := native.PrepareIncident(context.Background(), "inc-superseded")
	if err != nil {
		t.Fatal(err)
	}
	if err := native.ReleaseIncident(context.Background(), "inc-superseded"); err != nil {
		t.Fatal(err)
	}
	if err := native.ReleaseIncident(context.Background(), "inc-superseded"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(w.Worktree); !os.IsNotExist(err) {
		t.Fatalf("superseded checkout remains: %v", err)
	}
}

func TestNativeBrokerRecordedActionsDoNotCreateEmptyProposalCommit(t *testing.T) {
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
	target := nativeRepository(t)
	nativeMemoryFixture(t, target)
	repositoryBroker, err := broker.New(target, filepath.Join(t.TempDir(), "proposals"), func(_ context.Context, _ string, _ []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	native := NativeIncidentBroker{
		Repository: repositoryBroker, LedgerRoot: filepath.Join(t.TempDir(), "ledger"), ResponderBackend: "test", ResponderModel: "test",
		Deriver: OutcomeDeriverFunc(func(_ context.Context, closure IncidentClosure, _, _, _ string) (DerivedOutcome, error) {
			return testDerivedOutcome(closure.Request.IncidentID, "success", true), nil
		}),
		Reflector: ReflectionWorkerFunc(func(_ context.Context, _ IncidentClosure, workspace IncidentWorkspace, _ *contracts.OutcomeRecord, _, _ string) (ReflectionDecision, error) {
			manifest := filepath.Join(workspace.Worktree, ".sdo", "diagnostics", "detectors", "incidents", "learned", "manifest.yaml")
			if err := os.MkdirAll(filepath.Dir(manifest), 0o755); err != nil {
				return ReflectionDecision{}, err
			}
			if err := os.WriteFile(manifest, []byte(nativeDetectorManifest("incident", "learned", "inc-rollout")), 0o644); err != nil {
				return ReflectionDecision{}, err
			}
			path := filepath.Join(workspace.Worktree, ".sdo", "diagnostics", "detectors", "incidents", "learned", "detector.go")
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				return ReflectionDecision{}, err
			}
			return ReflectionDecision{LearningDecision: "updated"}, os.WriteFile(path, []byte("package learned\n"), 0o644)
		}),
	}
	workspace, err := native.PrepareIncident(context.Background(), "inc-rollout")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace.Worktree, "app.txt"), []byte("repair\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	receipt, err := native.ProcessClosure(context.Background(), nativeClosure(workspace))
	if err != nil {
		t.Fatal(err)
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
	bad := ControllerRolloutReceipt{Expectation: expectation, Success: true, BuildArtifact: "image", ControllerRevision: "rev", ObservedDetectorFingerprint: "wrong", VerifiedAt: time.Now()}
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
