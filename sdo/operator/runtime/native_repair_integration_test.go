//go:build integration

package runtime

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/patchset"
	"sdo.dev/operator/telemetry"
)

type recordedRepairer struct {
	calls  atomic.Int32
	result SourceRepair
	err    error
	// publish makes the fake accept the repair the way the deployed path
	// does, so the closure has a real accepted commit to derive from.
	publish func(context.Context, string) (string, error)
}

func (r *recordedRepairer) Repair(ctx context.Context, id string) (SourceRepair, error) {
	r.calls.Add(1)
	result := r.result
	if r.publish != nil {
		commit, err := r.publish(ctx, id)
		if err != nil {
			return SourceRepair{}, err
		}
		result.Commit = commit
	}
	return result, r.err
}

type repairClosureWorld struct {
	accepted  string
	repo      *broker.Broker
	native    NativeIncidentBroker
	workspace IncidentWorkspace
}

func repairGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	out, err := exec.Command("git", append([]string{"-C", dir}, args...)...).CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

func copyGoodMemory(t *testing.T, to string) {
	t.Helper()
	good := filepath.Join("..", "memory", "validate", "testdata", "good", ".sdo")
	err := filepath.WalkDir(good, func(path string, entry os.DirEntry, err error) error {
		rel, relErr := filepath.Rel(good, path)
		if err != nil || relErr != nil || entry.IsDir() || strings.HasPrefix(rel, "diagnostics") {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		target := filepath.Join(to, ".sdo", rel)
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		mode := os.FileMode(0o644)
		if strings.HasSuffix(rel, ".sh") {
			mode = 0o755
		}
		return os.WriteFile(target, data, mode)
	})
	if err != nil {
		t.Fatal(err)
	}
}

// newRepairClosureWorld prepares an incident whose responder kept a source
// repair: by committing app.txt (committed) or by capturing a deployment patch.
func newRepairClosureWorld(t *testing.T, repairs SourceRepairer, keep string) *repairClosureWorld {
	t.Helper()
	root := t.TempDir()
	w := &repairClosureWorld{accepted: filepath.Join(root, "accepted")}
	if err := os.MkdirAll(w.accepted, 0o755); err != nil {
		t.Fatal(err)
	}
	repairGit(t, w.accepted, "init", "-q")
	repairGit(t, w.accepted, "config", "user.name", "Synthetic Test")
	repairGit(t, w.accepted, "config", "user.email", "test@example.invalid")
	copyGoodMemory(t, w.accepted)
	if err := os.WriteFile(filepath.Join(w.accepted, "app.txt"), []byte("synthetic app\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	repairGit(t, w.accepted, "add", ".")
	repairGit(t, w.accepted, "commit", "-qm", "synthetic base")
	var err error
	if w.repo, err = broker.New(w.accepted, filepath.Join(root, "proposals"), func(context.Context, string, []string) error { return nil }, broker.ObservationPolicy{SettingsDigest: "settings-digest", Telemetry: broker.Telemetry{Registry: telemetry.NewRegistry()}}); err != nil {
		t.Fatal(err)
	}
	w.native = NativeIncidentBroker{
		Repository: w.repo, LedgerRoot: filepath.Join(root, "ledger"), Deriver: ProtoOutcomeDeriver{},
		ResponderBackend: "test", ResponderModel: "test", Repairs: repairs,
		Reflector: ReflectionWorkerFunc(func(context.Context, IncidentClosure, IncidentWorkspace, *contracts.OutcomeRecord, string, string, ReflectionFeedback) (ReflectionDecision, error) {
			return ReflectionDecision{LearningDecision: "no_change", NoChangeReason: "synthetic"}, nil
		}),
	}
	ctx := context.Background()
	if w.workspace, err = w.native.PrepareIncident(ctx, "incident-1"); err != nil {
		t.Fatal(err)
	}
	checkout := w.workspace.Worktree
	switch keep {
	case "committed":
		if err := os.WriteFile(filepath.Join(checkout, "app.txt"), []byte("synthetic app, repaired\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		repairGit(t, checkout, "config", "user.name", "Synthetic Agent")
		repairGit(t, checkout, "config", "user.email", "agent@example.invalid")
		repairGit(t, checkout, "add", "app.txt")
		repairGit(t, checkout, "commit", "-qm", "repair the synthetic application")
	case "patch":
		if err := os.WriteFile(filepath.Join(checkout, "app.txt"), []byte("synthetic app, repaired\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := patchset.Create(ctx, checkout, patchset.CreateRequest{Owner: patchset.OwnerResponder, Incident: "incident-1", Reason: "Repair the synthetic application the deployment ships with this change"}); err != nil {
			t.Fatal(err)
		}
	case "scratch":
		if err := os.WriteFile(filepath.Join(checkout, "app.txt"), []byte("synthetic app, edited and left\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("unknown way of keeping a repair %q", keep)
	}
	return w
}

func (w *repairClosureWorld) closure(policy string) IncidentClosure {
	closure := nativeClosure(w.workspace)
	closure.Request.RepairPolicy = policy
	return closure
}

func (w *repairClosureWorld) head(t *testing.T) string {
	return repairGit(t, w.accepted, "rev-parse", "HEAD")
}

func (w *repairClosureWorld) publishLikeTheDeployedPath(ctx context.Context, id string) (string, error) {
	receipt, err := w.repo.Publish(ctx, id, broker.Responder, broker.ProposalPhase)
	return receipt.Commit, err
}

// closeAndPublish closes the incident, acknowledges it and runs its queued
// dispatch once, the way the controller does.
func (w *repairClosureWorld) closeAndPublish(t *testing.T, policy string) (ClosureReceipt, error) {
	t.Helper()
	ctx := context.Background()
	receipt, err := w.native.ProcessClosure(ctx, w.closure(policy))
	if err != nil {
		t.Fatalf("closure: %v", err)
	}
	if err := w.native.AcknowledgeClosure(ctx, receipt); err != nil {
		t.Fatal(err)
	}
	return receipt, w.native.ProcessQueuedReflection(ctx, "incident-1")
}

func (w *repairClosureWorld) sourceRepairOfTheOutcome(t *testing.T) *contracts.SourceRepair {
	t.Helper()
	out := repairGit(t, w.accepted, "show", "HEAD:"+layout.OutcomePath("incident-1"))
	record := &contracts.OutcomeRecord{}
	if err := wire.UnmarshalDurable([]byte(out), record); err != nil {
		t.Fatal(err)
	}
	return record.GetSourceRepair()
}

// The closure never deploys a source repair: it acknowledges with the outcome
// and records that a repair is owed. The queued dispatch after the
// acknowledgment hands it to the configured repairer, which owns deployment.
func TestClosureOwesASourceRepairThatIsPublishedAfterTheAcknowledgment(t *testing.T) {
	for _, keep := range []string{"committed", "patch"} {
		t.Run(keep, func(t *testing.T) {
			repairs := &recordedRepairer{result: SourceRepair{Deployed: true}}
			w := newRepairClosureWorld(t, repairs, keep)
			repairs.publish = w.publishLikeTheDeployedPath
			before := w.head(t)
			receipt, err := w.closeAndPublish(t, "commit")
			if err != nil {
				t.Fatal(err)
			}
			if receipt.ProposalCommit != "" || receipt.OutcomeCommit == "" {
				t.Fatalf("the closure receipt = %+v, want an outcome and no repair commit yet", receipt)
			}
			if repairs.calls.Load() != 1 {
				t.Fatalf("the repairer ran %d times, want once, after the acknowledgment", repairs.calls.Load())
			}
			ledger, err := w.native.readLedger("incident-1")
			if err != nil {
				t.Fatal(err)
			}
			phases := ledger.GetClosurePhaseTimestamps()
			for _, want := range []string{"source_repair_pending", "source_repair_deployed"} {
				if _, ok := phases[want]; !ok {
					t.Fatalf("%s not recorded: %v", want, phases)
				}
			}
			if ledger.GetSourceRepair() != "deployed" || ledger.GetProposalCommit() == "" || ledger.GetProposalCommit() == before {
				t.Fatalf("ledger source repair %q, proposal commit %q", ledger.GetSourceRepair(), ledger.GetProposalCommit())
			}
			if got := w.sourceRepairOfTheOutcome(t); got.GetStatus() != "deployed" || got.GetCommit() != ledger.GetProposalCommit() {
				t.Fatalf("outcome source_repair = %v", got)
			}
			// Done is done: another dispatch does nothing.
			if err := w.native.ProcessQueuedReflection(context.Background(), "incident-1"); err != nil || repairs.calls.Load() != 1 {
				t.Fatalf("second dispatch: %v, repairer calls %d", err, repairs.calls.Load())
			}
		})
	}
}

// A repair the Deployer or the detectors refused is never accepted, and never
// costs the incident its outcome, whatever the repair policy. The hold is
// final: no later dispatch deploys it again.
func TestAHeldSourceRepairIsRecordedAndNeverFailsTheClosure(t *testing.T) {
	for _, policy := range []string{"recorded-actions", "commit"} {
		t.Run(policy, func(t *testing.T) {
			repairs := &recordedRepairer{result: SourceRepair{Held: "rollout: deployment never became ready"}}
			w := newRepairClosureWorld(t, repairs, "committed")
			before := w.head(t)
			receipt, err := w.closeAndPublish(t, policy)
			if err != nil || receipt.OutcomeCommit == "" {
				t.Fatalf("receipt = %+v, err = %v, want a closed incident with an outcome", receipt, err)
			}
			if strings.Contains(repairGit(t, w.accepted, "log", "--format=%B", before+"..HEAD"), "SDO-Phase: proposal") {
				t.Fatal("a held repair reached the accepted branch")
			}
			ledger, _ := w.native.readLedger("incident-1")
			if _, ok := ledger.GetClosurePhaseTimestamps()["source_repair_held"]; !ok || ledger.GetSourceRepair() != "held" || repairs.calls.Load() != 1 {
				t.Fatalf("phases %v, source repair %q, repairer calls %d", ledger.GetClosurePhaseTimestamps(), ledger.GetSourceRepair(), repairs.calls.Load())
			}
			if got := w.sourceRepairOfTheOutcome(t); got.GetStatus() != "held" || !strings.Contains(got.GetDetail(), "never became ready") {
				t.Fatalf("outcome source_repair = %v", got)
			}
			if err := w.native.ProcessQueuedReflection(context.Background(), "incident-1"); err != nil || repairs.calls.Load() != 1 {
				t.Fatalf("a later dispatch deployed a held repair again: %v, calls %d", err, repairs.calls.Load())
			}
		})
	}
}

// A deploy that cannot start (an incident is open, or the controller did not
// pause) is neither a hold nor a failure: the repair stays pending and the next
// dispatch publishes it.
func TestADeferredSourceRepairStaysPendingUntilItCanDeploy(t *testing.T) {
	repairs := &recordedRepairer{result: SourceRepair{Deferred: "an incident is open"}}
	w := newRepairClosureWorld(t, repairs, "committed")
	_, err := w.closeAndPublish(t, "commit")
	var deferred *SourceDeferredError
	if !errors.As(err, &deferred) {
		t.Fatalf("err = %v, want a deferral", err)
	}
	ledger, _ := w.native.readLedger("incident-1")
	if ledger.GetSourceRepair() != "pending" {
		t.Fatalf("source repair = %q, want pending", ledger.GetSourceRepair())
	}
	queued, err := w.native.QueuedReflections()
	if err != nil || len(queued) != 1 {
		t.Fatalf("queued = %v, %v: a deferred repair must stay in the queue", queued, err)
	}
	repairs.result = SourceRepair{Deployed: true}
	repairs.publish = w.publishLikeTheDeployedPath
	if err := w.native.ProcessQueuedReflection(context.Background(), "incident-1"); err != nil {
		t.Fatal(err)
	}
	if ledger, _ = w.native.readLedger("incident-1"); ledger.GetSourceRepair() != "deployed" {
		t.Fatalf("source repair = %q after the retry", ledger.GetSourceRepair())
	}
}

// Without a repairer (shell detectors or frozen memory) the repair is still
// accepted, but nothing deploys it: its own phase, never a deployment.
func TestSourceRepairWithoutARepairerIsPublishedUndeployed(t *testing.T) {
	w := newRepairClosureWorld(t, nil, "committed")
	w.native.Repairs = PlainPublication{Repository: w.repo, Reason: PublishesUndeployedShellDetectors}
	if _, err := w.closeAndPublish(t, "commit"); err != nil {
		t.Fatal(err)
	}
	ledger, _ := w.native.readLedger("incident-1")
	phases := ledger.GetClosurePhaseTimestamps()
	if _, ok := phases["source_repair_deployed"]; ok {
		t.Fatal("a plain publication claimed a deployment")
	}
	if _, ok := phases["source_repair_published_undeployed"]; !ok || ledger.GetSourceRepair() != "published" || ledger.GetProposalCommit() == "" {
		t.Fatalf("phases %v, source repair %q, proposal commit %q", phases, ledger.GetSourceRepair(), ledger.GetProposalCommit())
	}
}

// A configuration with no repairer can never publish committed source plainly:
// the missing repairer is a permanent condition, so the repair is held once.
func TestSourceRepairWithoutAnyRepairerIsHeld(t *testing.T) {
	w := newRepairClosureWorld(t, nil, "committed")
	before := w.head(t)
	if _, err := w.closeAndPublish(t, "recorded-actions"); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(repairGit(t, w.accepted, "log", "--format=%B", before+"..HEAD"), "SDO-Phase: proposal") {
		t.Fatal("the repair was accepted without a repairer")
	}
	ledger, _ := w.native.readLedger("incident-1")
	if ledger.GetSourceRepair() != "held" || !strings.Contains(ledger.GetSourceDetail(), "repairer_required") {
		t.Fatalf("source repair %q, detail %q", ledger.GetSourceRepair(), ledger.GetSourceDetail())
	}
}

// App files the agent left uncommitted are discarded, and said so. The
// outcome is committed first, the repairer never runs, nothing is accepted.
func TestUncommittedScratchIsDiscardedAndRecorded(t *testing.T) {
	for _, policy := range []string{"recorded-actions", "commit"} {
		t.Run(policy, func(t *testing.T) {
			repairs := &recordedRepairer{result: SourceRepair{Deployed: true}}
			w := newRepairClosureWorld(t, repairs, "scratch")
			before := w.head(t)
			receipt, err := w.closeAndPublish(t, policy)
			if err != nil || receipt.OutcomeCommit == "" || receipt.ProposalCommit != "" {
				t.Fatalf("receipt = %+v, err = %v, want an outcome and no published repair", receipt, err)
			}
			if repairs.calls.Load() != 0 {
				t.Fatalf("the repairer ran %d times on scratch", repairs.calls.Load())
			}
			if strings.Contains(repairGit(t, w.accepted, "log", "--format=%B", before+"..HEAD"), "SDO-Phase: proposal") {
				t.Fatal("scratch reached the accepted branch")
			}
			ledger, _ := w.native.readLedger("incident-1")
			if _, ok := ledger.GetClosurePhaseTimestamps()["source_repair_uncaptured"]; !ok || ledger.GetSourceRepair() != "none" {
				t.Fatalf("phases %v, source repair %q", ledger.GetClosurePhaseTimestamps(), ledger.GetSourceRepair())
			}
			if got := w.sourceRepairOfTheOutcome(t); got.GetStatus() != "none" || len(got.GetUncapturedPaths()) != 1 || got.GetUncapturedPaths()[0] != "app.txt" {
				t.Fatalf("outcome source_repair = %v", got)
			}
		})
	}
}

// A repair that fails for a reason that may pass (a registry that was
// unreachable) is retried by the queue, not recorded as a hold.
func TestAFailingRepairerIsRetried(t *testing.T) {
	repairs := &recordedRepairer{err: errors.New("registry unreachable")}
	w := newRepairClosureWorld(t, repairs, "committed")
	_, err := w.closeAndPublish(t, "commit")
	if err == nil || !strings.Contains(err.Error(), "registry unreachable") {
		t.Fatalf("err = %v", err)
	}
	if err := w.native.ProcessQueuedReflection(context.Background(), "incident-1"); err == nil || repairs.calls.Load() != 2 {
		t.Fatalf("second dispatch: %v, repairer calls %d", err, repairs.calls.Load())
	}
	if ledger, _ := w.native.readLedger("incident-1"); ledger.GetSourceRepair() != "pending" {
		t.Fatalf("source repair = %q, want pending", ledger.GetSourceRepair())
	}
}

// A source change that conflicts with the branch head is held as
// source_conflict, never retried, and its commits are kept for a person.
func TestASourceConflictIsHeldAndKeptNotRetried(t *testing.T) {
	conflict := &broker.MemoryConflict{Conflict: &contracts.MemoryConflict{ConflictingPaths: []string{"app.txt"}}}
	repairs := &recordedRepairer{err: conflict}
	w := newRepairClosureWorld(t, repairs, "committed")
	if _, err := w.closeAndPublish(t, "recorded-actions"); err != nil {
		t.Fatalf("a conflict must not fail the publication: %v", err)
	}
	ledger, _ := w.native.readLedger("incident-1")
	if ledger.GetSourceRepair() != "held" || !strings.Contains(ledger.GetSourceDetail(), "source_conflict: app.txt") {
		t.Fatalf("source repair %q, detail %q", ledger.GetSourceRepair(), ledger.GetSourceDetail())
	}
	if repairGit(t, w.accepted, "rev-parse", "--verify", "refs/sdo/held/incident-1") == "" {
		t.Fatal("the conflicting source was not kept")
	}
	if err := w.native.ProcessQueuedReflection(context.Background(), "incident-1"); err != nil || repairs.calls.Load() != 1 {
		t.Fatalf("a later dispatch retried a conflict: %v, calls %d", err, repairs.calls.Load())
	}
}
