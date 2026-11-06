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
	"sdo.dev/operator/patchset"
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

// newRepairClosureWorld prepares an incident whose responder captured a patch
// of the application as its source repair.
func newRepairClosureWorld(t *testing.T, policy string, repairs SourceRepairer) *repairClosureWorld {
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
	if w.repo, err = broker.New(w.accepted, filepath.Join(root, "proposals"), func(context.Context, string, []string) error { return nil }); err != nil {
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
	if err := os.WriteFile(filepath.Join(w.workspace.Worktree, "app.txt"), []byte("synthetic app, repaired\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := patchset.Create(ctx, w.workspace.Worktree, patchset.CreateRequest{Owner: patchset.OwnerResponder, Incident: "incident-1", Reason: "Repair the synthetic application the deployment ships with this change"}); err != nil {
		t.Fatal(err)
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

// The responder's accepted source repair goes through the configured repairer,
// which owns deployment: the closure itself never publishes it.
func TestClosureAcceptsASourceRepairOnlyThroughTheRepairer(t *testing.T) {
	repairs := &recordedRepairer{result: SourceRepair{Deployed: true}}
	w := newRepairClosureWorld(t, "commit", repairs)
	repairs.publish = w.publishLikeTheDeployedPath
	before := w.head(t)
	receipt, err := w.native.ProcessClosure(context.Background(), w.closure("commit"))
	if err != nil {
		t.Fatal(err)
	}
	if repairs.calls.Load() != 1 {
		t.Fatalf("the repairer ran %d times, want once", repairs.calls.Load())
	}
	if receipt.ProposalCommit == "" || receipt.ProposalCommit == before {
		t.Fatalf("proposal commit = %q", receipt.ProposalCommit)
	}
	ledger, err := w.native.readLedger("incident-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := ledger.GetClosurePhaseTimestamps()["source_repair_deployed"]; !ok {
		t.Fatalf("source_repair_deployed not recorded: %v", ledger.GetClosurePhaseTimestamps())
	}
}

// A repair the Deployer or the detectors refused is not accepted. Under
// recorded-actions the closure records no repair; under commit it fails, and a
// retry never deploys again.
func TestClosureDoesNotAcceptAHeldSourceRepair(t *testing.T) {
	ctx := context.Background()
	t.Run("recorded actions", func(t *testing.T) {
		repairs := &recordedRepairer{result: SourceRepair{Held: "rollout: deployment never became ready"}}
		w := newRepairClosureWorld(t, "recorded-actions", repairs)
		before := w.head(t)
		receipt, err := w.native.ProcessClosure(ctx, w.closure("recorded-actions"))
		if err != nil || receipt.ProposalCommit != "" {
			t.Fatalf("receipt = %+v, err = %v, want a closure with no accepted repair", receipt, err)
		}
		if w.head(t) != before && strings.Contains(repairGit(t, w.accepted, "log", "--format=%B", before+"..HEAD"), "SDO-Phase: proposal") {
			t.Fatal("a held repair reached the accepted branch")
		}
		ledger, _ := w.native.readLedger("incident-1")
		if _, ok := ledger.GetClosurePhaseTimestamps()["source_repair_held"]; !ok || repairs.calls.Load() != 1 {
			t.Fatalf("phases %v, repairer calls %d", ledger.GetClosurePhaseTimestamps(), repairs.calls.Load())
		}
	})
	t.Run("commit policy", func(t *testing.T) {
		repairs := &recordedRepairer{result: SourceRepair{Held: "health: detectors fire"}}
		w := newRepairClosureWorld(t, "commit", repairs)
		closure := w.closure("commit")
		for attempt := 1; attempt <= 2; attempt++ {
			_, err := w.native.ProcessClosure(ctx, closure)
			if err == nil || !strings.Contains(err.Error(), "not deployed") {
				t.Fatalf("attempt %d: err = %v, want the commit policy to fail the closure", attempt, err)
			}
		}
		if repairs.calls.Load() != 1 {
			t.Fatalf("the repairer ran %d times across retries, want once", repairs.calls.Load())
		}
	})
	t.Run("error is retried", func(t *testing.T) {
		repairs := &recordedRepairer{err: errors.New("registry unreachable")}
		w := newRepairClosureWorld(t, "commit", repairs)
		closure := w.closure("commit")
		for attempt := 1; attempt <= 2; attempt++ {
			if _, err := w.native.ProcessClosure(ctx, closure); err == nil {
				t.Fatalf("attempt %d: a failing repairer closed the incident", attempt)
			}
		}
		if repairs.calls.Load() != 2 {
			t.Fatalf("the repairer ran %d times, want one per attempt", repairs.calls.Load())
		}
	})
}

// Without a repairer (shell detectors or frozen memory) the closure still
// accepts a repair of deployment inputs, but nothing deploys it: the ledger
// records that as its own phase, never as a deployment.
func TestClosureWithoutARepairerRecordsTheRepairAsUndeployed(t *testing.T) {
	w := newRepairClosureWorld(t, "commit", nil)
	receipt, err := w.native.ProcessClosure(context.Background(), w.closure("commit"))
	if err != nil || receipt.ProposalCommit == "" {
		t.Fatalf("receipt = %+v, err = %v", receipt, err)
	}
	ledger, _ := w.native.readLedger("incident-1")
	phases := ledger.GetClosurePhaseTimestamps()
	if _, ok := phases["source_repair_deployed"]; ok {
		t.Fatal("a plain publication claimed a deployment")
	}
	if _, ok := phases["source_repair_published_undeployed"]; !ok {
		t.Fatalf("phases %v lack source_repair_published_undeployed", phases)
	}
}
