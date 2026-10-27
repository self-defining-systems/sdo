package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sdo.dev/operator/internal/testutil"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

type restartClassResponder struct {
	t      *testing.T
	health *chaosDetector
	start  time.Time
	world  restartClassWorld
}

func (r restartClassResponder) Dispatch(_ context.Context, request IncidentRequest) (IncidentResult, error) {
	if r.world.Edit != nil {
		if err := r.world.Edit(r.t, request.RepositoryWorktree); err != nil {
			return IncidentResult{}, err
		}
	} else {
		tracingWrite(r.t, request.RepositoryWorktree, "README.md", "accepted source restoration annotation\n")
	}
	r.health.active.set(false)
	result := completedResult(request.IncidentID)
	result.Timing = TimingMetrics{StartedAt: r.start.Add(2 * time.Second), CompletedAt: r.start.Add(3 * time.Second)}
	result.RepairActions = []RepairActionReceipt{{ActionID: "scripted-restore", Kind: "source", Target: "Deployment/jaeger", Summary: "restore source", Details: "hermetic scripted restoration", StartedAt: result.Timing.StartedAt, CompletedAt: result.Timing.CompletedAt, Success: true}}
	return result, nil
}

func restartClassSnapshot() sdktest.Snapshot {
	return sdktest.Snapshot{NamespaceName: "demo", ConfigMapList: []corev1.ConfigMap{{ObjectMeta: metav1.ObjectMeta{Name: "representative-namespace", Namespace: "demo"}, Data: map[string]string{"synthetic-asset": strings.Repeat("x", (1<<20)-(32<<10))}}}}
}

// Start with real detection, broker workspace preparation, dispatch, scripted
// source repair and independent controller clearing, rather than manufacturing
// a frozen closure. The process sweep begins at its durable public effect.
func restartClassIncident(t *testing.T, ctx context.Context, world restartClassWorld, root, base, path string, native NativeIncidentBroker) *Controller {
	t.Helper()
	start := time.Unix(1000, 0)
	now := start
	health := newChaosHealthDetector("health")
	health.spec.Persistence = sdk.PersistencePolicy{Firing: 1, Clearing: 1}
	health.active.set(true)
	cfg := testControllerConfig()
	cfg.SourceCommit, cfg.DeployedCommit = base, base
	cfg.Namespace = world.Namespace
	cfg.RepositoryWorktree = root
	cfg.BatchDebounce = 0
	c, err := NewController(cfg, []sdk.Detector{health}, staticProvider{snapshot: restartClassSnapshot()}, restartClassResponder{t: t, health: health, start: start, world: world}, start)
	if err != nil {
		t.Fatal(err)
	}
	c.now = func() time.Time { return now }
	if err := c.SetIncidentBroker(native); err != nil {
		t.Fatal(err)
	}
	if err := c.AttachStateStore(ctx, crashStateStore{path: path}); err != nil {
		t.Fatal(err)
	}
	for tick := 0; tick < 30; tick++ {
		now = start.Add(time.Duration(tick) * time.Second)
		if err := c.Step(ctx, now, nil); err != nil {
			t.Fatal(err)
		}
		if err := c.PersistState(ctx); err != nil {
			t.Fatal(err)
		}
		if effect, ok := c.PendingWorkspaceEffect(); ok {
			if err := c.ExecuteWorkspaceEffect(ctx, effect); err != nil {
				t.Fatal(err)
			}
			awaitBrokerResult(t, c.workspaceResults)
			c.processBrokerCompletions()
			if err := c.PersistState(ctx); err != nil {
				t.Fatal(err)
			}
		}
		if effect, ok := c.PendingDispatchEffect(); ok {
			if err := c.ExecuteDispatchEffect(ctx, effect); err != nil {
				t.Fatal(err)
			}
			awaitDispatchCompletionQueued(t, c)
		}
		if _, ok := c.PendingClosureEffect(); ok {
			return c
		}
	}
	t.Fatal("scripted incident did not reach a durable closure within 30 steps")
	return nil
}

// RT1 deliberately spans the native transaction, controller persistence, and
// rendered publication. Every cut is a real SIGKILL; no in-memory broker or
// receipt survives it. Synthetic inputs are independent of benchmark verdicts.
var restartClassCuts = []string{
	"after-ack-before-publication", "before-effect", "after-effect-before-receipt",
	"after-receipt-before-ack", "after-publication-before-teardown", "during-adoption",
}

type restartClassBroker struct {
	NativeIncidentBroker
	path, cut string
}

func (b restartClassBroker) ProcessClosure(ctx context.Context, closure IncidentClosure) (ClosureReceipt, error) {
	receipt, err := b.NativeIncidentBroker.ProcessClosure(ctx, closure)
	if err == nil {
		crashAt(b.path, b.cut, "after-effect-before-receipt")
	}
	return receipt, err
}

func restartClassNative(t *testing.T, root, proposals, ledger string) NativeIncidentBroker {
	t.Helper()
	authority, err := broker.New(root, proposals, func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	return NativeIncidentBroker{Repository: authority, LedgerRoot: ledger, ResponderBackend: "scripted", ResponderModel: "none",
		Deriver: ProtoOutcomeDeriver{}, Reflector: ReflectionWorkerFunc(func(context.Context, IncidentClosure, IncidentWorkspace, *contracts.OutcomeRecord, string, string, ReflectionFeedback) (ReflectionDecision, error) {
			return ReflectionDecision{LearningDecision: "no_change", NoChangeReason: "scripted repair already represented"}, nil
		})}
}

// TestRestartClassProcess is the disposable controller process. File state and
// Git are the only authorities shared with its successor.
func TestRestartClassProcess(t *testing.T) {
	root := os.Getenv("RT1_ROOT")
	if root == "" {
		t.Skip("subprocess only")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	path, cut := os.Getenv("RT1_STATE"), os.Getenv("RT1_CUT")
	proposals, ledger := os.Getenv("RT1_PROPOSALS"), os.Getenv("RT1_LEDGER")
	world := restartClassWorldFromEnv(t, root)
	native := restartClassNative(t, root, proposals, ledger)
	coordinator := world.Coordinator(t, root, proposals, path+".judge")
	revision, err := memory.AcceptedHealthRevision(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	if revision.NeedsRefresh {
		if _, err := coordinator.RefreshHealth(ctx, lifecycle.Config{Application: world.Application, Namespace: world.Namespace}, revision); err != nil {
			t.Fatal(err)
		}
		revision, err = memory.AcceptedHealthRevision(ctx, root)
		if err != nil {
			t.Fatal(err)
		}
	}
	cfg := testControllerConfig()
	cfg.SourceCommit = revision.Commit
	cfg.Namespace = world.Namespace
	snapshot := restartClassSnapshot()
	if os.Getenv("RT1_CHANGED_GENERATION") == "1" {
		snapshot.Generation = "adopted-generation"
	}
	c, err := NewController(cfg, []sdk.Detector{controllerDetector("health", time.Second, sdk.Finding{})}, staticProvider{snapshot: snapshot}, &recordingDispatcher{}, time.Unix(1000, 0))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.AttachStateStore(ctx, crashStateStore{path: path}); err != nil {
		t.Fatal(err)
	}
	if err := c.SetIncidentBroker(restartClassBroker{NativeIncidentBroker: native, path: path, cut: cut}); err != nil {
		t.Fatal(err)
	}
	rv6Guard(c, root, revision.Commit)
	c.discardRestoredHealthObservations(time.Unix(1000, 0))
	crashAt(path, cut, "during-adoption")
	crashAt(path, cut, "before-effect")
	if effect, ok := c.PendingClosureEffect(); ok {
		if err := c.ExecuteClosureEffect(ctx, effect); err != nil {
			t.Fatalf("closure replay: %v", err)
		}
		awaitBrokerResult(t, c.closureResults)
		c.processBrokerCompletions()
		// A source repair invalidates the compiled health binding before its receipt
		// can be saved. Persist on the next process, after the real judge adoption.
		if err := c.PersistState(ctx); err != nil {
			if !errors.Is(err, memory.ErrHealthRefreshRequired) {
				t.Fatal(err)
			}
			if cut == "after-effect-before-receipt" {
				t.Fatal("cut not reached")
			}
			// Ask the parent to launch a new process through durable state, as
			// the supervisor does. The parent permits at most two adoptions.
			os.Exit(75)
		}
	}
	crashAt(path, cut, "after-receipt-before-ack")
	if effect, ok := c.PendingClosureAcknowledgmentEffect(); ok {
		if err := c.ExecuteClosureAcknowledgmentEffect(ctx, effect); err != nil {
			t.Fatalf("ACK replay: %v", err)
		}
		awaitBrokerResult(t, c.acknowledgmentResults)
		c.processBrokerCompletions()
		if err := c.PersistState(ctx); err != nil {
			t.Fatal(err)
		}
	}
	id := os.Getenv("RT1_INCIDENT")
	restartClassWriteReceipt(t, native, path+".ack-receipt", id)
	crashAt(path, cut, "after-ack-before-publication")
	source := GitPlanSource{Repository: root, ManifestPaths: world.ManifestPaths}
	client := fake.NewSimpleClientset(world.Live(t, root, os.Getenv("RT1_BASE"))...)
	c.ReconcileRendered = func(ctx context.Context, revision string) (string, error) {
		started := time.Now()
		revision, err := (RenderedLockReconciler{Source: source, Client: client, Broker: native.Repository}).Reconcile(ctx, revision, world.Namespace)
		// The adapter waits a bounded time for this publication; keep its cost visible.
		_ = os.WriteFile(path+".reconcile", []byte(time.Since(started).String()), 0600)
		return revision, err
	}
	var diagnostics []string
	c.OnError = func(err error) { diagnostics = append(diagnostics, err.Error()) }
	if err := c.Step(ctx, time.Unix(1001, 0), nil); err != nil {
		t.Fatal(err)
	}
	if err := c.PersistState(ctx); err != nil {
		t.Fatal(err)
	}
	head := nativeGit(t, root, "rev-parse", "HEAD")
	if out, err := exec.Command("git", "-C", root, "show", head+":.sdo/rendered/lock.json").CombinedOutput(); err != nil {
		t.Logf("lock read: %s", out)
		t.Fatalf("ACK completed but lock never published: %v", diagnostics)
	}
	crashAt(path, cut, "after-publication-before-teardown")
	ledgerState, err := native.readLedger(id)
	if err != nil {
		t.Fatal(err)
	}
	if !ledgerState.Acknowledged || ledgerState.OutcomeDocument == nil || ledgerState.Receipt == nil {
		t.Fatal("missing acknowledged outcome/receipt")
	}
	if len(exportState(t, c).FailedClosures) != 0 || c.closureBlocksNextLocked() {
		t.Fatal("own committed closure retired or blocked")
	}
	restartClassWriteReceipt(t, native, path+".receipt", id)
}

func restartClassWriteReceipt(t *testing.T, native NativeIncidentBroker, path, id string) {
	t.Helper()
	ledgerState, err := native.readLedger(id)
	if err != nil {
		t.Fatal(err)
	}
	receipt := &contracts.NativeProductionReceipt{SchemaVersion: "sdo.production-receipt/v2", RecordedAt: contractTimestamp(time.Unix(1001, 0)), BrokerMode: "native", WorkerMountMode: "proposal-only", IncidentId: id, Namespace: ledgerState.Closure.Request.Namespace, ControlNamespace: "demo-sdo", ControllerImage: "hermetic-controller", ResponderImage: "scripted-responder", ValidatorImage: "hermetic-validator", ControllerPodUid: "fake-controller-uid", NativeLedger: ledgerState, ResponderJobEvidence: []*contracts.ResponderJobEvidence{{IncidentId: id, JobName: "scripted-restart-class", JobUid: "fake-job-uid", Succeeded: true}}}
	data, err := wire.Canonical(receipt)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}

// restartClassBoundary kills the controller at one durable boundary, restarts
// it, and checks the restart-class invariants against the real broker, outcome
// store and strict receipt. root is a prepared accepted checkout with a current
// rendered lock at base.
func restartClassBoundary(t *testing.T, ctx context.Context, world restartClassWorld, cut, directory, root, base string) {
	t.Helper()
	fixture := os.Getenv("RT1_CLOSEOUT_FIXTURE")
	proposals, ledger := filepath.Join(directory, "proposals"), filepath.Join(directory, "ledger")
	native := restartClassNative(t, root, proposals, ledger)
	source := GitPlanSource{Repository: root, ManifestPaths: world.ManifestPaths}
	if _, err := (RenderedLockReconciler{Source: source, Client: fake.NewSimpleClientset(world.Live(t, root, base)...), Broker: native.Repository}).Reconcile(ctx, base, world.Namespace); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "controller.json")
	c := restartClassIncident(t, ctx, world, root, base, path, native)
	id := c.pendingClosure.Request.IncidentID
	if cut == "after-receipt-before-ack" {
		// Isolate the receipt-durable boundary from the earlier lost-
		// receipt boundary. Persist the real public completion before
		// installing the successor's newly adopted health guard.
		effect, ok := c.PendingClosureEffect()
		if !ok {
			t.Fatal("missing closure before durable receipt")
		}
		if err := c.ExecuteClosureEffect(ctx, effect); err != nil {
			t.Fatal(err)
		}
		awaitBrokerResult(t, c.closureResults)
		c.processBrokerCompletions()
		if err := c.PersistState(ctx); err != nil {
			t.Fatal(err)
		}
	}
	env := []string{"RT1_ROOT=" + root, "RT1_PROPOSALS=" + proposals, "RT1_LEDGER=" + ledger, "RT1_STATE=" + path, "RT1_BASE=" + base, "RT1_INCIDENT=" + id}
	run := func(cut string, changed bool) ([]byte, error) {
		for adoption := 0; adoption < 3; adoption++ {
			command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRestartClassProcess$", "-test.timeout=45s")
			command.Env = append(append(os.Environ(), env...), "RT1_CUT="+cut)
			if changed {
				command.Env = append(command.Env, "RT1_CHANGED_GENERATION=1")
			}
			output, err := command.CombinedOutput()
			var exit *exec.ExitError
			if errors.As(err, &exit) && exit.ExitCode() == 75 {
				continue
			}
			return output, err
		}
		return nil, errors.New("controller exceeded two health adoptions")
	}
	output, err := run(cut, false)
	var exit *exec.ExitError
	if !errors.As(err, &exit) {
		t.Fatalf("did not crash at %s: %v %s", cut, err, output)
	}
	status, ok := exit.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("wrong crash at %s: %v %s", cut, err, output)
	}
	marker, err := os.ReadFile(path + ".cut")
	if err != nil || string(marker) != cut {
		t.Fatalf("wrong durable cut %q: %v", marker, err)
	}
	if fixture != "" {
		data, err := json.Marshal(env)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(fixture, "environment.json"), data, 0600); err != nil {
			t.Fatal(err)
		}
		return
	}
	beforeLedger, err := native.readLedger(id)
	if err != nil {
		t.Fatal(err)
	}
	beforeReceipt, err := wire.Canonical(beforeLedger.Receipt)
	if beforeLedger.Receipt != nil && err != nil {
		t.Fatal(err)
	}
	changed := cut == "after-effect-before-receipt" || cut == "after-receipt-before-ack"
	if os.Getenv("SDO_RT1_STABLE_OBSERVATION") == "1" {
		changed = false
	}
	if output, err := run("", changed); err != nil {
		t.Fatalf("restart boundary=%s changed_generation=%t: %v\n%s", cut, changed, err, output)
	}
	// Count actual accepted source changes rather than counting broker calls:
	// receipt replay is expected, a duplicate public source effect is forbidden.
	count := nativeGit(t, root, "rev-list", "--count", base+"..HEAD", "--", world.EffectPath)
	if count != "1" {
		t.Fatalf("public repair effects=%s, want exactly one", count)
	}
	afterLedger, err := native.readLedger(id)
	if err != nil {
		t.Fatal(err)
	}
	afterReceipt, err := wire.Canonical(afterLedger.Receipt)
	if err != nil {
		t.Fatal(err)
	}
	if beforeLedger.Receipt != nil && !bytes.Equal(beforeReceipt, afterReceipt) {
		t.Fatal("restart changed the committed receipt")
	}
	outcome, err := memory.OpenReader(root).Get(ctx, id)
	if err != nil || outcome.Record.GetClassification() != "success" || outcome.Record.GetRepairCommit() != afterLedger.ProposalCommit {
		t.Fatalf("broker outcome missing or mismatched: %v", err)
	}
	raw, err := os.ReadFile(path + ".receipt")
	if err != nil {
		t.Fatal(err)
	}
	restartClassStrictReceipt(t, ctx, raw)
	reconcile, _ := os.ReadFile(path + ".reconcile")
	t.Logf("boundary=%s changed_generation=%t public_effects=1 receipt=strict outcome=recorded lock_reconcile=%s", cut, changed, reconcile)
}

func TestRestartClassHermetic(t *testing.T) {
	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 270*time.Second)
	t.Cleanup(cancel)
	// One immutable seed amortizes accepted health publication across schedules.
	baseline, _, _, _, coordinator := tracingFixture(t)
	tracingRefresh(t, baseline, coordinator)
	for index, cut := range restartClassCuts {
		if ctx.Err() != nil {
			t.Fatal("restart sweep exceeded 270-second budget")
		}
		t.Run(fmt.Sprintf("seed=%d/%s", index+1, cut), func(t *testing.T) {
			// Each schedule clones the immutable seed and owns all process/state paths.
			// Respect go test -parallel while overlapping independent Git/process waits.
			t.Parallel()
			seed := uint64(index + 1)
			if raw := os.Getenv("SDO_CHAOS_SEED"); raw != "" {
				parsed, err := strconv.ParseUint(raw, 10, 64)
				if err != nil {
					t.Fatal(err)
				}
				seed ^= parsed
			}
			random := newChaosRNG(seed)
			fixture := os.Getenv("RT1_CLOSEOUT_FIXTURE")
			if fixture != "" && cut != "after-ack-before-publication" {
				t.Skip("adapter bridge selects ACK boundary")
			}
			directory := t.TempDir()
			if fixture != "" {
				directory = fixture
			}
			root := filepath.Join(directory, "accepted")
			testutil.CloneGit(t, baseline, root)
			nativeGit(t, root, "config", "user.name", "Test")
			nativeGit(t, root, "config", "user.email", "test@example.com")
			nativeGit(t, root, "config", "gc.auto", "0")
			// Rounded public inventory totals: astronomy 10,998,805 bytes;
			// social 86,977,143 bytes. No experiment source or secrets copied.
			asset, err := os.Create(filepath.Join(root, "synthetic-asset.bin"))
			if err != nil {
				t.Fatal(err)
			}
			size := int64(11 << 20)
			if index%2 == 1 || cut == "after-ack-before-publication" {
				size = 83 << 20
			}
			size += int64(random.IntN(2048))
			if err := asset.Truncate(size); err != nil {
				t.Fatal(err)
			}
			if err := asset.Close(); err != nil {
				t.Fatal(err)
			}
			nativeGit(t, root, "add", "synthetic-asset.bin")
			nativeGit(t, root, "commit", "-qm", "representative synthetic source inventory")
			judge, _ := crashCoordinator(t, root, filepath.Join(directory, "proposals"), filepath.Join(t.TempDir(), "judge"), "")
			old := tracingRefresh(t, root, judge)
			restartClassBoundary(t, ctx, restartClassTracingWorld(), cut, directory, root, old.Commit)
		})
	}
	t.Cleanup(func() { t.Logf("default restart sweep wall=%s", time.Since(started)) })
}

// STALE3: the detector scope changing mid-incident (SDO's own judge or
// reflection changed a detector) is not evidence about the cluster. The
// recheck runs under the detectors now in force: a healthy closure commits
// verified, an unhealthy one is refused by that live health.
func TestRestartClassRechecksUnderCurrentScopeBeforeCommit(t *testing.T) {
	for _, mutation := range []string{"generation", "health"} {
		t.Run(mutation, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			root, proposals, _, _, coordinator := tracingFixture(t)
			base := tracingRefresh(t, root, coordinator)
			native := restartClassNative(t, root, proposals, t.TempDir())
			c := restartClassIncident(t, ctx, restartClassTracingWorld(), root, base.Commit, filepath.Join(t.TempDir(), "controller.json"), native)
			rv6Guard(c, root, base.Commit)
			id := c.pendingClosure.Request.IncidentID
			if mutation == "generation" {
				snapshot := restartClassSnapshot()
				snapshot.Generation = "uncommitted-changed-scope"
				c.provider = staticProvider{snapshot: snapshot}
			} else {
				c.detectors["health"].(*chaosDetector).active.set(true)
			}
			effect, ok := c.PendingClosureEffect()
			if !ok {
				t.Fatal("missing durable closure")
			}
			err := c.ExecuteClosureEffect(ctx, effect)
			if mutation == "generation" {
				if err != nil {
					t.Fatalf("changed scope refused the closure: %v", err)
				}
				awaitBrokerResult(t, c.closureResults)
				c.processBrokerCompletions()
				if c.closureReceipt == nil || exportState(t, c).GetClosureState() == contracts.ClosureState_CLOSURE_STATE_FAILED {
					t.Fatalf("closure under a changed scope was not verified: receipt %v failure %v", c.closureReceipt, exportState(t, c).GetClosureFailure())
				}
				ledger, err := native.readLedger(id)
				if err != nil || ledger.Receipt == nil || ledger.OutcomeDocument == nil {
					t.Fatalf("verified closure did not publish its outcome: %v", err)
				}
				return
			}
			if err != nil || exportState(t, c).GetClosureState() != contracts.ClosureState_CLOSURE_STATE_FAILED || !strings.Contains(exportState(t, c).GetClosureFailure().GetLastError(), "is active") {
				t.Fatalf("restored success was not revalidated against fresh health: %v", err)
			}
			ledger, err := native.readLedger(id)
			if err != nil || ledger.Receipt != nil || ledger.OutcomeDocument != nil {
				t.Fatalf("refused closure reached publication: %v", err)
			}
		})
	}
}

// Use the benchmark receipt CLI as an external transport. The production
// runtime gains no benchmark import or module dependency.
func restartClassStrictReceipt(t *testing.T, ctx context.Context, raw []byte) {
	t.Helper()
	command := exec.CommandContext(ctx, "go", "run", "./cmd/sdo-native-receipt", "build")
	command.Dir = "../../../benchmarks/sregym/protocol/native_receipt_go"
	command.Stdin = bytes.NewReader(raw)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("strict receipt transport: %v %s", err, output)
	}
	result := &contracts.NativeReceiptResult{}
	if err := wire.UnmarshalDurable(output, result); err != nil {
		t.Fatal(err)
	}
	if result.Failure != nil {
		t.Fatalf("strict receipt rejected: %s", result.Failure.Message)
	}
	if !strings.Contains(string(output), "sdo_mitigated") {
		t.Fatal("strict receipt lost successful resolution")
	}
}
