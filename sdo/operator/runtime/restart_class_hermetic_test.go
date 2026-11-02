package runtime

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
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

func restartClassWriteReceipt(t *testing.T, native NativeIncidentBroker, path, id string) {
	t.Helper()
	ledgerState, err := native.readLedger(id)
	if err != nil {
		t.Fatal(err)
	}
	receipt := &contracts.NativeProductionReceipt{SchemaVersion: "sdo.production-receipt/v2", RecordedAt: contractTimestamp(time.Unix(1001, 0)), BrokerMode: "native", WorkerMountMode: "proposal-only", IncidentId: id, Namespace: ledgerState.Closure.Request.Namespace, ControlNamespace: "demo-sdo", ControllerImage: "hermetic-controller", ResponderImage: "scripted-responder", ControllerPodUid: "fake-controller-uid", NativeLedger: ledgerState, ResponderJobEvidence: []*contracts.ResponderJobEvidence{{IncidentId: id, JobName: "scripted-restart-class", JobUid: "fake-job-uid", Succeeded: true}}}
	data, err := wire.Canonical(receipt)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}
