package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"sdo.dev/operator/onboarding"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

func stagedInstallerController(t *testing.T, health sdk.Detector) (*Controller, *fake.Clientset) {
	t.Helper()
	ctx := context.Background()
	declared, live := cleanInstallerInventory()
	objects := []kruntime.Object{}
	for _, o := range live {
		obj, err := scheme.Scheme.New(o.GroupVersionKind())
		if err != nil {
			t.Fatal(err)
		}
		if err := kruntime.DefaultUnstructuredConverter.FromUnstructured(o.Object, obj); err != nil {
			t.Fatal(err)
		}
		objects = append(objects, obj)
	}
	client := fake.NewSimpleClientset(objects...)
	commit := strings.Repeat("a", 40)
	if err := onboarding.Stage(ctx, client, "control", "app", commit, declared, time.Unix(100, 0)); err != nil {
		t.Fatal(err)
	}
	config := testControllerConfig()
	config.Namespace, config.SourceCommit = "app", commit
	c, err := NewController(config, []sdk.Detector{health}, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}, &recordingDispatcher{requests: make(chan IncidentRequest, 10)}, time.Unix(100, 0))
	if err != nil {
		t.Fatal(err)
	}
	c.ConfigureDrift(DriftControllerConfig{Inventory: true, Source: &inventorySource{objects: declared}, Events: &inventoryStore{objects: live}})
	if err := c.AttachPendingOnboarding(ctx, client, "control"); err != nil {
		t.Fatal(err)
	}
	return c, client
}

func TestSourceInventoryPendingOnboardingUsesFirstClusterEvaluation(t *testing.T) {
	ctx := context.Background()
	c, client := stagedInstallerController(t, inventoryHealth())
	if record, err := onboarding.Load(ctx, client, "control", "app"); err != nil || record != nil {
		t.Fatalf("install prematurely granted authority: %v %v", record, err)
	}
	// Cluster detectors use the existing controller snapshot/prober, with no
	// host resolver or additional health-probe process involved.
	started := time.Now()
	if err := c.EvaluateAll(ctx, time.Unix(101, 0)); err != nil {
		t.Fatal(err)
	}
	record, err := onboarding.Load(ctx, client, "control", "app")
	if err != nil || record == nil {
		t.Fatalf("first in-cluster clear did not establish onboarding: %v %v", record, err)
	}
	t.Logf("hermetic first evaluation to persisted record: %s", time.Since(started))
	if !record.EstablishedAt.AsTime().Equal(time.Unix(100, 0)) {
		t.Fatal("lost install boundary")
	}
	if len(c.sourceInventoryFindings) != 0 {
		t.Fatalf("clean fixture produced findings: %+v", c.sourceInventoryFindings)
	}
	additions := latentInventoryObjects()
	c.drift.Events.(*inventoryStore).objects = append(c.drift.Events.(*inventoryStore).objects, additions[0], additions[3], additions[4])
	c.observeSourceInventory(time.Unix(102, 0))
	if ok, remaining := c.verifySourceInventoryLocked(nil); ok || len(remaining) != 3 {
		t.Fatalf("later controller and RBAC must block: %v %+v", ok, remaining)
	}
	if _, err := client.CoreV1().ConfigMaps("control").Get(ctx, onboarding.AttemptConfigMapName, metav1.GetOptions{}); err != nil {
		t.Fatal(err)
	}
}

func TestSourceInventoryPendingOnboardingNeverRetriesUnclearOrChangedCommit(t *testing.T) {
	for _, mode := range []string{"finding", "error", "commit", "incident", "prior-incident"} {
		t.Run(mode, func(t *testing.T) {
			health := inventoryHealth().(*sequenceDetector)
			c, client := stagedInstallerController(t, health)
			switch mode {
			case "finding":
				health.samples = [][]sdk.Finding{{stateFinding("broken")}, {}}
			case "error":
				health.errors = map[int]error{0: errors.New("cluster probe transport unavailable")}
			case "commit":
				c.config.SourceCommit = strings.Repeat("b", 40)
			case "incident":
				c.incidentOpen = true
			case "prior-incident":
				c.lastAcknowledgedIncidentID = "closed-before-onboarding"
			}
			if err := c.EvaluateAll(context.Background(), time.Unix(101, 0)); err != nil {
				t.Fatal(err)
			}
			if record, err := onboarding.Load(context.Background(), client, "control", "app"); err != nil || record != nil {
				t.Fatalf("unclear first evaluation granted authority: %v %v", record, err)
			}
			// Restore a healthy controller using the same durable pending/attempt state.
			restarted, err := NewController(c.config, []sdk.Detector{inventoryHealth()}, c.provider, c.dispatcher, time.Unix(102, 0))
			if err != nil {
				t.Fatal(err)
			}
			restarted.config.SourceCommit = strings.Repeat("a", 40)
			if err := restarted.AttachPendingOnboarding(context.Background(), client, "control"); err != nil {
				t.Fatal(err)
			}
			if err := restarted.EvaluateAll(context.Background(), time.Unix(103, 0)); err != nil {
				t.Fatal(err)
			}
			if record, err := onboarding.Load(context.Background(), client, "control", "app"); err != nil || record != nil {
				t.Fatalf("restart/later healthy period granted authority: %v %v", record, err)
			}
		})
	}
}

func TestSourceInventoryPendingOnboardingCannotBlessFaultBeforeFirstEvaluation(t *testing.T) {
	c, client := stagedInstallerController(t, inventoryHealth())
	additions := latentInventoryObjects()
	c.drift.Events.(*inventoryStore).objects = append(c.drift.Events.(*inventoryStore).objects, additions[0], additions[3], additions[4])
	if err := c.EvaluateAll(context.Background(), time.Unix(101, 0)); err != nil {
		t.Fatal(err)
	}
	record, err := onboarding.Load(context.Background(), client, "control", "app")
	if err != nil || record != nil {
		t.Fatalf("post-install latent incident must prevent onboarding: %+v %v", record, err)
	}
}

func TestSourceInventoryPendingOnboardingWaitsForCompleteEvaluation(t *testing.T) {
	c, client := stagedInstallerController(t, inventoryHealth())
	peer := inventoryHealth().(*sequenceDetector)
	peer.spec.ID, peer.spec.Interval = "peer-health", 10*time.Second
	c, err := NewController(c.config, []sdk.Detector{inventoryHealth(), peer}, c.provider, c.dispatcher, time.Unix(100, 0))
	if err != nil {
		t.Fatal(err)
	}
	// No inventory integration is needed to check detector completeness here;
	// install identities still come exclusively from the durable Stage snapshot.
	if err := c.AttachPendingOnboarding(context.Background(), client, "control"); err != nil {
		t.Fatal(err)
	}
	c.scheduler.SelectEvents(time.Unix(100, 0), nil)
	c.scheduler.Expedite("health", time.Unix(100, 1))
	if err := c.Step(context.Background(), time.Unix(100, 1), nil); err != nil {
		t.Fatal(err)
	}
	if record, err := onboarding.Load(context.Background(), client, "control", "app"); err != nil || record != nil {
		t.Fatalf("partial clear sample granted authority: %v %v", record, err)
	}
	if err := c.EvaluateAll(context.Background(), time.Unix(101, 0)); err != nil {
		t.Fatal(err)
	}
	if record, err := onboarding.Load(context.Background(), client, "control", "app"); err != nil || record == nil {
		t.Fatalf("complete clear sample did not establish record: %v %v", record, err)
	}
}

func TestSourceInventoryPendingOnboardingPrecedesStartupReconciliation(t *testing.T) {
	c, client := stagedInstallerController(t, inventoryHealth())
	calls := 0
	c.ReconcileRendered = func(context.Context, string) (string, error) { calls++; return strings.Repeat("b", 40), nil }
	if err := c.EvaluateAll(context.Background(), time.Unix(101, 0)); err != nil {
		t.Fatal(err)
	}
	record, err := onboarding.Load(context.Background(), client, "control", "app")
	if err != nil || record == nil || record.AcceptedCommit != strings.Repeat("a", 40) || calls != 0 {
		t.Fatalf("startup publication invalidated install verification: record=%v err=%v reconciliations=%d", record, err, calls)
	}
	if err := c.EvaluateAll(context.Background(), time.Unix(102, 0)); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("normal reconciliation did not resume after onboarding: %d", calls)
	}
}

type installWindowDetector struct {
	sdk.Detector
	confirmed *bool
}

func (d installWindowDetector) Confirmed(sdk.DetectionContext) bool { return *d.confirmed }

func TestSourceInventoryPendingOnboardingConfirmedSweep(t *testing.T) {
	for _, mode := range []string{"clear", "goal-error", "peer-error", "goal-finding", "commit", "prior-incident", "inventory", "transient-inventory", "transient-commit", "restart"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			goal := inventoryHealth().(*sequenceDetector)
			goal.spec.Interval = time.Hour
			goal.spec.Watches = []sdk.WatchKind{{APIVersion: "apps/v1", Kind: "Deployment"}}
			staged, client := stagedInstallerController(t, goal)
			peer := inventoryHealth().(*sequenceDetector)
			peer.spec.ID, peer.spec.Interval = "peer-health", time.Hour
			peer.spec.Watches = []sdk.WatchKind{{APIVersion: "v1", Kind: "Service"}}
			confirmed := false
			c, err := NewController(staged.config, []sdk.Detector{goal, installWindowDetector{Detector: peer, confirmed: &confirmed}}, staged.provider, staged.dispatcher, time.Unix(100, 0))
			if err != nil {
				t.Fatal(err)
			}
			c.ConfigureDrift(*staged.drift)
			if err := c.AttachPendingOnboarding(ctx, client, "control"); err != nil {
				t.Fatal(err)
			}
			if err := c.StepEvents(ctx, time.Unix(101, 0), nil); err != nil {
				t.Fatal(err)
			}
			event := []sdk.WatchKind{{APIVersion: "v1", Kind: "Service", Namespace: "app"}}
			historyStart := len(c.history)
			store := c.drift.Events.(*inventoryStore)
			initialObjects := append([]*unstructured.Unstructured(nil), store.objects...)
			if mode == "transient-inventory" {
				store.objects = append(store.objects, latentInventoryObjects()[0])
			}
			if mode == "transient-commit" {
				c.config.SourceCommit = strings.Repeat("b", 40)
			}
			if err := c.StepEvents(ctx, time.Unix(102, 0), event); err != nil {
				t.Fatal(err)
			}
			if evaluated := c.history[historyStart:]; len(evaluated) != 1 || evaluated[0].DetectorID != peer.spec.ID || evaluated[0].Status != DetectorEvaluationPending {
				t.Fatalf("expected partial pending event batch: %+v", evaluated)
			}
			if _, err := client.CoreV1().ConfigMaps("control").Get(ctx, onboarding.AttemptConfigMapName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
				t.Fatalf("partial unconfirmed batch consumed attempt: %v", err)
			}
			store.objects = initialObjects
			c.config.SourceCommit = staged.config.SourceCommit
			deadline := c.scheduler.Deadlines()[goal.spec.ID]
			confirmed = true
			if mode == "restart" {
				checkpoint := lifecycleio.NewConfigMapStateStore(client, "control", "sdo-controller-state")
				if err := c.AttachStateStore(ctx, checkpoint); err != nil {
					t.Fatal(err)
				}
				if err := c.PersistState(ctx); err != nil {
					t.Fatal(err)
				}
				restarted, err := NewController(c.config, []sdk.Detector{goal, installWindowDetector{Detector: peer, confirmed: &confirmed}}, staged.provider, staged.dispatcher, time.Unix(102, 0))
				if err != nil {
					t.Fatal(err)
				}
				restarted.ConfigureDrift(*staged.drift)
				if err := restarted.AttachStateStore(ctx, checkpoint); err != nil {
					t.Fatal(err)
				}
				if err := restarted.AttachPendingOnboarding(ctx, client, "control"); err != nil {
					t.Fatal(err)
				}
				c = restarted
			}
			switch mode {
			case "goal-error":
				goal.errors = map[int]error{goal.calls: errors.New("goal transport unavailable")}
			case "peer-error":
				peer.errors = map[int]error{peer.calls: errors.New("peer transport unavailable")}
			case "goal-finding":
				goal.samples = [][]sdk.Finding{{stateFinding("broken")}}
			case "commit":
				c.config.SourceCommit = strings.Repeat("b", 40)
			case "prior-incident":
				c.lastAcknowledgedIncidentID = "closed-during-install"
			case "inventory":
				store := c.drift.Events.(*inventoryStore)
				store.objects = append(store.objects, latentInventoryObjects()[0])
			}
			historyStart = len(c.history)
			if err := c.StepEvents(ctx, time.Unix(103, 0), event); err != nil {
				t.Fatal(err)
			}
			evaluated := map[string]int{}
			for _, evaluation := range c.history[historyStart:] {
				evaluated[evaluation.DetectorID]++
			}
			if evaluated[goal.spec.ID] != 1 || evaluated[peer.spec.ID] != 1 {
				t.Fatalf("confirmed event batch did not sweep every detector exactly once: %+v", evaluated)
			}
			if next := c.scheduler.Deadlines()[goal.spec.ID]; !next.Equal(deadline) {
				t.Fatalf("install sweep postponed ordinary fault detection: %s -> %s", deadline, next)
			}
			record, err := onboarding.Load(ctx, client, "control", "app")
			if err != nil || (record != nil) != (mode == "clear" || mode == "restart") {
				t.Fatalf("full-sweep authority: mode=%s record=%v err=%v", mode, record, err)
			}
			if _, err := client.CoreV1().ConfigMaps("control").Get(ctx, onboarding.AttemptConfigMapName, metav1.GetOptions{}); err != nil {
				t.Fatalf("confirmed full sweep did not durably consume attempt: %v", err)
			}
			if mode != "clear" && mode != "restart" {
				goal.errors, peer.errors = nil, nil
				goal.samples = [][]sdk.Finding{{}}
				c.config.SourceCommit = staged.config.SourceCommit
				c.lastAcknowledgedIncidentID = ""
				if err := c.EvaluateAll(ctx, time.Unix(104, 0)); err != nil {
					t.Fatal(err)
				}
				if record, err := onboarding.Load(ctx, client, "control", "app"); err != nil || record != nil {
					t.Fatalf("later healthy period retried the sole attempt: %v %v", record, err)
				}
			}
		})
	}
}
