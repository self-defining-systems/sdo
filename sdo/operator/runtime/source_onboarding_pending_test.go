package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"sdo.dev/operator/onboarding"
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
