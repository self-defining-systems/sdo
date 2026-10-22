package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/runtime/internal/lifecycleio"
)

func TestFailedClosureArchivePrecedesNextIncidentEffects(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1000, 0).UTC()
	controller := newClosureRetryController(t, ClosureRetryPolicy{MaxAttempts: 1}, &rejectingIncidentBroker{rejections: 10}, &now)
	client := fake.NewSimpleClientset()
	enforceConfigMapResourceVersions(client)
	store := lifecycleio.NewConfigMapStateStore(client, "demo", "state")
	if err := controller.AttachStateStore(ctx, store); err != nil {
		t.Fatal(err)
	}
	if err := controller.PersistState(ctx); err != nil {
		t.Fatal(err)
	}
	attemptClosure(t, controller)
	failedID := controller.pendingClosure.Request.IncidentID
	controller.history = testDetectorHistory(now)
	controller.batcher.AddAt(stateFinding("next"), now)
	if err := controller.dispatchReady(ctx, now, nil); err != nil {
		t.Fatal(err)
	}
	effect, ok := controller.PendingWorkspaceEffect()
	if !ok {
		t.Fatal("failed closure blocked next incident")
	}
	unavailable := true
	client.PrependReactor("create", "configmaps", func(action ktesting.Action) (bool, k8sruntime.Object, error) {
		object := action.(ktesting.CreateAction).GetObject().(*corev1.ConfigMap)
		if unavailable && strings.HasPrefix(object.Name, "sdo-failed-closure-") {
			return true, nil, apierrors.NewServiceUnavailable("archive unavailable")
		}
		return false, nil, nil
	})
	if err := controller.PersistState(ctx); err == nil {
		t.Fatal("failed archive was acknowledged")
	}
	if err := controller.ExecuteWorkspaceEffect(ctx, effect); !errors.Is(err, ErrEffectNotDurable) {
		t.Fatalf("new effect ran before old evidence durable: %v", err)
	}
	unavailable = false
	if err := controller.PersistState(ctx); err != nil {
		t.Fatal(err)
	}
	archives, err := client.CoreV1().ConfigMaps("demo").List(ctx, metav1.ListOptions{LabelSelector: "sdo.dev/closure-archive=failed"})
	if err != nil {
		t.Fatal(err)
	}
	if len(archives.Items) != 1 {
		t.Fatalf("failure archive count = %d", len(archives.Items))
	}
	record := &contracts.FailedIncidentClosure{}
	if err := wire.Unmarshal([]byte(archives.Items[0].Data[lifecycleio.FailedClosureDataKey]), record); err != nil {
		t.Fatal(err)
	}
	if record.GetClosure().GetRequest().GetIncidentId() != failedID || record.GetClosure().GetResult() == nil || !record.GetFailure().GetPermanent() {
		t.Fatalf("terminal evidence lost: %v", record)
	}
	loaded, _, err := store.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.GetFailedClosures()) != 0 || len(exportState(t, controller).FailedClosures) != 0 {
		t.Fatal("archive outbox accumulated in main state")
	}
	if err := controller.ExecuteWorkspaceEffect(ctx, effect); err != nil {
		t.Fatalf("next incident cannot progress: %v", err)
	}
	awaitBrokerResult(t, controller.workspaceResults)
	controller.processBrokerCompletions()
	if _, ok := controller.PendingDispatchEffect(); !ok {
		t.Fatal("next responder not ready")
	}
}

func TestClosureEffectRejectsAlteredPayloadWithoutConsumingPendingWork(t *testing.T) {
	now := time.Unix(1000, 0)
	controller := newClosureRetryController(t, ClosureRetryPolicy{}, &rejectingIncidentBroker{}, &now)
	effect, ok := controller.PendingClosureEffect()
	if !ok {
		t.Fatal("missing effect")
	}
	original := effect
	effect.Closure.Request.SourceCommit = "changed"
	if err := controller.ExecuteClosureEffect(context.Background(), effect); err == nil {
		t.Fatal("altered effect accepted")
	}
	next, ok := controller.PendingClosureEffect()
	if !ok || next.Identity != original.Identity {
		t.Fatal("rejection consumed pending work")
	}
	original.Closure = next.Closure
	original.Identity = "stale"
	if err := controller.ExecuteClosureEffect(context.Background(), original); err == nil {
		t.Fatal("stale identity accepted")
	}
	if _, ok := controller.PendingClosureEffect(); !ok {
		t.Fatal("stale identity consumed pending work")
	}
}

func TestFailureArchiveSurvivesCrashBeforeMainStateCAS(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1000, 0)
	broker := &rejectingIncidentBroker{rejections: 10}
	client := fake.NewSimpleClientset()
	enforceConfigMapResourceVersions(client)
	controller := newClosureRetryController(t, ClosureRetryPolicy{MaxAttempts: 1}, broker, &now)
	if err := controller.AttachStateStore(ctx, lifecycleio.NewConfigMapStateStore(client, "demo", "state")); err != nil {
		t.Fatal(err)
	}
	if err := controller.PersistState(ctx); err != nil {
		t.Fatal(err)
	}
	attemptClosure(t, controller)
	fail := true
	client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, k8sruntime.Object, error) {
		if fail && action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap).Name == "state" {
			return true, nil, apierrors.NewServiceUnavailable("root CAS response lost")
		}
		return false, nil, nil
	})
	if err := controller.PersistState(ctx); err == nil {
		t.Fatal("expected interrupted root CAS")
	}
	fail = false
	now = now.Add(time.Hour)
	restored := newClosureRetryController(t, ClosureRetryPolicy{MaxAttempts: 1}, broker, &now)
	if err := restored.AttachStateStore(ctx, lifecycleio.NewConfigMapStateStore(client, "demo", "state")); err != nil {
		t.Fatal(err)
	}
	attemptClosure(t, restored)
	if err := restored.PersistState(ctx); err != nil {
		t.Fatalf("archive replay blocked progress: %v", err)
	}
	archives, err := client.CoreV1().ConfigMaps("demo").List(ctx, metav1.ListOptions{LabelSelector: "sdo.dev/closure-archive=failed"})
	if err != nil {
		t.Fatal(err)
	}
	if len(archives.Items) != 2 {
		t.Fatalf("distinct terminal observations were overwritten: %d", len(archives.Items))
	}
}
