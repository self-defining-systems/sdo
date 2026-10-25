package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/worker_registry"
)

// bulkyMemoryCatalog is a hand-written logical memory snapshot far past one ConfigMap.
type bulkyMemoryCatalog struct {
	commit string
	bytes  int
}

func (catalog bulkyMemoryCatalog) Snapshot(context.Context) (*contracts.OperationalMemorySnapshot, error) {
	return &contracts.OperationalMemorySnapshot{AcceptedCommit: catalog.commit, GoalMarkdown: strings.Repeat("goal ", catalog.bytes/5)}, nil
}

// recordConfigMapWrites tracks the largest data payload of every ConfigMap create or update.
func recordConfigMapWrites(client *fake.Clientset) *map[string]int {
	sizes := map[string]int{}
	record := func(action ktesting.Action) (bool, k8sruntime.Object, error) {
		object := action.(interface{ GetObject() k8sruntime.Object }).GetObject().(*corev1.ConfigMap)
		total := 0
		for _, value := range object.Data {
			total += len(value)
		}
		sizes[object.Name] = max(sizes[object.Name], total)
		return false, nil, nil
	}
	client.PrependReactor("create", "configmaps", record)
	client.PrependReactor("update", "configmaps", record)
	return &sizes
}

// mountedBroker places proposal worktrees under the shared repository mount.
type mountedBroker struct {
	*rejectingIncidentBroker
	root string
}

func (b mountedBroker) PrepareIncident(_ context.Context, incidentID string) (IncidentWorkspace, error) {
	return IncidentWorkspace{IncidentID: incidentID, Worktree: b.root + "/proposals/" + incidentID, BaseCommit: "base"}, nil
}

func TestOversizedControllerStateFailsWithTypedErrorNamingLargestField(t *testing.T) {
	client := fake.NewSimpleClientset()
	sizes := recordConfigMapWrites(client)
	state := &contracts.ControllerState{Version: RuntimeStateVersion, DispatchError: strings.Repeat("x", lifecycleio.MaxConfigMapBytes+1)}
	_, err := lifecycleio.NewConfigMapStateStore(client, "demo", "state").Save(context.Background(), state, "")
	var tooLarge *lifecycleio.ConfigMapTooLargeError
	if !errors.As(err, &tooLarge) || len(tooLarge.Largest) == 0 || !strings.HasPrefix(tooLarge.Largest[0], "dispatch_error=") {
		t.Fatalf("expected a typed error naming the largest field, got %v", err)
	}
	if len(*sizes) != 0 {
		t.Fatalf("an over-budget state reached the API server: %v", *sizes)
	}
}

// TestEveryControllerConfigMapStaysUnderBudgetWithLargeMemory drives a closure failure and the next incident's
// dispatch with a 5 MiB memory snapshot and asserts no ConfigMap the controller writes (state with its incident
// view, failed-closure archive, worker request) exceeds the budget.
func TestEveryControllerConfigMapStaysUnderBudgetWithLargeMemory(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	now := time.Unix(1000, 0).UTC()
	root := t.TempDir()
	client := fake.NewSimpleClientset()
	sizes := recordConfigMapWrites(client)
	controller := newClosureRetryController(t, ClosureRetryPolicy{MaxAttempts: 1}, mountedBroker{&rejectingIncidentBroker{rejections: 10}, root}, &now)
	// Onboarding retains object identities, including in the incident view. It
	// must fit alongside a large task without copying live object contents.
	controller.namespaceOnboarding = &contracts.NamespaceOnboarding{
		Namespace: "demo", AcceptedCommit: strings.Repeat("a", 40), EstablishedAt: timestamppb.New(now),
	}
	for i := range 1000 {
		controller.namespaceOnboarding.Objects = append(controller.namespaceOnboarding.Objects,
			&contracts.ObjectRef{ApiVersion: "v1", Kind: "ConfigMap", Namespace: "demo", Name: fmt.Sprintf("settings-%04d", i)})
	}
	created := make(chan *batchv1.Job, 1)
	client.PrependReactor("create", "jobs", func(action ktesting.Action) (bool, k8sruntime.Object, error) {
		created <- action.(ktesting.CreateAction).GetObject().(*batchv1.Job).DeepCopy()
		return false, nil, nil
	})
	controller.dispatcher = KubernetesJobDispatcher{
		Client: client, Namespace: "demo", Registry: testIncidentRegistry(t, "responder:test"), AgentID: testIncidentAgentID(t),
		ServiceAccount: "sdo-responder", RepositoryPVC: "repository", RepositoryMountPath: root,
		Catalog: bulkyMemoryCatalog{commit: "base", bytes: 5 << 20}, PollInterval: time.Millisecond,
	}
	store := lifecycleio.NewConfigMapStateStore(client, "demo", "sdo-controller-state")
	if err := controller.AttachStateStore(ctx, store); err != nil {
		t.Fatal(err)
	}
	// The closed incident was dispatched with the same bulky task; its closure must not carry it.
	controller.pendingClosure.Request.PreparedAgentTask = strings.Repeat("t", 5<<20)
	if err := controller.PersistState(ctx); err != nil {
		t.Fatalf("persist pending closure: %v", err)
	}
	attemptClosure(t, controller)
	controller.history = testDetectorHistory(now)
	controller.batcher.AddAt(stateFinding("next"), now)
	if err := controller.dispatchReady(ctx, now, nil); err != nil {
		t.Fatal(err)
	}
	workspace, ok := controller.PendingWorkspaceEffect()
	if !ok {
		t.Fatal("failed closure blocked the next incident")
	}
	if err := controller.PersistState(ctx); err != nil {
		t.Fatalf("persist failed closure: %v", err)
	}
	if err := controller.ExecuteWorkspaceEffect(ctx, workspace); err != nil {
		t.Fatal(err)
	}
	awaitBrokerResult(t, controller.workspaceResults)
	controller.processBrokerCompletions()
	if err := controller.PersistState(ctx); err != nil {
		t.Fatalf("persist pending dispatch: %v", err)
	}
	dispatch, ok := controller.PendingDispatchEffect()
	if !ok {
		t.Fatal("next responder not ready")
	}
	if len(controller.currentIncidentRequest.PreparedAgentTask) < 5<<20 {
		t.Fatalf("prepared task did not carry the memory snapshot: %d bytes", len(controller.currentIncidentRequest.PreparedAgentTask))
	}
	if err := controller.ExecuteDispatchEffect(ctx, dispatch); err != nil {
		t.Fatal(err)
	}
	select {
	case completion := <-controller.results:
		t.Fatalf("dispatch ended before a Job was created: %v", completion.err)
	case job := <-created:
		if job.Name != IncidentJobName(dispatch.Request.IncidentID) {
			t.Fatalf("created Job %q for another incident", job.Name)
		}
	}
	if err := controller.PersistState(ctx); err != nil {
		t.Fatalf("persist dispatched incident: %v", err)
	}
	if len(*sizes) == 0 {
		t.Fatal("no ConfigMap writes were observed")
	}
	for name, size := range *sizes {
		if size > lifecycleio.MaxConfigMapBytes {
			t.Errorf("ConfigMap %q write is %d bytes, over the %d byte budget", name, size, lifecycleio.MaxConfigMapBytes)
		}
	}
	loaded, _, err := store.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := loaded.GetIncidentRequest().GetPreparedAgentTask(); !worker_registry.IsPreparedTaskReference(got) {
		t.Fatalf("state holds task bytes instead of a reference: %d bytes", len(got))
	}
	maps, err := client.CoreV1().ConfigMaps("demo").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, configMap := range maps.Items {
		if strings.HasSuffix(configMap.Name, "-request") {
			t.Errorf("an oversized worker request used a ConfigMap: %s", configMap.Name)
		}
	}
}

func TestRestoredControllerResolvesStagedTaskAndRejectsAlteredBytes(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1000, 0).UTC()
	root := t.TempDir()
	client := fake.NewSimpleClientset()
	dispatcher := KubernetesJobDispatcher{
		Client: client, Namespace: "demo", Registry: testIncidentRegistry(t, "responder:test"), AgentID: testIncidentAgentID(t),
		ServiceAccount: "sdo-responder", RepositoryPVC: "repository", RepositoryMountPath: root,
		Catalog: bulkyMemoryCatalog{commit: "base", bytes: 1 << 20}, PollInterval: time.Millisecond,
	}
	build := func() *Controller {
		controller := newClosureRetryController(t, ClosureRetryPolicy{MaxAttempts: 1}, &rejectingIncidentBroker{}, &now)
		controller.dispatcher = dispatcher
		controller.pendingClosure, controller.closureState = nil, contracts.ClosureState_CLOSURE_STATE_UNSPECIFIED
		return controller
	}
	first := build()
	store := lifecycleio.NewConfigMapStateStore(client, "demo", "sdo-controller-state")
	if err := first.AttachStateStore(ctx, store); err != nil {
		t.Fatal(err)
	}
	first.history = testDetectorHistory(now)
	first.batcher.AddAt(stateFinding("fault"), now)
	if err := first.dispatchReady(ctx, now, nil); err != nil {
		t.Fatal(err)
	}
	workspace, _ := first.PendingWorkspaceEffect()
	if err := first.PersistState(ctx); err != nil {
		t.Fatal(err)
	}
	if err := first.ExecuteWorkspaceEffect(ctx, workspace); err != nil {
		t.Fatal(err)
	}
	awaitBrokerResult(t, first.workspaceResults)
	first.processBrokerCompletions()
	if err := first.PersistState(ctx); err != nil {
		t.Fatal(err)
	}
	want := first.currentIncidentRequest.PreparedAgentTask
	second := build()
	if err := second.AttachStateStore(ctx, store); err != nil {
		t.Fatalf("restart could not resolve the staged task: %v", err)
	}
	if second.currentIncidentRequest == nil || second.currentIncidentRequest.PreparedAgentTask != want {
		t.Fatal("restored controller lost the exact frozen task")
	}
	incidentID := second.currentIncidentRequest.IncidentID
	dispatcher.ReleasePreparedTask(incidentID)
	if err := build().AttachStateStore(ctx, store); err == nil {
		t.Fatal("a missing staged task was accepted")
	}
	if _, err := dispatcher.StagePreparedTask(incidentID, []byte("different bytes")); err != nil {
		t.Fatal(err)
	}
	var unavailable *worker_registry.PreparedTaskUnavailableError
	if err := build().AttachStateStore(ctx, store); !errors.As(err, &unavailable) {
		t.Fatalf("altered staged bytes were accepted: %v", err)
	}
}
