package runtime

import (
	"context"
	"errors"
	"fmt"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

const durabilityStateName = "sdo-controller-state"

// durableState reads the controller state straight from the fake API
// server's storage, bypassing reactors, as a restarted controller would see it.
func durableState(client *fake.Clientset) (*contracts.ControllerState, error) {
	object, err := client.Tracker().Get(configMapsResource, "demo", durabilityStateName)
	if err != nil {
		return nil, err
	}
	return decodeControllerState([]byte(object.(*corev1.ConfigMap).Data[stateDataKey]))
}

func durabilityController(t *testing.T, client *fake.Clientset, config ControllerConfig, dispatcher Dispatcher) *Controller {
	t.Helper()
	config.FiringThreshold = 1
	controller, err := NewController(
		config,
		[]sdk.Detector{controllerDetector("fault", time.Second, stateFinding("fault")), controllerDetector("health", time.Second, sdk.Finding{})},
		staticProvider{snapshot: sdktest.Snapshot{}}, dispatcher, time.Unix(0, 0),
	)
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	controller.now = func() time.Time { return time.Unix(0, 0) }
	if err := controller.AttachStateStore(
		context.Background(), lifecycleio.NewConfigMapStateStore(client, "demo", durabilityStateName),
	); err != nil {
		t.Fatalf("attach state store: %v", err)
	}
	return controller
}

func TestDispatchRecordIsDurableBeforeResponderJobIsCreated(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := fake.NewSimpleClientset()
	observed := make(chan error, 1)
	client.PrependReactor("create", "jobs", func(k8stesting.Action) (bool, k8sruntime.Object, error) {
		state, err := durableState(client)
		switch {
		case err != nil:
			observed <- fmt.Errorf("no durable controller state at Job creation: %w", err)
		case !state.IncidentOpen || state.DispatchState != contracts.DispatchState_DISPATCH_STATE_PENDING || state.IncidentRequest == nil:
			observed <- fmt.Errorf("durable state lacks the pending dispatch at Job creation: %#v", state)
		case state.IncidentRequest.PreparedAgentTask == "":
			observed <- fmt.Errorf("durable state lacks the exact dispatched agent task")
		default:
			observed <- nil
		}
		return false, nil, nil
	})
	config := testControllerConfig()
	config.RepositoryWorktree = "/workspace/demo"
	controller := durabilityController(t, client, config, KubernetesJobDispatcher{
		Client: client, Namespace: "demo", Registry: testIncidentRegistry(t, "sdo-responder:test"), AgentID: testIncidentAgentID(t),
		ServiceAccount: "sdo-responder", RepositoryPVC: "repository", CredentialsSecret: "sdo-codex",
		PollInterval: time.Millisecond,
	})
	if err := controller.Step(ctx, time.Unix(0, 0), nil); err != nil {
		t.Fatalf("open incident: %v", err)
	}
	effect, ok := controller.PendingDispatchEffect()
	if !ok {
		t.Fatal("expected pending dispatch")
	}
	if err := controller.ExecuteDispatchEffect(ctx, effect); !errors.Is(err, ErrEffectNotDurable) {
		t.Fatalf("dispatch before persistence: got %v, want ErrEffectNotDurable", err)
	}
	if jobs, _ := client.BatchV1().Jobs("demo").List(ctx, metav1.ListOptions{}); len(jobs.Items) != 0 {
		t.Fatalf("responder Job created before its dispatch record was durable: %d", len(jobs.Items))
	}

	if err := controller.PersistState(ctx); err != nil {
		t.Fatalf("persist dispatch: %v", err)
	}
	if err := executePendingEffects(ctx, controller); err != nil {
		t.Fatalf("execute persisted dispatch: %v", err)
	}
	select {
	case err := <-observed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("responder Job was never created")
	}
	state, err := durableState(client)
	if err != nil {
		t.Fatal(err)
	}
	requestMap, err := client.CoreV1().ConfigMaps("demo").Get(ctx, IncidentJobName(state.IncidentRequest.IncidentId)+"-request", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if requestMap.Data[jobRequestDataKey] != state.IncidentRequest.PreparedAgentTask {
		t.Fatal("agent request differs from durable task")
	}
}

func TestDispatchRequestChangedAfterPersistenceIsNotLaunched(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	controller := durabilityController(t, client, testControllerConfig(), dispatcher)
	if err := controller.Step(ctx, time.Unix(0, 0), nil); err != nil {
		t.Fatalf("open incident: %v", err)
	}
	if err := controller.PersistState(ctx); err != nil {
		t.Fatalf("persist dispatch: %v", err)
	}
	// Evidence attached before launch changes the request the responder
	// would receive; that version must be durable before it launches.
	if !controller.attachBeforeLaunch([]sdk.Finding{stateFinding("corroborating")}) {
		t.Fatal("expected pre-launch attachment")
	}
	effect, ok := controller.PendingDispatchEffect()
	if !ok {
		t.Fatal("expected pending dispatch")
	}
	if err := controller.ExecuteDispatchEffect(ctx, effect); !errors.Is(err, ErrEffectNotDurable) {
		t.Fatalf("launch of an unpersisted request: got %v, want ErrEffectNotDurable", err)
	}
	assertNoRequest(t, dispatcher.requests)
	if err := controller.PersistState(ctx); err != nil {
		t.Fatalf("persist attached evidence: %v", err)
	}
	if err := controller.ExecuteDispatchEffect(ctx, effect); err != nil {
		t.Fatalf("launch persisted request: %v", err)
	}
	if request := awaitRequest(t, dispatcher.requests); len(request.Findings) != 2 {
		t.Fatalf("responder did not receive the durable request: %#v", request.Findings)
	}
}

func TestFailedStateWriteBlocksDispatch(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	controller := durabilityController(t, client, testControllerConfig(), dispatcher)
	failWrite := true
	client.PrependReactor("create", "configmaps", func(k8stesting.Action) (bool, k8sruntime.Object, error) {
		if failWrite {
			return true, nil, errors.New("API server unavailable")
		}
		return false, nil, nil
	})
	if err := controller.Step(ctx, time.Unix(0, 0), nil); err != nil {
		t.Fatalf("open incident: %v", err)
	}
	if err := controller.PersistState(ctx); err == nil {
		t.Fatal("failed state write reported success")
	}
	effect, ok := controller.PendingDispatchEffect()
	if !ok {
		t.Fatal("expected pending dispatch")
	}
	if err := controller.ExecuteDispatchEffect(ctx, effect); !errors.Is(err, ErrEffectNotDurable) {
		t.Fatalf("dispatch after failed write: got %v, want ErrEffectNotDurable", err)
	}
	assertNoRequest(t, dispatcher.requests)
	if err := executePendingEffects(ctx, controller); err != nil {
		t.Fatalf("failed persistence stopped controller effects loop: %v", err)
	}
	assertNoRequest(t, dispatcher.requests)
	failWrite = false
	if err := controller.PersistState(ctx); err != nil {
		t.Fatalf("retry state write: %v", err)
	}
	if err := executePendingEffects(ctx, controller); err != nil {
		t.Fatalf("dispatch after successful retry: %v", err)
	}
	awaitRequest(t, dispatcher.requests)
}

func TestUnchangedPersistMakesNoAPICallsAndKeepsDispatchDurable(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	controller := durabilityController(t, client, testControllerConfig(), dispatcher)
	if err := controller.Step(ctx, time.Unix(0, 0), nil); err != nil {
		t.Fatalf("open incident: %v", err)
	}
	if err := controller.PersistState(ctx); err != nil {
		t.Fatalf("persist dispatch: %v", err)
	}
	client.ClearActions()
	if err := controller.PersistState(ctx); err != nil {
		t.Fatalf("persist unchanged state: %v", err)
	}
	if got := verbs(client); len(got) != 0 {
		t.Fatalf("unchanged state made API calls: %v", got)
	}
	executePendingEffect(t, controller)
	awaitRequest(t, dispatcher.requests)
}

// stateCheckingBroker asserts, at each broker side effect, what a restarted
// controller would recover from durable state.
type stateCheckingBroker struct {
	client    *fake.Clientset
	receipt   ClosureReceipt
	workspace IncidentWorkspace
	observed  chan string
}

func (b *stateCheckingBroker) report(operation string, ok bool, state *contracts.ControllerState, err error) {
	if err != nil || !ok {
		b.observed <- fmt.Sprintf("%s without durable record: err=%v state=%#v", operation, err, state)
		return
	}
	b.observed <- operation
}

func (b *stateCheckingBroker) PrepareIncident(_ context.Context, incidentID string) (IncidentWorkspace, error) {
	state, err := durableState(b.client)
	b.report("prepare", err == nil && state.IncidentOpen && state.DispatchState == contracts.DispatchState_DISPATCH_STATE_WORKSPACE_PENDING &&
		state.IncidentRequest != nil && state.IncidentRequest.IncidentId == incidentID, state, err)
	workspace := b.workspace
	workspace.IncidentID = incidentID
	return workspace, nil
}

func (b *stateCheckingBroker) ProcessClosure(_ context.Context, closure IncidentClosure) (ClosureReceipt, error) {
	state, err := durableState(b.client)
	b.report("process", err == nil && state.ClosureState == contracts.ClosureState_CLOSURE_STATE_PENDING && state.PendingClosure != nil &&
		state.PendingClosure.Request.IncidentId == closure.Request.IncidentID && !state.IncidentOpen, state, err)
	receipt := b.receipt
	receipt.IncidentID = closure.Request.IncidentID
	return receipt, nil
}

func (b *stateCheckingBroker) AcknowledgeClosure(_ context.Context, receipt ClosureReceipt) error {
	state, err := durableState(b.client)
	b.report("ack", err == nil && state.ClosureState == contracts.ClosureState_CLOSURE_STATE_COMMITTED && state.ClosureReceipt != nil &&
		state.ClosureReceipt.AckToken == receipt.AckToken, state, err)
	return nil
}

func awaitObservation(t *testing.T, observed <-chan string, want string) {
	t.Helper()
	select {
	case got := <-observed:
		if got != want {
			t.Fatal(got)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for broker %s", want)
	}
}

func TestIncidentAndWorktreeAreDurableBeforeBrokerAndResponderEffects(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	controller := durabilityController(t, client, testControllerConfig(), dispatcher)
	broker := &stateCheckingBroker{
		client: client, observed: make(chan string, 4),
		workspace: IncidentWorkspace{Worktree: "/worktrees/incident", BaseCommit: "base-sha"},
	}
	if err := controller.SetIncidentBroker(broker); err != nil {
		t.Fatalf("set broker: %v", err)
	}
	if err := controller.Step(ctx, time.Unix(0, 0), nil); err != nil {
		t.Fatalf("open incident: %v", err)
	}
	workspace, ok := controller.PendingWorkspaceEffect()
	if !ok {
		t.Fatal("expected workspace effect")
	}
	if err := controller.ExecuteWorkspaceEffect(ctx, workspace); !errors.Is(err, ErrEffectNotDurable) {
		t.Fatalf("worktree preparation before incident open was durable: got %v", err)
	}
	if err := controller.PersistState(ctx); err != nil {
		t.Fatalf("persist incident open: %v", err)
	}
	if err := executePendingEffects(ctx, controller); err != nil {
		t.Fatalf("execute workspace effect: %v", err)
	}
	awaitObservation(t, broker.observed, "prepare")
	awaitBrokerResult(t, controller.workspaceResults)
	controller.processBrokerCompletions()

	// The prepared worktree is part of the responder request; it must be
	// durable before the responder launches into it.
	dispatch, ok := controller.PendingDispatchEffect()
	if !ok || dispatch.Request.RepositoryWorktree != "/worktrees/incident" {
		t.Fatalf("expected dispatch into the prepared worktree: %#v", dispatch)
	}
	if err := controller.ExecuteDispatchEffect(ctx, dispatch); !errors.Is(err, ErrEffectNotDurable) {
		t.Fatalf("dispatch before worktree assignment was durable: got %v", err)
	}
	if err := controller.PersistState(ctx); err != nil {
		t.Fatalf("persist worktree assignment: %v", err)
	}
	if err := executePendingEffects(ctx, controller); err != nil {
		t.Fatalf("execute dispatch: %v", err)
	}
	if request := awaitRequest(t, dispatcher.requests); request.RepositoryWorktree != "/worktrees/incident" {
		t.Fatalf("responder launched outside the durable worktree: %q", request.RepositoryWorktree)
	}
}

func TestIncidentClosureAndReceiptAreDurableBeforeBrokerCommitAndAcknowledgment(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	controller := durabilityController(
		t, client, testControllerConfig(), &recordingDispatcher{requests: make(chan IncidentRequest, 1)},
	)
	broker := &stateCheckingBroker{client: client, observed: make(chan string, 4), receipt: ClosureReceipt{
		Worktree: "/worktrees/incident", BaseCommit: "base", ProposalCommit: "repair",
		OutcomeCommit: "outcome", AckToken: "token",
	}}
	if err := controller.SetIncidentBroker(broker); err != nil {
		t.Fatalf("set broker: %v", err)
	}
	start := time.Unix(0, 0)
	request := controller.incidentRequest(start, []sdk.Finding{stateFinding("fault")})
	request.DetectorHistory = testDetectorHistory(start)
	controller.mu.Lock()
	controller.pendingClosure = &IncidentClosure{
		Request: request, Result: cloneIncidentResultPointer(completedResult(request.IncidentID)),
		FinalDetectorStates: []DetectorEvaluation{{DetectorID: "health", Status: DetectorEvaluationClear}},
		DetectedAt:          start, DispatchedAt: start, ResponderCompletedAt: start, VerifiedAt: start,
	}
	controller.closureState = contracts.ClosureState_CLOSURE_STATE_PENDING
	controller.mu.Unlock()

	closure, ok := controller.PendingClosureEffect()
	if !ok {
		t.Fatal("expected closure effect")
	}
	if err := controller.ExecuteClosureEffect(ctx, closure); !errors.Is(err, ErrEffectNotDurable) {
		t.Fatalf("closure committed before incident close was durable: got %v", err)
	}
	if err := executePausedEffects(ctx, controller); err != nil || len(broker.observed) != 0 {
		t.Fatalf("paused controller stopped or committed undurable closure: %v", err)
	}
	if err := controller.PersistState(ctx); err != nil {
		t.Fatalf("persist incident close: %v", err)
	}
	if err := executePendingEffects(ctx, controller); err != nil {
		t.Fatalf("execute closure: %v", err)
	}
	awaitObservation(t, broker.observed, "process")
	awaitBrokerResult(t, controller.closureResults)
	controller.processBrokerCompletions()

	ack, ok := controller.PendingClosureAcknowledgmentEffect()
	if !ok {
		t.Fatal("expected acknowledgment effect")
	}
	if err := controller.ExecuteClosureAcknowledgmentEffect(ctx, ack); !errors.Is(err, ErrEffectNotDurable) {
		t.Fatalf("closure acknowledged before its receipt was durable: got %v", err)
	}
	if err := executePausedEffects(ctx, controller); err != nil || len(broker.observed) != 0 {
		t.Fatalf("paused controller stopped or acknowledged undurable receipt: %v", err)
	}
	if err := controller.PersistState(ctx); err != nil {
		t.Fatalf("persist closure receipt: %v", err)
	}
	if err := executePendingEffects(ctx, controller); err != nil {
		t.Fatalf("execute acknowledgment: %v", err)
	}
	awaitObservation(t, broker.observed, "ack")
	awaitBrokerResult(t, controller.acknowledgmentResults)
	controller.processBrokerCompletions()
	if err := controller.PersistState(ctx); err != nil {
		t.Fatalf("persist acknowledgment: %v", err)
	}
	state, err := durableState(client)
	if err != nil || state.LastAcknowledgedIncidentId != request.IncidentID || state.PendingClosure != nil {
		t.Fatalf("acknowledged closure is not durable: err=%v state=%#v", err, state)
	}
}
