package runtime

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

var dataRiskOpened = time.Unix(10_000, 0).UTC()

func dataPod(name string, created time.Time, volumes []corev1.Volume, started time.Time) corev1.Pod {
	yes := true
	mounts := []corev1.VolumeMount{}
	for _, volume := range volumes {
		mounts = append(mounts, corev1.VolumeMount{Name: volume.Name, MountPath: "/" + volume.Name})
	}
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app", CreationTimestamp: metav1.NewTime(created), OwnerReferences: []metav1.OwnerReference{{Kind: "StatefulSet", Name: "store", Controller: &yes}}},
		Spec:       corev1.PodSpec{Volumes: volumes, Containers: []corev1.Container{{Name: "store", VolumeMounts: mounts}}},
		Status:     corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: "store", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(started)}}}}},
	}
}

// riskSnapshots serves one fixed cluster view to the closure recheck.
type riskSnapshots struct{ view sdktest.Snapshot }

func (s riskSnapshots) Snapshot(context.Context) (sdk.DetectionContext, error) { return s.view, nil }

// A repair that replaced a volume-less data store is health-clear. What the
// replacement did to its data is no verdict: the closure label is the recovery
// verdict's alone, whether or not the map marks the store holds_data.
func TestReplacedDataPodWithoutClaimLeavesClosureLabelToRecovery(t *testing.T) {
	ctx := context.Background()
	for name, test := range map[string]struct {
		document string
		early    bool
	}{
		"marked store replaced":               {holdsDataArchitecture, false},
		"verified while the responder worked": {holdsDataArchitecture, true},
		"no map":                              {"", false},
	} {
		t.Run(name, func(t *testing.T) {
			after := dataRiskOpened.Add(time.Minute)
			cfg := testControllerConfig()
			cfg.Namespace = "app"
			snapshots := riskSnapshots{view: sdktest.Snapshot{NamespaceName: "app", PodList: []corev1.Pod{dataPod("store-0", after, nil, after)}}}
			world := &sameWriteWorld{}
			world.healthy.Store(true)
			controller, err := NewController(cfg, []sdk.Detector{sameWriteDetector{world: world}}, snapshots, &recordingDispatcher{}, after)
			if err != nil {
				t.Fatal(err)
			}
			controller.now = func() time.Time { return after.Add(time.Minute) }
			controller.gitRead = fakeArchitecture(test.document)
			controller.broker = &riskBroker{}
			result := completedResult("synthetic-incident")
			result.Timing = TimingMetrics{StartedAt: dataRiskOpened, CompletedAt: after}
			request := IncidentRequest{SchemaVersion: ProtocolSchemaVersion, Application: "demo", Namespace: "app", IncidentID: "synthetic-incident", SourceCommit: "synthetic-accepted", RepositoryWorktree: "synthetic-worktree"}
			controller.pendingClosure = &IncidentClosure{Request: request, Result: &result, DetectedAt: dataRiskOpened, DispatchedAt: dataRiskOpened, ResponderCompletedAt: after, VerifiedAt: after, VerifiedWhileResponderRunning: test.early,
				FinalDetectorStates: []DetectorEvaluation{{DetectorID: "traffic", EvaluatedAt: after, Status: DetectorEvaluationClear}}}
			controller.closureState = contracts.ClosureState_CLOSURE_STATE_PENDING
			effect, ok := controller.PendingClosureEffect()
			if !ok || !effect.Freeze {
				t.Fatalf("want a freeze effect, got %+v ok=%v", effect, ok)
			}
			if err := controller.ExecuteClosureEffect(ctx, effect); err != nil {
				t.Fatal(err)
			}
			pending := controller.pendingClosure
			if pending == nil {
				t.Fatal("closure lost")
			}
			if pending.UnconfirmedReason != contracts.ClosureUnconfirmedReason_CLOSURE_UNCONFIRMED_REASON_UNSPECIFIED || len(pending.FinalDetectorStates) == 0 {
				t.Fatalf("the replaced Pod must not decide the label: reason %s (detail %q)", pending.UnconfirmedReason, pending.UnconfirmedDetail)
			}
			derived, err := ProtoOutcomeDeriver{}.Derive(ctx, *pending, "", "synthetic", "synthetic")
			if err != nil {
				t.Fatal(err)
			}
			if derived.Record.GetClassification() == "unconfirmed" || validate.DataLossOutcome(derived.Record) {
				t.Fatalf("the outcome must carry the recovery verdict, not a data-loss rule: %+v", derived.Record)
			}
		})
	}
}

// riskBroker stands in for the native broker; the test only reads the closure.
type riskBroker struct {
	mu        sync.Mutex
	processed []IncidentClosure
}

func (b *riskBroker) closures() []IncidentClosure {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]IncidentClosure(nil), b.processed...)
}

func (b *riskBroker) PrepareIncident(_ context.Context, id string) (IncidentWorkspace, error) {
	return IncidentWorkspace{IncidentID: id, Worktree: "synthetic-worktree", BaseCommit: "synthetic-accepted"}, nil
}

func (b *riskBroker) ProcessClosure(_ context.Context, closure IncidentClosure) (ClosureReceipt, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.processed = append(b.processed, closure)
	return ClosureReceipt{IncidentID: closure.Request.IncidentID}, nil
}

func (b *riskBroker) AcknowledgeClosure(context.Context, ClosureReceipt) error { return nil }

// restartedStore is a Deployment-run store with only a config volume. Restart
// replaces its Pod, the way a responder's hand repair does.
type restartedStore struct {
	mu   sync.Mutex
	now  time.Time
	view sdktest.Snapshot
}

func newRestartedStore(now time.Time) *restartedStore {
	yes, replicas := true, int32(1)
	store := &restartedStore{now: now, view: sdktest.Snapshot{NamespaceName: "app",
		DeploymentList: []appsv1.Deployment{{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app", Generation: 1}, Spec: appsv1.DeploymentSpec{Replicas: &replicas},
			Status: appsv1.DeploymentStatus{ObservedGeneration: 1, Replicas: 1, UpdatedReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1}}},
		ReplicaSetList: []appsv1.ReplicaSet{{ObjectMeta: metav1.ObjectMeta{Name: "api-1", Namespace: "app", OwnerReferences: []metav1.OwnerReference{{Kind: "Deployment", Name: "api", Controller: &yes}}}}},
	}}
	store.replacePod("api-1-old", now.Add(-time.Hour))
	return store
}

func (s *restartedStore) replacePod(name string, created time.Time) {
	yes := true
	config := corev1.Volume{Name: "config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{}}}
	pod := dataPod(name, created, []corev1.Volume{config}, created)
	pod.OwnerReferences = []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "api-1", Controller: &yes}}
	s.view.PodList = []corev1.Pod{pod}
}

func (s *restartedStore) Snapshot(context.Context) (sdk.DetectionContext, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.view, nil
}

// handRestartDispatcher is a responder that restarts the store and reports
// success. The request it keeps is the one production built for this incident.
type handRestartDispatcher struct {
	store   *restartedStore
	world   *sameWriteWorld
	mu      sync.Mutex
	request IncidentRequest
}

func (d *handRestartDispatcher) Dispatch(_ context.Context, request IncidentRequest) (IncidentResult, error) {
	d.store.mu.Lock()
	d.store.replacePod("api-1-new", d.store.now)
	d.store.mu.Unlock()
	d.world.healthy.Store(true)
	d.mu.Lock()
	d.request = request
	d.mu.Unlock()
	return completedResult(request.IncidentID), nil
}

// A namespace SDO onboarded carries its onboarding record on every incident
// request. A responder restarts the marked store by hand and health clears:
// the replaced Pod decides nothing, and the label is the recovery verdict's.
func TestResponderRestartOfDataStoreInOnboardedNamespaceLeavesLabelToRecovery(t *testing.T) {
	runOnOneProcessor(t)
	ctx := context.Background()
	opened := dataRiskOpened
	store := newRestartedStore(opened)
	world := &sameWriteWorld{}
	dispatcher := &handRestartDispatcher{store: store, world: world}
	cfg := testControllerConfig()
	cfg.Namespace, cfg.SourceCommit, cfg.RepositoryWorktree = "app", "synthetic-accepted", "synthetic-worktree"
	cfg.FiringThreshold, cfg.ClearThreshold = 1, 1
	cfg.ClosureStabilityWindow = time.Second
	controller, err := NewController(cfg, []sdk.Detector{sameWriteDetector{world: world}}, store, dispatcher, opened)
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.SetNamespaceOnboarding(&contracts.NamespaceOnboarding{Namespace: "app", AcceptedCommit: strings.Repeat("a", 40), EstablishedAt: timestamppb.New(opened.Add(-time.Hour))}); err != nil {
		t.Fatal(err)
	}
	now := opened
	controller.now = func() time.Time {
		store.mu.Lock()
		defer store.mu.Unlock()
		return store.now
	}
	controller.gitRead = fakeArchitecture(holdsDataArchitecture)
	broker := &riskBroker{}
	controller.broker = broker
	for step := 0; step < 200 && len(broker.closures()) == 0; step++ {
		if err := controller.Step(ctx, now, nil); err != nil {
			t.Fatal(err)
		}
		if effect, ok := controller.PendingWorkspaceEffect(); ok {
			if err := controller.ExecuteWorkspaceEffect(ctx, effect); err != nil {
				t.Fatal(err)
			}
			completion := <-controller.workspaceResults
			controller.workspaceResults <- completion
		} else if effect, ok := controller.PendingDispatchEffect(); ok {
			if err := controller.ExecuteDispatchEffect(ctx, effect); err != nil {
				t.Fatal(err)
			}
			awaitDispatchCompletionQueued(t, controller)
		} else if effect, ok := controller.PendingClosureEffect(); ok {
			if err := controller.ExecuteClosureEffect(ctx, effect); err != nil {
				t.Fatal(err)
			}
			awaitEffectGoroutines(controller)
		}
		now = now.Add(time.Second)
		store.mu.Lock()
		store.now = now
		store.mu.Unlock()
	}
	closures := broker.closures()
	if len(closures) == 0 {
		t.Fatalf("the incident never reached the broker (dispatch state %s)", controller.dispatchState)
	}
	dispatcher.mu.Lock()
	request := dispatcher.request
	dispatcher.mu.Unlock()
	if request.NamespaceOnboarding == nil || request.DetectedAt.IsZero() {
		t.Fatalf("the responder's request must look like production's: %+v", request)
	}
	closure := closures[0]
	if closure.UnconfirmedReason == contracts.ClosureUnconfirmedReason_CLOSURE_UNCONFIRMED_REASON_DATA_MAY_BE_LOST {
		t.Fatalf("reason = %s, detail %q: a replaced Pod is no verdict", closure.UnconfirmedReason, closure.UnconfirmedDetail)
	}
	derived, err := ProtoOutcomeDeriver{}.Derive(ctx, closure, "", "synthetic", "synthetic")
	if err != nil {
		t.Fatal(err)
	}
	if validate.DataLossOutcome(derived.Record) {
		t.Fatalf("outcome = %+v", derived.Record)
	}
}

// A historic outcome a data-at-risk closure wrote still reads the same: it
// holds no verified repair and gives reflection nothing to learn.
func TestDataLossOutcomeGivesReflectionNoVerifiedRepair(t *testing.T) {
	at := timestamppb.New(dataRiskOpened)
	ref := &contracts.ObjectRef{ApiVersion: "apps/v1", Kind: "StatefulSet", Namespace: "app", Name: "store"}
	outcome := &contracts.OutcomeRecord{
		Classification:      "unconfirmed",
		DetectorHistory:     []*contracts.DetectorEvaluation{{DetectorId: validate.DataAtRiskDetector, EvaluatedAt: at, Status: string(DetectorEvaluationPending), Reason: proto.String("StatefulSet/store has no persistent storage and its Pods were replaced")}},
		ConfirmedRootCauses: []*contracts.ConfirmedRootCause{{Summary: "store stalled", Resources: []*contracts.ObjectRef{ref}}},
		RepairActions:       []*contracts.RepairActionReceipt{{Success: true, Resources: []*contracts.ObjectRef{ref}, StartedAt: at, CompletedAt: at}},
	}
	if got := partialRepairCauses(outcome); len(got) != 0 {
		t.Fatalf("a repair that may have lost data is no verified cause: %v", got)
	}
	if reflectionRepairVerified(outcome) {
		t.Fatal("reflection must not publish learning from a repair that may have lost data")
	}
}

// A responder's opaque command inside a data store may have changed its
// records. SDO records that data effect on the outcome and in verify, and the
// closure stays verified: a data effect is evidence, not a verdict. A typed
// store-setting change is recorded with its class;
// a command that reaches no data records nothing.
func TestClosureRecordsDataEffectsWithoutUnconfirming(t *testing.T) {
	ctx := context.Background()
	storePod, webPod := holdsDataRef("v1", "Pod", "store-0"), holdsDataRef("v1", "Pod", "web-1-a")
	opaque := func(pod sdk.ObjectRef) program.Action {
		return program.EnsureExec{Observe: program.Command{Pod: pod, Container: "c", Args: []string{"true"}}, Ensure: program.Command{Pod: pod, Container: "c", Args: []string{"flush"}}}
	}
	user := program.EnsureDatabaseUser{Target: program.DatabaseTarget{Engine: program.MongoDB, Pod: storePod, Container: "store", Database: "app"}, User: "app", Roles: []program.DatabaseRole{{Database: "app", Name: "readWrite"}}, Password: program.CredentialRef{Env: "PASSWORD"}}
	for name, test := range map[string]struct {
		action program.Action
		want   []string
	}{
		"opaque command in the store": {opaque(storePod), []string{"StatefulSet/store", "effect unknown", "no backup"}},
		"store setting":               {user, []string{"StatefulSet/store", "effect store-setting"}},
		"opaque command elsewhere":    {opaque(webPod), nil},
	} {
		t.Run(name, func(t *testing.T) {
			after := dataRiskOpened.Add(time.Minute)
			cfg := testControllerConfig()
			cfg.Namespace = "app"
			world := &sameWriteWorld{}
			world.healthy.Store(true)
			controller, err := NewController(cfg, []sdk.Detector{sameWriteDetector{world: world}}, riskSnapshots{view: holdsDataView()}, &recordingDispatcher{}, after)
			if err != nil {
				t.Fatal(err)
			}
			controller.now = func() time.Time { return after.Add(time.Minute) }
			controller.gitRead = fakeArchitecture(holdsDataArchitecture)
			controller.broker = &riskBroker{}
			result := completedResult("synthetic-incident")
			result.Timing = TimingMetrics{StartedAt: dataRiskOpened, CompletedAt: after}
			result.PlanApplications = []RecordedPlanApplication{{ID: "synthetic-application", StartedAt: after, CompletedAt: after,
				Result: PlanApplicationResult{Plan: program.Plan{Actions: []program.Action{test.action}}, Applied: true, VerificationCompleted: true, VerificationPassed: true, ActionsApplied: []program.Action{test.action}}}}
			request := IncidentRequest{SchemaVersion: ProtocolSchemaVersion, Application: "demo", Namespace: "app", IncidentID: "synthetic-incident", SourceCommit: "synthetic-accepted", RepositoryWorktree: "synthetic-worktree"}
			controller.pendingClosure = &IncidentClosure{Request: request, Result: &result, DetectedAt: dataRiskOpened, DispatchedAt: dataRiskOpened, ResponderCompletedAt: after, VerifiedAt: after,
				FinalDetectorStates: []DetectorEvaluation{{DetectorID: "traffic", EvaluatedAt: after, Status: DetectorEvaluationClear}}}
			controller.closureState = contracts.ClosureState_CLOSURE_STATE_PENDING
			effect, ok := controller.PendingClosureEffect()
			if !ok || !effect.Freeze {
				t.Fatalf("want a freeze effect, got %+v ok=%v", effect, ok)
			}
			if err := controller.ExecuteClosureEffect(ctx, effect); err != nil {
				t.Fatal(err)
			}
			pending := controller.pendingClosure
			if pending == nil || pending.UnconfirmedReason != contracts.ClosureUnconfirmedReason_CLOSURE_UNCONFIRMED_REASON_UNSPECIFIED {
				t.Fatalf("closure = %+v, want it verified: a data effect is evidence, not a verdict", pending)
			}
			requireDataEffectRecord(t, "closure notes", pending.SourceCheckNotes, test.want)

			_, loop, _ := sameWriteLoop(t)
			loop.Request.Namespace, loop.Request.DetectedAt = "app", dataRiskOpened
			loop.Request.SourceCommit, loop.Request.RepositoryWorktree = "synthetic-accepted", "synthetic-worktree"
			loop.Provider, loop.GitRead = riskSnapshots{view: holdsDataView()}, fakeArchitecture(holdsDataArchitecture)
			loop.applicationsLog = result.PlanApplications
			response := loop.Handle(ctx, ToolRequest{Command: "verify"})
			requireDataEffectRecord(t, "verify data_effects", response.DataEffects, test.want)
		})
	}
}

// requireDataEffectRecord holds when records carry exactly one data-effect
// line containing every want, or none when want is empty.
func requireDataEffectRecord(t *testing.T, where string, records []string, want []string) {
	t.Helper()
	var found []string
	for _, record := range records {
		if strings.HasPrefix(record, "data effect:") {
			found = append(found, record)
		}
	}
	if len(want) == 0 {
		if len(found) != 0 {
			t.Fatalf("%s = %v, want no data effect", where, found)
		}
		return
	}
	if len(found) != 1 {
		t.Fatalf("%s = %v, want one data effect", where, records)
	}
	for _, part := range want {
		if !strings.Contains(found[0], part) {
			t.Fatalf("%s = %q, want it to name %q", where, found[0], part)
		}
	}
}
