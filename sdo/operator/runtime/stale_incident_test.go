package runtime

import (
	"context"
"os"
"path/filepath"
	"errors"
	"fmt"
	"google.golang.org/protobuf/proto"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"math/rand"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"strings"
	"testing"
	"time"
)

func TestExpiredIncidentWithoutLiveWorkIsArchivedBeforeFreshDispatch(t *testing.T) {
	ctx := context.Background()
	start := time.Unix(1000, 0)
	config := testControllerConfig()
	config.FiringThreshold = 1
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 2)}
	first, err := NewController(config, []sdk.Detector{controllerDetector("fault", time.Hour, stateFinding("fault"))}, staticProvider{snapshot: sdktest.Snapshot{}}, dispatcher, start)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Step(ctx, start, nil); err != nil {
		t.Fatal(err)
	}
	state := exportState(t, first)
	oldID := state.GetIncidentRequest().GetIncidentId()
	now := state.GetIncidentRequest().GetResponseDeadline().AsTime().Add(time.Second)
	restored, err := NewController(config, []sdk.Detector{controllerDetector("fault", time.Hour, stateFinding("fault"))}, staticProvider{snapshot: sdktest.Snapshot{}}, dispatcher, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.RestoreState(state); err != nil {
		t.Fatal(err)
	}
	if err := restored.Step(ctx, now, nil); err != nil {
		t.Fatal(err)
	}
	got := exportState(t, restored)
	if len(got.GetFailedClosures()) != 1 || got.GetFailedClosures()[0].GetFailure().GetReason() != contracts.ClosureFailureReason_CLOSURE_FAILURE_REASON_AGENT_TIMEOUT {
		t.Fatalf("expired incident has no typed terminal record: %v", got)
	}
	if got.GetIncidentRequest().GetIncidentId() == oldID {
		t.Fatal("resumed expired incident")
	}
	executePendingEffect(t, restored)
	request := awaitRequest(t, dispatcher.requests)
	if request.IncidentID == oldID {
		t.Fatal("redispatched expired incident")
	}
}

func TestExpiredIncidentWorkStatusIsConservative(t *testing.T) {
	for _, tc := range []struct {
		name     string
		job      *batchv1.Job
		active   bool
		apiError bool
	}{
		{name: "absent"},
		{name: "pending", job: &batchv1.Job{}, active: true},
		{name: "running", job: &batchv1.Job{Status: batchv1.JobStatus{Active: 1}}, active: true},
		{name: "deleting", job: &batchv1.Job{ObjectMeta: metav1.ObjectMeta{DeletionTimestamp: &metav1.Time{Time: time.Now()}}}, active: true},
		{name: "complete", job: &batchv1.Job{Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}}}},
		{name: "failed", job: &batchv1.Job{Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}}}},
		{name: "api-uncertain", apiError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := fake.NewSimpleClientset()
			if tc.job != nil {
				tc.job.Name = IncidentJobName("stale")
				tc.job.Namespace = "control"
				if _, err := client.BatchV1().Jobs("control").Create(context.Background(), tc.job, metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			if tc.apiError {
				client.PrependReactor("get", "jobs", func(ktesting.Action) (bool, kruntime.Object, error) { return true, nil, errors.New("API unavailable") })
			}
			active, err := (KubernetesJobDispatcher{Client: client, Namespace: "control"}).IncidentWorkActive(context.Background(), "stale")
			if active != tc.active || (err != nil) != tc.apiError {
				t.Fatalf("active=%v err=%v", active, err)
			}
		})
	}
}

type liveIncidentDispatcher struct {
	*recordingDispatcher
	active bool
	err    error
}

func (d liveIncidentDispatcher) IncidentWorkActive(context.Context, string) (bool, error) {
	return d.active, d.err
}

func TestExpiredIncidentRetainsLiveWorkAndUncertainBindings(t *testing.T) {
	for _, tc := range []struct {
		name   string
		active bool
		err    error
	}{
		{name: "live", active: true}, {name: "uncertain", err: errors.New("API unavailable")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			start := time.Unix(1000, 0)
			config := testControllerConfig()
			config.FiringThreshold = 1
			dispatcher := liveIncidentDispatcher{recordingDispatcher: &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, active: tc.active, err: tc.err}
			controller, err := NewController(config, []sdk.Detector{controllerDetector("fault", time.Second, stateFinding("fault"))}, staticProvider{snapshot: sdktest.Snapshot{}}, dispatcher, start)
			if err != nil {
				t.Fatal(err)
			}
			if err := controller.Step(context.Background(), start, nil); err != nil {
				t.Fatal(err)
			}
			state := exportState(t, controller)
			oldID := state.IncidentRequest.IncidentId
			now := state.IncidentRequest.ResponseDeadline.AsTime().Add(time.Second)
			err = controller.Step(context.Background(), now, nil)
			if (err != nil) != (tc.err != nil) {
				t.Fatalf("uncertainty not surfaced: %v", err)
			}
			got := exportState(t, controller)
			if !got.IncidentOpen || got.IncidentRequest.IncidentId != oldID || len(got.FailedClosures) != 0 {
				t.Fatalf("live or uncertain binding discarded: %v", got)
			}
		})
	}
}

func TestExpiredIncidentFailureArchiveSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	start := time.Unix(1000, 0)
	config := testControllerConfig()
	config.FiringThreshold = 1
	detector := controllerDetector("fault", time.Second, stateFinding("fault"))
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	controller, err := NewController(config, []sdk.Detector{detector}, staticProvider{snapshot: sdktest.Snapshot{}}, dispatcher, start)
	if err != nil {
		t.Fatal(err)
	}
	client := fake.NewSimpleClientset()
	store := lifecycleio.NewConfigMapStateStore(client, "control", "sdo-controller-state")
	if err := controller.AttachStateStore(ctx, store); err != nil {
		t.Fatal(err)
	}
	if err := controller.Step(ctx, start, nil); err != nil {
		t.Fatal(err)
	}
	old := exportState(t, controller).IncidentRequest
	if err := controller.PersistState(ctx); err != nil {
		t.Fatal(err)
	}
	now := old.ResponseDeadline.AsTime().Add(time.Second)
	if err := controller.Step(ctx, now, nil); err != nil {
		t.Fatal(err)
	}
	if err := controller.PersistState(ctx); err != nil {
		t.Fatal(err)
	}
	maps, err := client.CoreV1().ConfigMaps("control").List(ctx, metav1.ListOptions{LabelSelector: "sdo.dev/closure-archive=failed"})
	if err != nil || len(maps.Items) != 1 {
		t.Fatalf("failure not archived: %v %v", maps, err)
	}
	fresh, err := NewController(config, []sdk.Detector{controllerDetector("fault", time.Second, stateFinding("fault"))}, staticProvider{snapshot: sdktest.Snapshot{}}, dispatcher, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := fresh.AttachStateStore(ctx, store); err != nil {
		t.Fatal(err)
	}
	request := exportState(t, fresh).IncidentRequest
	if request.IncidentId == old.IncidentId {
		t.Fatal("restart resumed expired incident")
	}
	executePendingEffect(t, fresh)
	if got := awaitRequest(t, dispatcher.requests); got.IncidentID != request.IncidentId {
		t.Fatalf("wrong fresh incident: %v", got)
	}
}

func TestExpiredIncidentKeepsOrphanWorkerPod(t *testing.T) {
	client := fake.NewSimpleClientset(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "orphan", Namespace: "control", Labels: map[string]string{"sdo.dev/incident": IncidentJobName("stale")}}, Status: corev1.PodStatus{Phase: corev1.PodRunning}})
	active, err := (KubernetesJobDispatcher{Client: client, Namespace: "control"}).IncidentWorkActive(context.Background(), "stale")
	if err != nil || !active {
		t.Fatalf("orphan worker treated as absent: active=%v err=%v", active, err)
	}
}

// Restart cuts retain the fake API and inject a lost archive write, a failed
// state update, or a state update whose success response was lost. No effect
// may redispatch the old incident on either side of the durability boundary.
func TestExpiredIncidentSeededArchiveCrashCuts(t *testing.T) {
	for seed := int64(1); seed <= 12; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			ctx := context.Background()
			start := time.Unix(1000, 0)
			config := testControllerConfig()
			config.FiringThreshold = 1
			dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
			client := fake.NewSimpleClientset()
			create := func(at time.Time) *Controller {
				controller, err := NewController(config, []sdk.Detector{controllerDetector("fault", time.Hour, stateFinding("fault"))}, staticProvider{snapshot: sdktest.Snapshot{}}, dispatcher, at)
				if err != nil {
					t.Fatal(err)
				}
				if err := controller.AttachStateStore(ctx, lifecycleio.NewConfigMapStateStore(client, "control", "sdo-controller-state")); err != nil {
					t.Fatal(err)
				}
				return controller
			}
			first := create(start)
			if err := first.Step(ctx, start, nil); err != nil {
				t.Fatal(err)
			}
			old := exportState(t, first).IncidentRequest
			if err := first.PersistState(ctx); err != nil {
				t.Fatal(err)
			}
			cut := rand.New(rand.NewSource(seed)).Intn(3)
			injected := false
			client.PrependReactor("*", "configmaps", func(action ktesting.Action) (bool, kruntime.Object, error) {
				if injected {
					return false, nil, nil
				}
				if create, ok := action.(ktesting.CreateAction); ok && cut == 0 {
					object := create.GetObject().(*corev1.ConfigMap)
					if strings.HasPrefix(object.Name, "sdo-failed-closure-") {
						injected = true
						return true, nil, errors.New("archive write failed")
					}
				}
				if update, ok := action.(ktesting.UpdateAction); ok && cut != 0 {
					object := update.GetObject().(*corev1.ConfigMap)
					if object.Name == "sdo-controller-state" {
						injected = true
						if cut == 2 {
							if err := client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), object, "control"); err != nil {
								t.Fatal(err)
							}
						}
						return true, nil, errors.New("state write response lost")
					}
				}
				return false, nil, nil
			})
			now := old.ResponseDeadline.AsTime().Add(time.Second)
			if err := first.Step(ctx, now, nil); err == nil || !injected {
				t.Fatal("fault cut did not interrupt retirement")
			}
			assertNoRequest(t, dispatcher.requests)
			// A new process sees whichever write reached the server before the cut.
			restarted := create(now)
			if err := restarted.Step(ctx, now, nil); err != nil {
				t.Fatal(err)
			}
			if err := restarted.PersistState(ctx); err != nil {
				t.Fatal(err)
			}
			executePendingEffect(t, restarted)
			if request := awaitRequest(t, dispatcher.requests); request.IncidentID == old.IncidentId {
				t.Fatal("crash resumed expired incident")
			}
			archives, err := client.CoreV1().ConfigMaps("control").List(ctx, metav1.ListOptions{LabelSelector: "sdo.dev/closure-archive=failed"})
			if err != nil || len(archives.Items) != 1 {
				t.Fatalf("lost or duplicated terminal archive: %v %v", archives, err)
			}
		})
	}
}

func TestExpiredIncidentEffectPumpRefusesDispatchWithoutAnotherEvaluation(t *testing.T) {
	ctx := context.Background()
	start := time.Unix(1000, 0)
	config := testControllerConfig()
	config.FiringThreshold = 1
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	controller, err := NewController(config, []sdk.Detector{controllerDetector("fault", time.Hour, stateFinding("fault"))}, staticProvider{snapshot: sdktest.Snapshot{}}, dispatcher, start)
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.Step(ctx, start, nil); err != nil {
		t.Fatal(err)
	}
	old := exportState(t, controller).IncidentRequest
	controller.now = func() time.Time { return old.ResponseDeadline.AsTime().Add(time.Second) }
	// Workspace and transport completions pump effects without another Step.
	if err := executePendingEffects(ctx, controller); err != nil {
		t.Fatal(err)
	}
	got := exportState(t, controller)
	if got.IncidentOpen || len(got.FailedClosures) != 1 {
		t.Fatal("effect pump resumed expired request")
	}
	assertNoRequest(t, dispatcher.requests)
}

func TestExpiredIncidentPreservesClaimedClosure(t *testing.T) {
	for _, phase := range []contracts.ClosureState{
		contracts.ClosureState_CLOSURE_STATE_PENDING,
		contracts.ClosureState_CLOSURE_STATE_PROCESSING,
		contracts.ClosureState_CLOSURE_STATE_COMMITTED,
		contracts.ClosureState_CLOSURE_STATE_ACKNOWLEDGING,
		contracts.ClosureState_CLOSURE_STATE_FAILED,
	} {
		t.Run(phase.String(), func(t *testing.T) {
			ctx := context.Background()
			start := time.Unix(1000, 0)
			config := testControllerConfig()
			config.FiringThreshold = 1
			controller, err := NewController(config, []sdk.Detector{controllerDetector("fault", time.Hour, stateFinding("fault"))}, staticProvider{snapshot: sdktest.Snapshot{}}, &recordingDispatcher{}, start)
			if err != nil {
				t.Fatal(err)
			}
			if err := controller.Step(ctx, start, nil); err != nil {
				t.Fatal(err)
			}
			request := *cloneIncidentRequest(controller.currentIncidentRequest)
			controller.pendingClosure = &IncidentClosure{Request: request, DetectedAt: start, DispatchedAt: start, ResponderCompletedAt: start, VerifiedAt: start}
			controller.closureState = phase
			if phase == contracts.ClosureState_CLOSURE_STATE_FAILED {
				controller.closureFailure = &ClosureFailure{IncidentID: request.IncidentID, Permanent: true, LastError: ErrStaleHealthInvestigation.Error(), Reason: contracts.ClosureFailureReason_CLOSURE_FAILURE_REASON_BROKER_ERROR}
			}
			before := exportState(t, controller)
			if err := controller.retireExpiredIncident(ctx, request.ResponseDeadline.Add(time.Second)); err != nil {
				t.Fatalf("expiry interfered with claimed closure: %v", err)
			}
			if after := exportState(t, controller); !proto.Equal(before, after) {
				t.Fatalf("expiry replaced a closure claimed by another path: %v", after)
			}
		})
	}
}

func TestExpiredIncidentHealthRetirementHasPriority(t *testing.T) {
	for _, entry := range []struct {
		name string
		run  func(context.Context, *Controller, time.Time) error
	}{
		{"evaluation", func(ctx context.Context, c *Controller, at time.Time) error { return c.Step(ctx, at, nil) }},
		{"effects", func(ctx context.Context, c *Controller, _ time.Time) error { return executePendingEffects(ctx, c) }},
	} {
		t.Run(entry.name, func(t *testing.T) {
			ctx := context.Background()
			start := time.Unix(1000, 0)
			config := testControllerConfig()
			config.FiringThreshold = 1
			controller, err := NewController(config, []sdk.Detector{controllerDetector("fault", time.Hour, stateFinding("fault"))}, staticProvider{snapshot: sdktest.Snapshot{}}, &recordingDispatcher{}, start)
			if err != nil {
				t.Fatal(err)
			}
			if err := controller.Step(ctx, start, nil); err != nil {
				t.Fatal(err)
			}
			now := controller.currentIncidentRequest.ResponseDeadline.Add(time.Second)
			controller.now = func() time.Time { return now }
			controller.CheckHealth = func(context.Context) error {
				if controller.currentIncidentRequest != nil {
					return ErrStaleHealthInvestigation
				}
				return nil
			}
			controller.checkInvestigationHealth = func(context.Context, string) error { return ErrStaleHealthInvestigation }
			notified := 0
			controller.OnClosureFailed = func(f ClosureFailure) {
				if f.LastError == ErrStaleHealthInvestigation.Error() {
					notified++
				}
			}
			if err := entry.run(ctx, controller, now); err != nil {
				t.Fatal(err)
			}
			if notified != 1 || controller.closureFailure == nil || controller.closureFailure.LastError != ErrStaleHealthInvestigation.Error() {
				t.Fatalf("health retirement lost ownership: notifications=%d failure=%v", notified, controller.closureFailure)
			}
		})
	}
}

func TestExpiredIncidentDoesNotInheritEarlierFailure(t *testing.T) {
	ctx := context.Background()
	start := time.Unix(1000, 0)
	config := testControllerConfig()
	config.FiringThreshold = 1
	controller, err := NewController(config, []sdk.Detector{controllerDetector("fault", time.Hour, stateFinding("fault"))}, staticProvider{snapshot: sdktest.Snapshot{}}, &recordingDispatcher{}, start)
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.Step(ctx, start, nil); err != nil {
		t.Fatal(err)
	}
	request := *cloneIncidentRequest(controller.currentIncidentRequest)
	earlier := request
	earlier.IncidentID = "earlier"
	controller.pendingClosure = &IncidentClosure{Request: earlier, DetectedAt: start, DispatchedAt: start, ResponderCompletedAt: start, VerifiedAt: start}
	controller.closureState = contracts.ClosureState_CLOSURE_STATE_FAILED
	controller.closureFailure = &ClosureFailure{IncidentID: earlier.IncidentID, Permanent: true, LastError: ErrStaleHealthInvestigation.Error(), Reason: contracts.ClosureFailureReason_CLOSURE_FAILURE_REASON_BROKER_ERROR}
	archived := &contracts.FailedIncidentClosure{Closure: mustProject(t, incidentClosureContract, *controller.pendingClosure), Failure: closureFailureContract(controller.closureFailure)}
	controller.failedClosures = []*contracts.FailedIncidentClosure{archived}
	if err := controller.retireExpiredIncident(ctx, request.ResponseDeadline.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	state := exportState(t, controller)
	if state.IncidentOpen || len(state.FailedClosures) != 2 || !proto.Equal(state.FailedClosures[0], archived) {
		t.Fatalf("earlier failure blocked or was lost during expiry: %v", state)
	}
	if failure := state.FailedClosures[1].Failure; failure.IncidentId != request.IncidentID || failure.Reason != contracts.ClosureFailureReason_CLOSURE_FAILURE_REASON_AGENT_TIMEOUT {
		t.Fatalf("current incident not expired: %v", failure)
	}
}

func TestRWSBoundStandbyCountsAsLiveWork(t *testing.T){
 root:=t.TempDir();dir:=filepath.Join(root,".sdo-runtime/standby/bindings");os.MkdirAll(dir,0700)
 data:=[]byte(`{"job_name":"sdo-standby-live","incident_id":"stale","request_digest":"digest"}`);os.WriteFile(filepath.Join(dir,"digest.json"),data,0600)
 job:=&batchv1.Job{ObjectMeta:metav1.ObjectMeta{Name:"sdo-standby-live",Namespace:"control"},Status:batchv1.JobStatus{Active:1}}
 pod:=&corev1.Pod{ObjectMeta:metav1.ObjectMeta{Name:"worker",Namespace:"control",OwnerReferences:[]metav1.OwnerReference{{Kind:"Job",Name:job.Name}}},Status:corev1.PodStatus{Phase:corev1.PodRunning}}
 client:=fake.NewSimpleClientset(job,pod);d:=KubernetesJobDispatcher{Client:client,Namespace:"control",RepositoryMountPath:root}
 active,err:=d.IncidentWorkActive(context.Background(),"stale");if err!=nil||!active{t.Fatalf("bound worker invisible: %v %v",active,err)}
 client.BatchV1().Jobs("control").Delete(context.Background(),job.Name,metav1.DeleteOptions{});active,err=d.IncidentWorkActive(context.Background(),"stale");if err!=nil||!active{t.Fatalf("orphan bound pod invisible: %v %v",active,err)}
}
