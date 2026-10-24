package runtime

import (
	"context"
	"errors"
	"fmt"
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
