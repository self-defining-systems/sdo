//go:build integration

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
	"testing/synctest"
	"time"
)

func TestExpiredIncidentWorkStatusIsConservative(t *testing.T) {
	finished := time.Unix(5000, 0).UTC()
	for _, tc := range []struct {
		name     string
		job      *batchv1.Job
		active   bool
		finished bool
		apiError bool
	}{
		{name: "absent"},
		{name: "pending", job: &batchv1.Job{}, active: true},
		{name: "running", job: &batchv1.Job{Status: batchv1.JobStatus{Active: 1}}, active: true},
		{name: "deleting", job: &batchv1.Job{ObjectMeta: metav1.ObjectMeta{DeletionTimestamp: &metav1.Time{Time: time.Now()}}}, active: true},
		{name: "complete", job: &batchv1.Job{Status: batchv1.JobStatus{CompletionTime: &metav1.Time{Time: finished}, Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}}}, finished: true},
		{name: "failed", job: &batchv1.Job{Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Time{Time: finished}}}}}, finished: true},
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
			work, err := (KubernetesJobDispatcher{Client: client, Namespace: "control"}).IncidentWork(context.Background(), "stale")
			if work.Active != tc.active || work.FinishedAt.Equal(finished) != tc.finished || (err != nil) != tc.apiError {
				t.Fatalf("work=%+v err=%v", work, err)
			}
		})
	}
}

// Restart cuts retain the fake API and inject a lost archive write, a failed
// state update, or a state update whose success response was lost. No effect
// may redispatch the old incident on either side of the durability boundary.
func TestExpiredIncidentSeededArchiveCrashCuts(t *testing.T) {
	for seed := int64(1); seed <= 12; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
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
		})
	}
}

// awaitHarvest waits until the dispatch goroutine has queued its result.
func awaitHarvest(t *testing.T, controller *Controller) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); len(controller.results) == 0; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("dispatch result never queued")
		}
	}
}

// A responder Job that finished before its deadline owns its incident until its
// result is harvested, on every path: a restart past the OWNER_LOST bound, and
// a live controller whose step sees the queued result after the bound.
func TestFinishedResponderJobIsHarvestedBeforeRetirement(t *testing.T) {
	for _, restart := range []bool{true, false} {
		t.Run(fmt.Sprintf("restart=%v", restart), func(t *testing.T) {
			ctx := context.Background()
			start := time.Unix(1000, 0)
			config := testControllerConfig()
			config.FiringThreshold = 1
			detectors := []sdk.Detector{controllerDetector("fault", time.Hour, stateFinding("fault"))}
			client := fake.NewSimpleClientset()
			dispatcher := finishedJobDispatcher{recordingDispatcher: &recordingDispatcher{requests: make(chan IncidentRequest, 2)}, work: KubernetesJobDispatcher{Client: client, Namespace: "control"}}
			first, err := NewController(config, detectors, staticProvider{snapshot: sdktest.Snapshot{}}, dispatcher, start)
			if err != nil {
				t.Fatal(err)
			}
			if err := first.Step(ctx, start, nil); err != nil {
				t.Fatal(err)
			}
			executePendingEffect(t, first)
			id := awaitRequest(t, dispatcher.requests).IncidentID
			state := exportState(t, first)
			deadline := state.GetIncidentRequest().GetResponseDeadline().AsTime()
			if _, err := client.BatchV1().Jobs("control").Create(ctx, completedResponderJob(id, deadline.Add(-30*time.Second)), metav1.CreateOptions{}); err != nil {
				t.Fatal(err)
			}
			// Past the running dispatch's OWNER_LOST bound, within the finished
			// Job's verification hold.
			now := deadline.Add(ownerLostGrace + 15*time.Second)
			controller := first
			if restart {
				controller, err = NewController(config, detectors, staticProvider{snapshot: sdktest.Snapshot{}}, dispatcher, now)
				if err != nil {
					t.Fatal(err)
				}
				controller.now = func() time.Time { return now }
				if err := controller.RestoreState(state); err != nil {
					t.Fatal(err)
				}
				// Startup order in Run: retirement, then the restored dispatch
				// effect re-attaches to the finished Job.
				if err := controller.retireExpiredIncident(ctx, now); err != nil {
					t.Fatal(err)
				}
				if err := executePendingEffects(ctx, controller); err != nil {
					t.Fatal(err)
				}
				if request := awaitRequest(t, dispatcher.requests); request.IncidentID != id {
					t.Fatalf("restart dispatched %q, want the finished incident %q", request.IncidentID, id)
				}
			}
			awaitHarvest(t, controller)
			if err := controller.Step(ctx, now, nil); err != nil {
				t.Fatal(err)
			}
			got := exportState(t, controller)
			if len(got.GetFailedClosures()) != 0 {
				t.Fatalf("finished responder retired before its result was harvested: %v", got.GetFailedClosures()[0].GetFailure())
			}
			if closure := got.GetPendingClosure(); closure != nil {
				// A late harvest can already be at the original verification
				// bound: its result is preserved in the normal frozen closure.
				if closure.GetRequest().GetIncidentId() != id || closure.GetResult().GetStatus() != string(IncidentCompleted) || got.GetClosureState() != contracts.ClosureState_CLOSURE_STATE_PENDING {
					t.Fatalf("harvested result lost from bounded closure: %v", got)
				}
			} else if got.GetIncidentRequest().GetIncidentId() != id || !got.GetResponderDone() || got.GetIncidentResult().GetStatus() != string(IncidentCompleted) {
				t.Fatalf("finished responder result not harvested: %v", got)
			}
		})
	}
}
