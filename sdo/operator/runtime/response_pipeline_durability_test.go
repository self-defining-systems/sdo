package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

func TestResponseStageHandoffTaskIsDurableBeforeJobCreation(t *testing.T) {
	for _, failSave := range []bool{false, true} {
		t.Run(fmt.Sprint(failSave), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			client := fake.NewSimpleClientset()
			observed := make(chan error, 1)
			client.PrependReactor("create", "jobs", func(k8stesting.Action) (bool, k8sruntime.Object, error) {
				state, err := durableState(client)
				if err == nil {
					requestObject, readErr := client.Tracker().Get(configMapsResource, "demo", IncidentJobName(state.IncidentRequest.IncidentId)+"-request")
					if readErr != nil {
						err = readErr
					} else if len(state.IncidentRequest.PriorStages) != 1 || requestObject.(*corev1.ConfigMap).Data[jobRequestDataKey] != state.IncidentRequest.PreparedAgentTask {
						err = errors.New("stage handoff task differs from durable request")
					}
				}
				observed <- err
				return false, nil, nil
			})
			if failSave {
				client.PrependReactor("update", "configmaps", func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
					object := action.(k8stesting.UpdateAction).GetObject().(*corev1.ConfigMap)
					if object.Name == durabilityStateName {
						state, err := decodeControllerState([]byte(object.Data[stateDataKey]))
						if err == nil && len(state.GetIncidentRequest().GetPriorStages()) > 0 {
							return true, nil, errors.New("injected stage handoff save failure")
						}
					}
					return false, nil, nil
				})
			}
			config := testControllerConfig()
			config.RepositoryWorktree = "/workspace/demo"
			dispatcher := KubernetesJobDispatcher{Client: client, Namespace: "demo", Registry: testIncidentRegistry(t, "sdo-responder:test"), AgentID: testIncidentAgentID(t), ServiceAccount: "sdo-responder", RepositoryPVC: "repository", PollInterval: time.Millisecond, LoopFactory: func(request IncidentRequest) *ResponderLoop {
				return NewResponderLoop(request, nil, staticProvider{snapshot: sdktest.Snapshot{}}, nil, PlanExecutor{}, 5)
			}}
			controller := durabilityController(t, client, config, dispatcher)
			if err := controller.Step(ctx, time.Unix(0, 0), nil); err != nil {
				t.Fatal(err)
			}
			if err := controller.PersistState(ctx); err != nil {
				t.Fatal(err)
			}
			initial, err := durableState(client)
			if err != nil {
				t.Fatal(err)
			}
			if initial.GetIncidentRequest().GetPreparedAgentTask() != "" {
				t.Fatal("worker task frozen before automatic stage completed")
			}
			if err := executePendingEffects(ctx, controller); err != nil {
				t.Fatal(err)
			}
			if failSave {
				awaitDispatchCompletionQueued(t, controller)
				jobs, err := client.BatchV1().Jobs("demo").List(ctx, metav1.ListOptions{})
				if err != nil || len(jobs.Items) != 0 {
					t.Fatalf("Job launched without durable stage handoff: %v, %v", jobs, err)
				}
				return
			}
			select {
			case err := <-observed:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("no stage handoff worker")
			}
		})
	}
}

// Pause a real scheduler operation, rather than relying on random goroutine
// timing, while the dispatch goroutine tries to persist its stage handoff.
type stageHandoffDetector struct {
	sdk.Detector
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (d *stageHandoffDetector) Spec() sdk.DetectorSpec {
	if d.armed.CompareAndSwap(true, false) {
		close(d.entered)
		<-d.release
	}
	return d.Detector.Spec()
}

func TestResponseStageHandoffAlongsideStepEvents(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := fake.NewSimpleClientset()
	config := testControllerConfig()
	config.FiringThreshold = 1
	detector := &stageHandoffDetector{Detector: controllerDetector("fault", time.Second, stateFinding("fault")), entered: make(chan struct{}), release: make(chan struct{})}
	start := time.Unix(0, 0)
	// A wider inventory lets the race check observe snapshot/selection overlap.
	detectors := []sdk.Detector{detector}
	for i := 0; i < 256; i++ {
		detectors = append(detectors, controllerDetector(fmt.Sprintf("quiet-%d", i), time.Second, sdk.Finding{}))
	}
	controller, err := NewController(config, detectors, staticProvider{snapshot: sdktest.Snapshot{}}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, start)
	if err != nil {
		t.Fatal(err)
	}
	controller.now = func() time.Time { return start }
	if err := controller.AttachStateStore(ctx, lifecycleio.NewConfigMapStateStore(client, "demo", durabilityStateName)); err != nil {
		t.Fatal(err)
	}
	if err := controller.StepEvents(ctx, start, nil); err != nil {
		t.Fatal(err)
	}
	controller.mu.Lock()
	request := *cloneIncidentRequest(controller.currentIncidentRequest)
	controller.mu.Unlock()
	request.PriorStages = []*contracts.StageReport{{Stage: "deterministic"}}
	detector.armed.Store(true)
	observed := make(chan error, 1)
	go func() { observed <- controller.StepEvents(ctx, start.Add(time.Second), nil) }()
	select {
	case <-detector.entered:
	case <-ctx.Done():
		t.Fatal("controller did not enter scheduler selection")
	}
	handedOff := make(chan error, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		_, err := controller.prepareResponseStageHandoff(ctx, request)
		handedOff <- err
	}()
	// Release selection independently of handoff completion. Waiting for the
	// snapshot before releasing would create a happens-before edge and hide
	// the actual read/write race from the race detector.
	go func() {
		<-started
		time.Sleep(time.Millisecond)
		close(detector.release)
	}()
	for _, completed := range []chan error{observed, handedOff} {
		select {
		case err := <-completed:
			if err != nil {
				t.Error(err)
			}
		case <-ctx.Done():
			t.Fatal("observation/handoff did not finish")
		}
	}
	state, err := durableState(client)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.GetIncidentRequest().GetPriorStages()) != 1 {
		t.Fatal("handoff evidence was not persisted")
	}
	deadline := state.SchedulerDeadlines["fault"]
	if deadline == nil || !deadline.AsTime().Equal(start.Add(2*time.Second)) {
		t.Fatalf("handoff persisted a partial scheduler transition: %v", deadline)
	}
}
