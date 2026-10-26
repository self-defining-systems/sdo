package runtime

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
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
