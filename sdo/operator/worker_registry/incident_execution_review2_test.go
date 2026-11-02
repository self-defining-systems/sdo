package worker_registry

import (
	"context"
	"errors"
	corev1 "k8s.io/api/core/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"google.golang.org/protobuf/types/known/anypb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func TestRWS2PVCContinuationRemainsDiscoverable(t *testing.T) {
	for _, conflict := range []bool{false, true} {
		t.Run(map[bool]string{false: "canonical", true: "request-conflict"}[conflict], func(t *testing.T) {
			_, e, task := standbyFixture(t)
			task.TaskId += "-continue-1"
			request := new(contracts.IncidentRequest)
			if err := task.Request.UnmarshalTo(request); err != nil {
				t.Fatal(err)
			}
			request.MemorySnapshot = &contracts.OperationalMemorySnapshot{AcceptedCommit: task.Workspace.BaseCommit, GoalMarkdown: strings.Repeat("synthetic memory\n", 40)}
			task.Request, _ = anypb.New(request)
			if conflict {
				if err := e.ensureRequest(context.Background(), JobName(task.TaskId)+"-request", `{"different":"request"}`); err != nil {
					t.Fatal(err)
				}
			}
			stopped := errors.New("leadership lost after publication")
			e.OnPhase = func(context.Context, string) error { return stopped }
			if _, err := e.Execute(context.Background(), task); !errors.Is(err, stopped) {
				t.Fatal(err)
			}
			name := JobName(task.TaskId)
			if conflict {
				name = TaskJobName(task)
			}
			if _, err := e.Client.BatchV1().Jobs(e.Namespace).Get(context.Background(), name, metav1.GetOptions{}); err != nil {
				t.Fatalf("real PVC worker missing: %v", err)
			}
			// Discover independently of both request transport and local session files.
			names, err := IncidentExecutionNames(context.Background(), e.Client, e.Namespace, "", request.IncidentId, false)
			if err != nil || !slices.Contains(names, name) {
				t.Fatalf("PVC continuation invisible: %v %v", names, err)
			}
			if err := e.Client.BatchV1().Jobs(e.Namespace).Delete(context.Background(), name, metav1.DeleteOptions{}); err != nil {
				t.Fatal(err)
			}
			names, err = IncidentExecutionNames(context.Background(), e.Client, e.Namespace, "", request.IncidentId, false)
			if err != nil || !slices.Contains(names, name) {
				t.Fatalf("orphan-Pod execution identity lost with Job: %v %v", names, err)
			}
		})
	}
}

func TestRWS2ExecutionIdentityWriteFailureDoesNotReleaseWorker(t *testing.T) {
	for _, warm := range []bool{false, true} {
		t.Run(map[bool]string{false: "cold", true: "warm"}[warm], func(t *testing.T) {
			pool, e, task := standbyFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var inbox string
			if warm {
				readyStandby(t, pool)
				bound, err := pool.TryBind(ctx, e, task)
				if err != nil || bound.WarmBinding == nil {
					t.Fatalf("bind: %v", err)
				}
				e = bound
				inbox = filepath.Join(bound.WarmBinding.Directory, "control", TaskDataKey)
			}
			unavailable := errors.New("execution identity storage unavailable")
			client := e.Client.(*fake.Clientset)
			client.PrependReactor("create", "configmaps", func(action ktesting.Action) (bool, kruntime.Object, error) {
				cm := action.(ktesting.CreateAction).GetObject().(*corev1.ConfigMap)
				if strings.HasPrefix(cm.Name, "sdo-exec-") {
					return true, nil, unavailable
				}
				return false, nil, nil
			})
			if _, err := e.Execute(ctx, task); !errors.Is(err, unavailable) {
				t.Fatalf("identity failure lost: %v", err)
			}
			jobs, err := client.BatchV1().Jobs(e.Namespace).List(ctx, metav1.ListOptions{})
			if err != nil || len(jobs.Items) != 0 {
				t.Fatalf("identity failure released cold work: %v %v", jobs, err)
			}
			if warm {
				if _, err := os.Stat(inbox); !os.IsNotExist(err) {
					t.Fatalf("identity failure published warm inbox: %v", err)
				}
			}
		})
	}
}

func TestRWS2PublishedWarmRejoinBackfillsExecutionIndex(t *testing.T) {
	pool, e, task := standbyFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	readyStandby(t, pool)
	bound, err := pool.TryBind(ctx, e, task)
	if err != nil || bound.WarmBinding == nil {
		t.Fatalf("bind: %v", err)
	}
	if err := bound.publishWarmTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	// Earlier controllers published the durable warm inbox without this index.
	maps, err := e.Client.CoreV1().ConfigMaps(e.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, cm := range maps.Items {
		if cm.Labels[incidentExecutionLabel] != "" {
			if err := e.Client.CoreV1().ConfigMaps(e.Namespace).Delete(ctx, cm.Name, metav1.DeleteOptions{}); err != nil {
				t.Fatal(err)
			}
		}
	}
	stopped := errors.New("leadership lost")
	bound.OnPhase = func(context.Context, string) error { return stopped }
	if _, err := bound.Execute(ctx, task); !errors.Is(err, stopped) {
		t.Fatal(err)
	}
	names, err := IncidentExecutionNames(ctx, e.Client, e.Namespace, "", "incident", false)
	if err != nil || !slices.Contains(names, bound.WarmBinding.JobName) {
		t.Fatalf("published warm rejoin lacks common identity: %v %v", names, err)
	}
}
