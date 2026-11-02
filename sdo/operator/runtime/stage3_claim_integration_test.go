//go:build integration

package runtime

import (
	"context"
	"errors"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/worker_registry"
	"strings"
	"testing"
	"time"
)

// A real stage application feeds the production dispatcher. Health clears
// during dispatch preparation after the writes; publication of
// a worker Job (cold claim) must not occur. No worker result is fabricated.
func TestStageRecoveryRechecksBeforeFirstWorkerPublication(t *testing.T) {
	loop, state := pipelineTestLoop(t, true)
	stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, ObservationCeiling: time.Nanosecond}
	if err := RunResponseStages(context.Background(), state, []ResponseStage{stage}); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	request := state.Request
	request.RepositoryWorktree = root + "/proposal"
	loop.Request.RepositoryWorktree = request.RepositoryWorktree
	request.PriorStages = state.PriorStages
	client := fake.NewSimpleClientset()
	claims := 0
	client.PrependReactor("create", "jobs", func(ktesting.Action) (bool, runtime.Object, error) {
		claims++
		return true, nil, errors.New("synthetic worker publication trap")
	})
	dispatcher := KubernetesJobDispatcher{Client: client, Namespace: "control", Registry: testIncidentRegistry(t, "worker"), AgentID: testIncidentAgentID(t), RepositoryPVC: "repo", RepositoryMountPath: root, ServiceAccount: "responder", LoopFactory: func(IncidentRequest) *ResponderLoop { return loop }, DispatchEnvironment: func(context.Context) map[string]string {
		// The uncovered Deployment has recovered while preparation ran. Its real
		// detector now clears from the production snapshot boundary.
		typed := loop.Executor.Client.(TypedPlanClient)
		deployment, err := typed.Client.AppsV1().Deployments("app").Get(context.Background(), "api", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		deployment.Spec.Template.Spec.Containers[0].Image = "good:v1"
		if _, err = typed.Client.AppsV1().Deployments("app").Update(context.Background(), deployment, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
		return nil
	}}
	lifecycle := &memoryLifecycleSink{}
	controller := &Controller{dispatcher: dispatcher, lifecycleSink: lifecycle}
	result, err := (agentLifecycleDispatcher{controller: controller}).Dispatch(context.Background(), request)
	if err != nil || claims != 0 || result.ResponderSessionID != "" || result.ResolutionPath != "deterministic" {
		t.Fatalf("stale handoff claimed worker: claims=%d result=%+v err=%v", claims, result, err)
	}
	if len(lifecycle.byPhase(PhaseDispatched)) != 0 {
		t.Fatal("recovered incident emitted a responder dispatch")
	}
	if len(result.PlanApplications) != 1 {
		t.Fatalf("skipped worker lost real saved application: %+v", result)
	}
	if result.PlanApplications[0].Result.RecoveryWait.GetExitReason() != contracts.RecoveryWait_EXIT_REASON_CLEARED {
		t.Fatal("claim gate lost shared exit")
	}
}

// Ownership checks precede recovery reclassification. Published requests and
// results are uncertain/already-executed worker facts, not fresh claim authority.
func TestStageClaimGateKeepsPublishedWorkerOwnership(t *testing.T) {
	for _, kind := range []string{"job", "request", "result", "addressed-job", "addressed-request", "addressed-result", "addressed-execution"} {
		t.Run(kind, func(t *testing.T) {
			loop, state := pipelineTestLoop(t, true)
			if err := RunResponseStages(context.Background(), state, []ResponseStage{&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, ObservationCeiling: time.Nanosecond}}); err != nil {
				t.Fatal(err)
			}
			request := state.Request
			request.PriorStages = state.PriorStages
			client := fake.NewSimpleClientset()
			task := &contracts.AgentTask{TaskId: request.IncidentID}
			name := worker_registry.JobName(request.IncidentID)
			evidence := kind
			if strings.HasPrefix(kind, "addressed-") {
				name = worker_registry.TaskJobName(task)
				evidence = strings.TrimPrefix(kind, "addressed-")
			}
			if evidence == "job" {
				_, err := client.BatchV1().Jobs("control").Create(context.Background(), &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "control"}}, metav1.CreateOptions{})
				if err != nil {
					t.Fatal(err)
				}
			} else {
				_, err := client.CoreV1().ConfigMaps("control").Create(context.Background(), &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name + "-" + evidence, Namespace: "control"}}, metav1.CreateOptions{})
				if err != nil {
					t.Fatal(err)
				}
			}
			d := KubernetesJobDispatcher{Client: client, Namespace: "control"}
			recovered, err := d.beforeFirstResponderClaim(context.Background(), request, task, loop)
			if err != nil || recovered != nil {
				t.Fatalf("published worker was reclassified: %v %v", recovered, err)
			}
		})
	}
}
