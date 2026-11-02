package lifecycle

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/worker_registry"
)

func judgeKubernetesRegistry(t *testing.T) *worker_registry.Registry {
	t.Helper()
	registry, err := worker_registry.New(&contracts.AgentRegistration{
		SchemaVersion: worker_registry.SchemaVersion, AgentId: "judge-agent",
		Capabilities: []*contracts.AgentCapability{{TaskType: healthJudgeTask, RequestTypeUrl: "type.googleapis.com/sdodev.contracts.v1alpha1.HealthJudgeRequest", ResultTypeUrl: "type.googleapis.com/sdodev.contracts.v1alpha1.HealthJudgeResult"}},
		Launch:       &contracts.AgentRegistration_KubernetesJob{KubernetesJob: &contracts.KubernetesJobLaunch{Image: "example/judge:1", Command: []string{"/judge"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return registry
}

// completeJobsWithJudgeResult plays the worker: a created Job gets a valid
// judge result and is marked complete. It returns the number of Jobs created.
func completeJobsWithJudgeResult(t *testing.T, client *fake.Clientset) *int {
	t.Helper()
	var mu sync.Mutex
	created := 0
	client.PrependReactor("create", "jobs", func(action ktesting.Action) (bool, runtime.Object, error) {
		mu.Lock()
		created++
		mu.Unlock()
		job := action.(ktesting.CreateAction).GetObject().(*batchv1.Job).DeepCopy()
		request, err := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("configmaps"), "demo", job.Name+"-request")
		if err != nil {
			t.Errorf("Job created without its request: %v", err)
			return true, nil, err
		}
		task := &contracts.AgentTask{}
		if err := wire.UnmarshalDurable([]byte(request.(*corev1.ConfigMap).Data[worker_registry.TaskDataKey]), task); err != nil {
			t.Error(err)
			return true, nil, err
		}
		judge := &contracts.HealthJudgeRequest{}
		if err := task.GetRequest().UnmarshalTo(judge); err != nil {
			t.Error(err)
			return true, nil, err
		}
		response, _ := anypb.New(&contracts.HealthJudgeResult{Authored: true, Summary: "authored", AgentSessionId: "session", ObjectiveDigest: objectiveDigest(judge.GetHealthObjective()), DetectorIds: []string{"health-objective"}})
		result := &contracts.AgentResult{SchemaVersion: worker_registry.SchemaVersion, TaskId: task.GetTaskId(), Status: "completed", ProposalId: task.GetWorkspace().GetProposalId(), Response: response}
		data, err := wire.Canonical(proto.Message(result))
		if err != nil {
			t.Error(err)
			return true, nil, err
		}
		if err := client.Tracker().Create(corev1.SchemeGroupVersion.WithResource("configmaps"), &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: job.Name + "-result", Namespace: "demo"}, Data: map[string]string{worker_registry.ResultDataKey: string(data)}}, "demo"); err != nil {
			t.Error(err)
			return true, nil, err
		}
		job.Status.Succeeded = 1
		job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
		return true, job, client.Tracker().Create(batchv1.SchemeGroupVersion.WithResource("jobs"), job, "demo")
	})
	return &created
}

// The h9.9 shape: a controller was killed after dispatching a health refresh,
// its Job is gone, and the next controller finds the stale request and
// dispatch receipt. The refresh must dispatch exactly one fresh Job and finish
// without recording a rejection.
func TestHealthRefreshResumesFromStaleWorkerConfigMapsWithoutJob(t *testing.T) {
	base, b, _, _ := testCoordinator(t)
	b.lastCommit = "accepted-base"
	mount := t.TempDir()
	b.proposalRoot = mount + "/proposals/"
	client := fake.NewSimpleClientset()
	executor := func() worker_registry.KubernetesExecutor {
		return worker_registry.KubernetesExecutor{Client: client, Namespace: "demo", Registry: judgeKubernetesRegistry(t), RepositoryPVC: "repo", RepositoryMountPath: mount, PollInterval: time.Millisecond}
	}
	coordinator := func(workers worker_registry.KubernetesExecutor) Coordinator {
		c := base
		c.Registry, c.Workers = workers.Registry, workers
		c.SourceFacts = func(context.Context, string, string) (memory.SourceFacts, error) {
			return memory.SourceFacts{Fingerprint: strings.Repeat("f", 64), Resources: []*contracts.SourceResource{{Kind: "Deployment", Name: "demo", Namespace: "demo", Source: "deploy.yaml"}}}, nil
		}
		return c
	}
	cfg := Config{Application: "demo", Namespace: "demo", JudgeAgentID: "judge-agent"}
	revision := memory.HealthRevision{Commit: "accepted-base", SourceInputs: "source", Objective: "serve", DetectorRevision: "detectors", NeedsRefresh: true}

	// First controller: dispatches the refresh, then is killed with the Job alive.
	ctx, kill := context.WithCancel(context.Background())
	first := make(chan error, 1)
	go func() { _, err := coordinator(executor()).RefreshHealth(ctx, cfg, revision); first <- err }()
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(time.Millisecond) {
		jobs, _ := client.BatchV1().Jobs("demo").List(context.Background(), metav1.ListOptions{})
		if len(jobs.Items) == 1 {
			break
		}
		select {
		case err := <-first:
			t.Fatalf("first controller stopped before dispatching: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			kill()
			t.Fatal("first controller never dispatched the refresh Job")
		}
	}
	kill()
	if err := <-first; err == nil {
		t.Fatal("killed controller reported success")
	}
	jobs, _ := client.BatchV1().Jobs("demo").List(context.Background(), metav1.ListOptions{})
	name := jobs.Items[0].Name
	if err := client.BatchV1().Jobs("demo").Delete(context.Background(), name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"-request", "-execution"} {
		if _, err := client.CoreV1().ConfigMaps("demo").Get(context.Background(), name+suffix, metav1.GetOptions{}); err != nil {
			t.Fatalf("stale %s missing from the fixture: %v", suffix, err)
		}
	}

	// Restarted controller.
	dispatched := completeJobsWithJudgeResult(t, client)
	receipt, err := coordinator(executor()).RefreshHealth(context.Background(), cfg, revision)
	if err != nil {
		t.Fatalf("restart failed on stale worker state: %v", err)
	}
	if receipt.Commit == "" {
		t.Fatalf("restart did not publish a refreshed judge: %+v", receipt)
	}
	if *dispatched != 1 {
		t.Fatalf("restart created %d Jobs, want 1", *dispatched)
	}
	id := revision.RefreshID() + "-base-" + revision.Commit
	if log, _ := b.Rejections(id); len(log.GetRejections()) != 0 {
		t.Fatalf("restart recorded rejections: %v", log)
	}
}

// Retry exhaustion caused only by lost workers must not pin every later start
// to the terminal log: the next start gets a fresh bounded generation.
func TestHealthRefreshAfterLostWorkerExhaustionStartsFreshGeneration(t *testing.T) {
	c, b, e, _ := testCoordinator(t)
	b.lastCommit = "accepted-base"
	revision := memory.HealthRevision{Commit: "accepted-base", SourceInputs: "source", Objective: "serve", DetectorRevision: "detectors", NeedsRefresh: true}
	c.SourceFacts = func(context.Context, string, string) (memory.SourceFacts, error) {
		return memory.SourceFacts{Fingerprint: strings.Repeat("f", 64), Resources: []*contracts.SourceResource{{Kind: "Deployment", Name: "demo", Namespace: "demo", Source: "deploy.yaml"}}}, nil
	}
	cfg := Config{Application: "demo", Namespace: "demo", JudgeAgentID: "judge-agent"}
	e.judgeJobsVanished = 1000
	if _, err := c.RefreshHealth(context.Background(), cfg, revision); err == nil {
		t.Fatal("expected the lost-worker budget to end the refresh")
	}
	id := revision.RefreshID() + "-base-" + revision.Commit
	if log, _ := b.Rejections(id); !log.GetTerminal() {
		t.Fatalf("first generation was not exhausted: %v", log)
	}
	e.judgeJobsVanished = 0
	if _, err := c.RefreshHealth(context.Background(), cfg, revision); err != nil {
		t.Fatalf("restart stayed pinned to the exhausted generation: %v", err)
	}
	if log, _ := b.Rejections(id + "-g1"); log == nil || len(log.GetRejections()) != 0 {
		t.Fatalf("fresh generation unexpectedly rejected: %v", log)
	}
}

// A terminal log from the judge's own rejections stays terminal.
func TestHealthRefreshKeepsJudgeRejectionsTerminal(t *testing.T) {
	c, b, _, _ := testCoordinator(t)
	revision := memory.HealthRevision{Commit: "accepted-base", SourceInputs: "source", Objective: "serve", DetectorRevision: "detectors", NeedsRefresh: true}
	id := revision.RefreshID() + "-base-" + revision.Commit
	for i := 0; i <= broker.MaxValidationRetries; i++ {
		if _, err := b.RecordRejection(id, []*contracts.MemoryDiagnostic{{Rule: "detector.manifest", Message: "bad"}}); err != nil {
			t.Fatal(err)
		}
	}
	got, err := c.refreshGeneration(id)
	if err != nil || got != id {
		t.Fatalf("judge-rejected log was bypassed: %q %v", got, err)
	}
}

func objectiveDigest(objective string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(strings.TrimSpace(objective))))
}
