package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/anypb"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"sdo.dev/operator/apihistory"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/worker_registry"
)

func rws2WarmBinding(t *testing.T, root, id, name string) {
	t.Helper()
	directory := filepath.Join(root, ".sdo-runtime/standby/bindings")
	if err := os.MkdirAll(directory, 0700); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(worker_registry.WarmBinding{IncidentID: id, JobName: name})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "initial.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
}
func rws2DeletedJob(client *fake.Clientset, name string) bool {
	for _, action := range client.Actions() {
		if action.Matches("delete", "jobs") && action.(ktesting.DeleteAction).GetName() == name {
			return true
		}
	}
	return false
}
func TestRWS2EarlyClosureStopsBoundWarmJob(t *testing.T) {
	root := t.TempDir()
	name := "sdo-standby-bound"
	rws2WarmBinding(t, root, "incident", name)
	client := fake.NewSimpleClientset(&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "control"}, Status: batchv1.JobStatus{Active: 1}})
	dispatcher := KubernetesJobDispatcher{Client: client, Namespace: "control", RepositoryMountPath: root}
	if err := dispatcher.StopResponder(context.Background(), "incident", 0); err != nil {
		t.Fatal(err)
	}
	if !rws2DeletedJob(client, name) {
		t.Fatal("actual bound standby never received termination")
	}
	work, err := dispatcher.IncidentWork(context.Background(), "incident")
	if active := work.Active; err != nil || active {
		t.Fatalf("terminated bound worker remains active: %v %v", active, err)
	}
}
func TestRWS2EarlyClosureGraceDoesNotQuiesceBoundWarmJob(t *testing.T) {
	c, _, step := earlyWaitController(t, stateFinding("health"), stateFinding("health"), sdk.Finding{})
	c.broker = &recordingIncidentBroker{}
	root := t.TempDir()
	gate, err := apihistory.PrepareMutationGate(filepath.Join(root, "api-history"))
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	c.responderLoop = &ResponderLoop{MutationGate: gate}
	c.mu.Unlock()
	for i := 2; i <= 17; i++ {
		step(i)
	}
	id := c.currentIncidentRequest.IncidentID
	name := "sdo-standby-grace"
	rws2WarmBinding(t, root, id, name)
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "control"}, Status: batchv1.JobStatus{Active: 1}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "still-running", Namespace: "control", OwnerReferences: []metav1.OwnerReference{{Kind: "Job", Name: name}}}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}
	client := fake.NewSimpleClientset(job, pod)
	client.PrependReactor("delete", "jobs", func(ktesting.Action) (bool, kruntime.Object, error) { return true, nil, nil })
	client.PrependReactor("delete", "pods", func(ktesting.Action) (bool, kruntime.Object, error) { return true, nil, nil })
	c.dispatcher = KubernetesJobDispatcher{Client: client, Namespace: "control", RepositoryMountPath: root}
	c.finishEarlyReport(context.Background(), time.Unix(47, 0))
	if !rws2DeletedJob(client, name) {
		t.Fatal("report grace never targeted the actual warm Job")
	}
	if c.currentIncidentRequest == nil || !c.earlyReportPendingLocked() {
		t.Fatal("closure released before Job/Pod quiescence")
	}
	if warning, err := gate.Verified(); err != nil || warning == nil {
		t.Fatalf("closure lost mutation fence: %v", err)
	}
	if err := client.Tracker().Delete(batchv1.SchemeGroupVersion.WithResource("jobs"), "control", name); err != nil {
		t.Fatal(err)
	}
	c.finishEarlyReport(context.Background(), time.Unix(48, 0))
	if c.currentIncidentRequest == nil || !c.earlyReportPendingLocked() {
		t.Fatal("orphan warm Pod bypassed the closure fence")
	}
	if err := client.Tracker().Delete(corev1.SchemeGroupVersion.WithResource("pods"), "control", pod.Name); err != nil {
		t.Fatal(err)
	}
	c.finishEarlyReport(context.Background(), time.Unix(49, 0))
	if c.currentIncidentRequest != nil {
		t.Fatal("verified incident remained owned after warm worker quiescence")
	}
	if _, ok := c.PendingClosureEffect(); !ok {
		t.Fatal("quiescence did not release reflection")
	}
}
func TestRWS2RestartAfterDeadlineSeesPVCContinuation(t *testing.T) {
	start := time.Unix(1000, 0)
	config := testControllerConfig()
	config.FiringThreshold = 1
	detector := controllerDetector("fault", time.Hour, stateFinding("fault"))
	initial, err := NewController(config, []sdk.Detector{detector}, staticProvider{snapshot: sdktest.Snapshot{}}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, start)
	if err != nil {
		t.Fatal(err)
	}
	if err := initial.Step(context.Background(), start, nil); err != nil {
		t.Fatal(err)
	}
	state := exportState(t, initial)
	if state.DispatchState != contracts.DispatchState_DISPATCH_STATE_PENDING {
		t.Fatalf("not the production restart dispatch state: %v", state.DispatchState)
	}
	id := state.IncidentRequest.IncidentId
	root := t.TempDir()
	request := &contracts.IncidentRequest{IncidentId: id, MemorySnapshot: &contracts.OperationalMemorySnapshot{GoalMarkdown: strings.Repeat("synthetic memory\n", 40000)}}
	payload, _ := anypb.New(request)
	task := &contracts.AgentTask{SchemaVersion: ProtocolSchemaVersion, TaskId: id + "-continue-1", AgentId: testIncidentAgentID(t), TaskType: testIncidentTaskType(t), Request: payload, Workspace: &contracts.ProposalWorkspace{CheckoutPath: root + "/proposal", BaseCommit: "accepted"}}
	client := fake.NewSimpleClientset()
	executor := worker_registry.KubernetesExecutor{Client: client, Namespace: "control", Registry: testIncidentRegistry(t, "worker"), RepositoryPVC: "repo", RepositoryMountPath: root}
	stopped := errors.New("leadership lost after Job publication")
	executor.OnPhase = func(context.Context, string) error { return stopped }
	if _, err := executor.Execute(context.Background(), task); !errors.Is(err, stopped) {
		t.Fatal(err)
	}
	name := worker_registry.JobName(task.TaskId)
	if _, err := client.BatchV1().Jobs("control").Get(context.Background(), name, metav1.GetOptions{}); err != nil {
		t.Fatal(err)
	}
	// The initial warm execution has completed; only the real PVC continuation is live.
	warm := "sdo-standby-initial-complete"
	rws2WarmBinding(t, root, id, warm)
	completed := metav1.NewTime(start)
	if _, err := client.BatchV1().Jobs("control").Create(context.Background(), &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: warm}, Status: batchv1.JobStatus{CompletionTime: &completed}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	now := state.IncidentRequest.ResponseDeadline.AsTime().Add(time.Minute)
	dispatcher := KubernetesJobDispatcher{Client: client, Namespace: "control", RepositoryMountPath: root}
	restored, err := NewController(config, []sdk.Detector{detector}, initial.provider, dispatcher, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.RestoreState(state); err != nil {
		t.Fatal(err)
	}
	if err := restored.retireExpiredIncident(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	got := exportState(t, restored)
	if !got.IncidentOpen || len(got.FailedClosures) > 0 {
		t.Fatal("restart retired an incident with a live PVC continuation")
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pvc-orphan", Namespace: "control", OwnerReferences: []metav1.OwnerReference{{Kind: "Job", Name: name}}}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}
	if _, err := client.CoreV1().Pods("control").Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := client.BatchV1().Jobs("control").Delete(context.Background(), name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := restored.retireExpiredIncident(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	if !restored.IncidentOpen() {
		t.Fatal("restart ignored PVC continuation's orphan Pod")
	}
}
