package runtime

import (
	"context"
	"errors"
	"fmt"
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
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/worker_registry"
)

func recRestartRequest(t *testing.T, id string, attempt int, conflict bool) *corev1.ConfigMap {
	t.Helper()
	taskID := id
	if attempt > 0 {
		taskID = fmt.Sprintf("%s-continue-%d", id, attempt)
	}
	request, err := anypb.New(&contracts.IncidentRequest{IncidentId: id})
	if err != nil {
		t.Fatal(err)
	}
	task := &contracts.AgentTask{SchemaVersion: ProtocolSchemaVersion, TaskId: taskID, AgentId: "responder", TaskType: "incident", Request: request}
	payload, err := wire.Marshal(task)
	if err != nil {
		t.Fatal(err)
	}
	name := IncidentJobName(taskID)
	if conflict {
		name = worker_registry.TaskJobName(task)
	}
	return &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name + "-request", Namespace: "control"}, Data: map[string]string{jobRequestDataKey: string(payload)}}
}

func TestRECPostRestartWaitsForContinuation(t *testing.T) {
	for _, conflict := range []bool{false, true} {
		t.Run(fmt.Sprintf("request-conflict-%v", conflict), func(t *testing.T) {
			c, _, step := earlyWaitController(t, stateFinding("health"), stateFinding("health"), sdk.Finding{})
			for i := 2; i <= 17; i++ {
				step(i)
			}
			state := exportState(t, c)
			id := state.GetIncidentRequest().GetIncidentId()
			request := recRestartRequest(t, id, 1, conflict)
			name := request.Name[:len(request.Name)-len("-request")]
			client := fake.NewSimpleClientset(request, &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "control"}, Status: batchv1.JobStatus{Active: 1}})
			// Delete acceptance does not prove foreground termination completed.
			client.PrependReactor("delete", "jobs", func(ktesting.Action) (bool, kruntime.Object, error) { return true, nil, nil })
			restored, err := NewController(testControllerConfig(), c.detectorList(), c.provider, KubernetesJobDispatcher{Client: client, Namespace: "control"}, time.Unix(17, 0))
			if err != nil {
				t.Fatal(err)
			}
			restored.broker = &recordingIncidentBroker{}
			if err := restored.RestoreState(state); err != nil {
				t.Fatal(err)
			}
			restored.finishEarlyReport(context.Background(), time.Unix(47, 0))
			if _, ok := restored.PendingClosureEffect(); ok {
				t.Fatal("closure/reflection released while continuation Job remains active after restart")
			}
			deleted := false
			for _, action := range client.Actions() {
				if action.Matches("delete", "jobs") && action.(ktesting.DeleteAction).GetName() == name {
					deleted = true
				}
			}
			if !deleted {
				t.Fatal("continuation termination was not requested")
			}
			// Dispatch cancellation may complete before foreground deletion does.
			restored.mu.Lock()
			restored.responderDone = true
			restored.mu.Unlock()
			restored.finishEarlyReport(context.Background(), time.Unix(48, 0))
			if _, ok := restored.PendingClosureEffect(); ok {
				t.Fatal("dispatch completion bypassed worker quiescence")
			}
			if err := client.Tracker().Delete(batchv1.SchemeGroupVersion.WithResource("jobs"), "control", name); err != nil {
				t.Fatal(err)
			}
			restored.finishEarlyReport(context.Background(), time.Unix(49, 0))
			if _, ok := restored.PendingClosureEffect(); !ok {
				t.Fatal("closure stayed blocked after all continuation work terminated")
			}
		})
	}
}

func TestRECStopResponderChecksAllContinuationsAndOrphanPods(t *testing.T) {
	id := "restart-incident"
	first, last := IncidentJobName(id+"-continue-1"), IncidentJobName(id+"-continue-4")
	other := IncidentJobName(id + "-other-continue-1")
	client := fake.NewSimpleClientset(recRestartRequest(t, id, 1, false), recRestartRequest(t, id, 4, false), recRestartRequest(t, id+"-other", 1, false),
		&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: first, Namespace: "control"}, Status: batchv1.JobStatus{Active: 1}},
		&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: last, Namespace: "control"}, Status: batchv1.JobStatus{Active: 1}},
		&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: other, Namespace: "control"}, Status: batchv1.JobStatus{Active: 1}})
	for i, metadata := range []metav1.ObjectMeta{
		{Labels: map[string]string{"sdo.dev/incident": first}},
		{Labels: map[string]string{"job-name": last}},
		{Labels: map[string]string{"batch.kubernetes.io/job-name": last}},
		{OwnerReferences: []metav1.OwnerReference{{Kind: "Job", Name: last}}},
	} {
		metadata.Name, metadata.Namespace = fmt.Sprintf("orphan-%d", i), "control"
		if _, err := client.CoreV1().Pods("control").Create(context.Background(), &corev1.Pod{ObjectMeta: metadata, Status: corev1.PodStatus{Phase: corev1.PodRunning}}, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	client.PrependReactor("delete", "jobs", func(ktesting.Action) (bool, kruntime.Object, error) { return true, nil, nil })
	client.PrependReactor("delete", "pods", func(ktesting.Action) (bool, kruntime.Object, error) { return true, nil, nil })
	err := (KubernetesJobDispatcher{Client: client, Namespace: "control"}).StopResponder(context.Background(), id, 0)
	if !errors.Is(err, errResponderTerminating) {
		t.Fatalf("live continuations/orphans must block closure: %v", err)
	}
	deleted := map[string]bool{}
	for _, action := range client.Actions() {
		if action.GetVerb() == "delete" {
			deleted[action.GetResource().Resource+"/"+action.(ktesting.DeleteAction).GetName()] = true
		}
	}
	for _, name := range []string{"jobs/" + first, "jobs/" + last, "pods/orphan-0", "pods/orphan-1", "pods/orphan-2", "pods/orphan-3"} {
		if !deleted[name] {
			t.Errorf("did not request termination of %s", name)
		}
	}
	if deleted["jobs/"+other] {
		t.Fatal("stopped another incident's worker")
	}
}

func TestRECStopResponderFailsClosedOnInventoryAndDeletionErrors(t *testing.T) {
	for _, tc := range []struct{ verb, resource string }{{"list", "configmaps"}, {"list", "jobs"}, {"list", "pods"}, {"delete", "jobs"}, {"delete", "pods"}} {
		t.Run(tc.verb+"-"+tc.resource, func(t *testing.T) {
			id := "restart-incident"
			name := IncidentJobName(id)
			client := fake.NewSimpleClientset(recRestartRequest(t, id, 0, false), &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "control"}}, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "worker", Namespace: "control", Labels: map[string]string{"job-name": name}}})
			failure := errors.New("inventory denied")
			client.PrependReactor(tc.verb, tc.resource, func(ktesting.Action) (bool, kruntime.Object, error) { return true, nil, failure })
			if err := (KubernetesJobDispatcher{Client: client, Namespace: "control"}).StopResponder(context.Background(), id, 0); !errors.Is(err, failure) {
				t.Fatalf("uncertain worker state must fail closed: %v", err)
			}
		})
	}
}

func TestRECStopResponderFindsRealStagedConflictJobAndSessionOrphan(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	request := goldenRequest(t)
	request.RepositoryWorktree = root + "/proposals/incident"
	request.Findings[0].Evidence = strings.Repeat("x", worker_registry.MaxConfigMapRequestBytes+1024)
	registry := testIncidentRegistry(t, "sdo-responder:test")
	task, payload, err := encodeIncidentAgentTask(request, registry, testIncidentAgentID(t))
	if err != nil {
		t.Fatal(err)
	}
	task.TaskId = request.IncidentID + "-continue-1"
	// The original payload was oversized before changing only its task identity.
	if len(payload) <= worker_registry.MaxConfigMapRequestBytes {
		t.Fatal("fixture did not exceed request ConfigMap budget")
	}
	session, err := memory.PrepareIncidentSession(root, "tenant", request.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	legacyRequest := recRestartRequest(t, request.IncidentID, 1, false)
	legacyRequest.Namespace = "demo"
	client := fake.NewSimpleClientset(legacyRequest)
	executor := worker_registry.KubernetesExecutor{Client: client, Namespace: "demo", Registry: registry, RepositoryPVC: "repo", RepositoryMountPath: root, RepositoryPVCSubPath: "tenant", SessionPVCSubPath: session, PollInterval: time.Millisecond}
	workerCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	goUntilCleanup(t, cancel, func() {
		defer close(done)
		_, executeErr := executor.Execute(workerCtx, task)
		if executeErr != nil && workerCtx.Err() == nil {
			t.Errorf("staged executor: %v", executeErr)
		}
	})
	name := worker_registry.TaskJobName(task)
	job := awaitJob(t, client, name)
	cancel()
	<-done
	if _, err := client.CoreV1().ConfigMaps("demo").Get(ctx, name+"-request", metav1.GetOptions{}); err == nil {
		t.Fatal("staged request unexpectedly has a ConfigMap")
	}
	orphan := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "staged-orphan", Namespace: "demo"}, Spec: *job.Spec.Template.Spec.DeepCopy(), Status: corev1.PodStatus{Phase: corev1.PodRunning}}
	if _, err := client.CoreV1().Pods("demo").Create(ctx, orphan, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	client.PrependReactor("delete", "jobs", func(ktesting.Action) (bool, kruntime.Object, error) { return true, nil, nil })
	client.PrependReactor("delete", "pods", func(ktesting.Action) (bool, kruntime.Object, error) { return true, nil, nil })
	dispatcher := KubernetesJobDispatcher{Client: client, Namespace: "demo", RepositoryMountPath: root, RepositoryPVC: "repo", RepositoryPVCSubPath: "tenant"}
	if err := dispatcher.StopResponder(ctx, request.IncidentID, 0); !errors.Is(err, errResponderTerminating) {
		t.Fatalf("staged request conflict worker escaped inventory: %v", err)
	}
	deletedJob, deletedPod := false, false
	for _, action := range client.Actions() {
		if action.GetVerb() == "delete" {
			deleted := action.(ktesting.DeleteAction).GetName()
			deletedJob = deletedJob || action.Matches("delete", "jobs") && deleted == name
			deletedPod = deletedPod || action.Matches("delete", "pods") && deleted == orphan.Name
		}
	}
	if !deletedJob || !deletedPod {
		t.Fatalf("termination requested: Job=%v orphan=%v", deletedJob, deletedPod)
	}
	if err := client.Tracker().Delete(batchv1.SchemeGroupVersion.WithResource("jobs"), "demo", name); err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.StopResponder(ctx, request.IncidentID, 0); !errors.Is(err, errResponderTerminating) {
		t.Fatalf("session-only orphan escaped inventory after Job disappeared: %v", err)
	}
	if err := client.Tracker().Delete(corev1.SchemeGroupVersion.WithResource("pods"), "demo", orphan.Name); err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.StopResponder(ctx, request.IncidentID, 0); err != nil {
		t.Fatalf("terminated staged workers still block: %v", err)
	}
}

func TestRECStopResponderReadsJournalForStagedContinuation(t *testing.T) {
	ctx := context.Background()
	id := "journal-only"
	name := IncidentJobName(id + "-continue-3")
	client := fake.NewSimpleClientset(&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "control"}}, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "journal-orphan", Namespace: "control", Labels: map[string]string{"job-name": name}}, Status: corev1.PodStatus{Phase: corev1.PodRunning}})
	dispatcher := KubernetesJobDispatcher{Client: client, Namespace: "control"}
	loop := NewResponderLoop(IncidentRequest{IncidentID: id}, nil, nil, nil, PlanExecutor{}, 4)
	attachRecoveryStore(t, dispatcher, loop)
	loop.continuation = responderContinuation{Attempts: 3, Result: &contracts.IncidentResult{IncidentId: id}}
	if err := loop.persistToolState(ctx); err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.StopResponder(ctx, id, 0); err != nil {
		t.Fatal(err)
	}
	pods, err := client.CoreV1().Pods("control").List(ctx, metav1.ListOptions{})
	if err != nil || len(pods.Items) != 0 {
		t.Fatalf("journal-owned orphan remained: %v %v", pods, err)
	}
	jobs, err := client.BatchV1().Jobs("control").List(ctx, metav1.ListOptions{})
	if err != nil || len(jobs.Items) != 0 {
		t.Fatalf("journal-owned continuation remained: %v %v", jobs, err)
	}
}

func TestRECStopResponderFailsClosedOnQuiescenceReadErrors(t *testing.T) {
	for _, resource := range []string{"jobs", "pods"} {
		t.Run(resource, func(t *testing.T) {
			client := fake.NewSimpleClientset()
			count := 0
			failure := errors.New("second inventory unavailable")
			client.PrependReactor("list", resource, func(ktesting.Action) (bool, kruntime.Object, error) {
				count++
				if count == 2 {
					return true, nil, failure
				}
				return false, nil, nil
			})
			if err := (KubernetesJobDispatcher{Client: client, Namespace: "control"}).StopResponder(context.Background(), "restart", 0); !errors.Is(err, failure) {
				t.Fatalf("quiescence read error accepted as absence: %v", err)
			}
		})
	}
}

func TestRECStopResponderSessionOwnershipRequiresExactPVCAndSubPath(t *testing.T) {
	session, err := memory.IncidentSessionSubPath("tenant", "current-incident")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, claim, subPath, mountPath string }{
		{"other-pvc", "other-repo", session, "/sdo/session"},
		{"other-session", "repo", session + "-other", "/sdo/session"},
		{"nested-session", "repo", session + "/tools", "/sdo/session"},
		{"other-mount", "repo", session, "/another/session"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec := corev1.PodSpec{Volumes: []corev1.Volume{{Name: "repository", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: tc.claim}}}}, Containers: []corev1.Container{{Name: "agent", VolumeMounts: []corev1.VolumeMount{{Name: "repository", MountPath: tc.mountPath, SubPath: tc.subPath}}}}}
			client := fake.NewSimpleClientset(&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "foreign-worker", Namespace: "control"}, Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: spec}}}, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foreign-orphan", Namespace: "control"}, Spec: spec, Status: corev1.PodStatus{Phase: corev1.PodRunning}})
			dispatcher := KubernetesJobDispatcher{Client: client, Namespace: "control", RepositoryPVC: "repo", RepositoryPVCSubPath: "tenant"}
			if err := dispatcher.StopResponder(context.Background(), "current-incident", 0); err != nil {
				t.Fatal(err)
			}
			for _, action := range client.Actions() {
				if action.GetVerb() == "delete" {
					t.Fatalf("deleted unowned worker through %s", tc.name)
				}
			}
		})
	}
}

func TestRECPostRestartWaitsForCanonicalContinuationWithoutRequest(t *testing.T) {
	c, _, step := earlyWaitController(t, stateFinding("health"), stateFinding("health"), sdk.Finding{})
	c.mu.Lock()
	c.currentIncidentRequest.MaxResponseLoops = 3
	c.mu.Unlock()
	for i := 2; i <= 17; i++ {
		step(i)
	}
	state := exportState(t, c)
	id := state.GetIncidentRequest().GetIncidentId()
	name := IncidentJobName(id + "-continue-1")
	// Both the request ConfigMap and tools journal have disappeared. The frozen
	// request still bounds all canonical task IDs authorized for this incident.
	client := fake.NewSimpleClientset(&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "control"}, Status: batchv1.JobStatus{Active: 1}})
	client.PrependReactor("delete", "jobs", func(ktesting.Action) (bool, kruntime.Object, error) { return true, nil, nil })
	restored, err := NewController(testControllerConfig(), c.detectorList(), c.provider, KubernetesJobDispatcher{Client: client, Namespace: "control"}, time.Unix(17, 0))
	if err != nil {
		t.Fatal(err)
	}
	restored.broker = &recordingIncidentBroker{}
	if err := restored.RestoreState(state); err != nil {
		t.Fatal(err)
	}
	restored.finishEarlyReport(context.Background(), time.Unix(47, 0))
	if _, ok := restored.PendingClosureEffect(); ok {
		t.Fatal("frozen attempt bound ignored: canonical continuation released closure without request or journal")
	}
	deleted := false
	for _, action := range client.Actions() {
		if action.Matches("delete", "jobs") && action.(ktesting.DeleteAction).GetName() == name {
			deleted = true
		}
	}
	if !deleted {
		t.Fatal("canonical continuation termination was not requested from frozen attempt bound")
	}
	if err := client.Tracker().Delete(batchv1.SchemeGroupVersion.WithResource("jobs"), "control", name); err != nil {
		t.Fatal(err)
	}
	restored.finishEarlyReport(context.Background(), time.Unix(48, 0))
	if _, ok := restored.PendingClosureEffect(); !ok {
		t.Fatal("terminated canonical continuation still blocks closure")
	}
}
