package runtime

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
)

type fastloopDispatch struct {
	requests chan IncidentRequest
	results  chan IncidentResult
}

func (d fastloopDispatch) Dispatch(ctx context.Context, request IncidentRequest) (IncidentResult, error) {
	select {
	case d.requests <- request:
	case <-ctx.Done():
		return IncidentResult{}, ctx.Err()
	}
	select {
	case result := <-d.results:
		return result, nil
	case <-ctx.Done():
		return IncidentResult{}, ctx.Err()
	}
}

func TestHermeticControllerClosureReachesNativeOutcomeBroker(t *testing.T) {
	ctx := context.Background()
	client := fastloopClient()
	d, err := client.AppsV1().Deployments("app").Get(ctx, "api", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	d.UID, d.Generation = "deployment", 1
	oldTemplate := *d.Spec.Template.DeepCopy()
	if _, err := client.AppsV1().Deployments("app").Update(ctx, d, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	accepted := nativeRepository(t)
	repository, err := broker.New(accepted, filepath.Join(t.TempDir(), "proposals"), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	native := NativeIncidentBroker{Repository: repository, LedgerRoot: filepath.Join(t.TempDir(), "ledger"), Deriver: ProtoOutcomeDeriver{}, ResponderBackend: "scripted", ResponderModel: "none",
		Reflector: ReflectionWorkerFunc(func(context.Context, IncidentClosure, IncidentWorkspace, *contracts.OutcomeRecord, string, string, ReflectionFeedback) (ReflectionDecision, error) {
			return ReflectionDecision{LearningDecision: "no_change", NoChangeReason: "fixture program already covers this class"}, nil
		})}
	dispatcher := fastloopDispatch{requests: make(chan IncidentRequest, 1), results: make(chan IncidentResult, 1)}
	config := ControllerConfig{Application: "app", Namespace: "app", SourceCommit: nativeGit(t, accepted, "rev-parse", "HEAD"), DeployedCommit: nativeGit(t, accepted, "rev-parse", "HEAD"), RepositoryWorktree: accepted,
		CorpusSessionRoot: t.TempDir(), ResponseTimeout: time.Minute, VerificationTimeout: time.Minute, FiringThreshold: 1, ClearThreshold: 1, RepairPolicy: "recorded-actions"}
	start := time.Now().UTC()
	controller, err := NewController(config, []sdk.Detector{fastloopDetector{}}, fastloopObjects{client: client}, dispatcher, start)
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.SetIncidentBroker(native); err != nil {
		t.Fatal(err)
	}
	closed := make(chan IncidentClosure, 1)
	controller.OnIncidentClosed = func(closure IncidentClosure) { closed <- closure }
	if err := controller.Step(ctx, start, nil); err != nil {
		t.Fatal(err)
	}
	workspaceEffect, ok := controller.PendingWorkspaceEffect()
	if !ok {
		t.Fatalf("controller did not prepare the broker workspace: %+v", exportState(t, controller))
	}
	if err := controller.ExecuteWorkspaceEffect(ctx, workspaceEffect); err != nil {
		t.Fatal(err)
	}
	awaitBrokerResult(t, controller.workspaceResults)
	if err := controller.Step(ctx, start.Add(10*time.Millisecond), nil); err != nil {
		t.Fatal(err)
	}
	executePendingEffect(t, controller)
	request := awaitRequest(t, dispatcher.requests)
	if request.RepositoryWorktree == accepted || request.IncidentID != workspaceEffect.IncidentID {
		t.Fatalf("dispatch did not use broker checkout: %+v", request)
	}
	for _, item := range []struct {
		ref   sdk.ObjectRef
		patch string
	}{
		{sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}, `{"spec":{"ports":[{"port":80,"targetPort":8080}]}}`},
		{sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}, `{"spec":{"template":{"spec":{"containers":[{"name":"api","image":"good:v1","ports":[{"containerPort":8080}]}]}}}}`},
	} {
		if err := (TypedPlanClient{Client: client}).Patch(ctx, item.ref, types.MergePatchType, []byte(item.patch)); err != nil {
			t.Fatal(err)
		}
	}
	// The fake API has no Deployment controller. Model its separate observation
	// explicitly: source fields are repaired, but admission still blocks new Pods.
	d, err = client.AppsV1().Deployments("app").Get(ctx, "api", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	d.Generation = 2
	d.Status = appsv1.DeploymentStatus{ObservedGeneration: 2, Replicas: 1, ReadyReplicas: 1, AvailableReplicas: 1}
	if _, err := client.AppsV1().Deployments("app").Update(ctx, d, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	one, owner := int32(1), true
	old := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{Name: "api-old", Namespace: "app", UID: "old-replicaset", OwnerReferences: []metav1.OwnerReference{{Kind: "Deployment", UID: d.UID, Controller: &owner}}},
		Spec:       appsv1.ReplicaSetSpec{Replicas: &one, Template: oldTemplate},
	}
	if _, err := client.AppsV1().ReplicaSets("app").Create(ctx, old, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	pod, err := client.CoreV1().Pods("app").Get(ctx, "api-pod", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pod.UID = "old-pod"
	pod.OwnerReferences = []metav1.OwnerReference{{Kind: "ReplicaSet", UID: old.UID, Controller: &owner}}
	pod.Status = corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}
	if _, err := client.CoreV1().Pods("app").Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{Name: "api-current", Namespace: "app", UID: "replicaset", OwnerReferences: []metav1.OwnerReference{{Kind: "Deployment", UID: d.UID, Controller: &owner}}},
		Spec:       appsv1.ReplicaSetSpec{Replicas: &one, Template: *d.Spec.Template.DeepCopy()},
		Status:     appsv1.ReplicaSetStatus{Conditions: []appsv1.ReplicaSetCondition{{Type: appsv1.ReplicaSetReplicaFailure, Status: corev1.ConditionTrue, Reason: "FailedCreate", Message: "admission webhook policy.example denied Pod creation"}}},
	}
	if _, err := client.AppsV1().ReplicaSets("app").Create(ctx, rs, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	event := &corev1.Event{ObjectMeta: metav1.ObjectMeta{Name: "admission-blocked", Namespace: "app"}, Type: corev1.EventTypeWarning, InvolvedObject: corev1.ObjectReference{Kind: "ReplicaSet", Namespace: "app", Name: rs.Name, UID: rs.UID}, Reason: "FailedCreate", Message: "admission webhook policy.example denied Pod creation on retry"}
	if _, err := client.CoreV1().Events("app").Create(ctx, event, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	result := completedResult(request.IncidentID)
	result.Timing.StartedAt = start.Add(100 * time.Millisecond)
	result.Timing.CompletedAt = start.Add(200 * time.Millisecond)
	result.ConfirmedRootCauses = []ConfirmedRootCause{{Summary: "Service port drift and image pull failure", Resources: []sdk.ObjectRef{{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}, {APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}}, Evidence: []RootCauseEvidence{{Kind: "live-observation", Source: "fake Kubernetes API", Observation: "port and image returned to their source values"}}, ExplainedDetectors: []string{"availability"}}}
	result.RepairActions = []RepairActionReceipt{{ActionID: "repair", Kind: "kubectl", Target: "app", Summary: "restored port and image", Details: "applied source values", Resources: result.ConfirmedRootCauses[0].Resources, StartedAt: result.Timing.StartedAt, CompletedAt: result.Timing.CompletedAt, Success: true}}
	result.VerificationEvidence = []VerificationEvidence{{Name: "scripted verification", Passed: true, Details: "detectors clear", ObservedAt: result.Timing.CompletedAt}}
	dispatcher.results <- result
	awaitDispatchCompletionQueued(t, controller)
	if err := controller.Step(ctx, start.Add(time.Second), nil); err != nil {
		t.Fatal(err)
	}
	select {
	case closure := <-closed:
		t.Fatalf("source repair closed before rollout recovery: %+v", closure)
	default:
	}
	verification := controller.responderVerification(ctx, request.IncidentID)
	if verification.Healthy || len(verification.Findings) != 1 || !strings.Contains(verification.Findings[0].Evidence, "policy.example") || !strings.Contains(verification.Findings[0].Evidence, event.Message) || !strings.Contains(verification.Findings[0].Evidence, "Pod/api-pod still runs old template") {
		t.Fatalf("pending rollout lost its named admission evidence: %+v", verification)
	}
	// Recovery is an independent API observation. Retain the old warning and
	// ReplicaFailure condition, as Kubernetes can, after the desired Pod is live.
	if err := client.CoreV1().Pods("app").Delete(ctx, pod.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	pod = &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "api-current-pod", Namespace: "app", UID: "current-pod", Labels: d.Spec.Template.Labels, OwnerReferences: []metav1.OwnerReference{{Kind: "ReplicaSet", UID: rs.UID, Controller: &owner}}},
		Spec:       *d.Spec.Template.Spec.DeepCopy(),
		Status:     corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
	}
	if _, err := client.CoreV1().Pods("app").Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	zero := int32(0)
	old.Spec.Replicas = &zero
	if _, err := client.AppsV1().ReplicaSets("app").Update(ctx, old, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	rs.Status.Replicas, rs.Status.ReadyReplicas, rs.Status.AvailableReplicas = 1, 1, 1
	if _, err := client.AppsV1().ReplicaSets("app").UpdateStatus(ctx, rs, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	d.Status.UpdatedReplicas = 1
	if _, err := client.AppsV1().Deployments("app").UpdateStatus(ctx, d, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := controller.Step(ctx, start.Add(2*time.Second), nil); err != nil {
		t.Fatal(err)
	}
	var closure IncidentClosure
	select {
	case closure = <-closed:
	case <-time.After(time.Second):
		t.Fatal("controller did not close after verified repair")
	}
	if closure.Request.IncidentID != request.IncidentID || closure.HealthClearedAt == nil {
		t.Fatalf("closure lost controller evidence: %+v", closure)
	}
	receipt, err := native.ProcessClosure(ctx, closure)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.OutcomeCommit == "" {
		t.Fatal("native broker did not commit the outcome")
	}
	stored, err := memory.OpenReader(accepted).Get(ctx, request.IncidentID)
	if err != nil || stored.Record.GetClassification() != "success" {
		t.Fatalf("outcome was not derived from verified closure: %+v %v", stored.Record, err)
	}
	if err := native.AcknowledgeClosure(ctx, receipt); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(request.RepositoryWorktree); !os.IsNotExist(err) {
		t.Fatalf("acknowledged proposal checkout remains: %v", err)
	}
	if _, err := client.CoreV1().Services("app").Get(ctx, "api", metav1.GetOptions{}); err != nil {
		t.Fatal(err)
	}
}
