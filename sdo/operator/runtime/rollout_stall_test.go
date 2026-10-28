package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// A rollout that only another actor can unblock is handed off as soon as that
// is observable, naming the blocker. Waiting cannot clear it, so the bounded
// wait is never spent on it. Rollouts that can still progress keep waiting.
func TestSourceRolloutStallHandsOffNamingTheBlocker(t *testing.T) {
	for _, mode := range []string{"anti-affinity", "progress-deadline", "old-pod-terminating"} {
		t.Run(mode, func(t *testing.T) {
			deployment, current, stale := reconcileRolloutObjects(t)
			deployment.Status.UpdatedReplicas, deployment.Status.AvailableReplicas = 1, 1
			controller := true
			oldPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "old-pod", Namespace: stale.Namespace, UID: "old-pod-uid", OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: stale.Name, UID: stale.UID, Controller: &controller}}},
				Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}
			unschedulable := "0/1 nodes are available: 1 node(s) didn't match pod anti-affinity rules."
			newPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "new-pod", Namespace: current.Namespace, UID: "new-pod-uid", OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: current.Name, UID: current.UID, Controller: &controller}}},
				Status: corev1.PodStatus{Phase: corev1.PodPending, Conditions: []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: corev1.PodReasonUnschedulable, Message: unschedulable}}}}
			event := &corev1.Event{ObjectMeta: metav1.ObjectMeta{Name: "new-pod.failed", Namespace: newPod.Namespace}, Type: corev1.EventTypeWarning, Reason: "FailedScheduling", Message: unschedulable, LastTimestamp: metav1.Now(),
				InvolvedObject: corev1.ObjectReference{Kind: "Pod", Namespace: newPod.Namespace, Name: newPod.Name, UID: newPod.UID}}
			switch mode {
			case "progress-deadline":
				newPod.Status.Conditions = nil
				deployment.Status.Conditions = []appsv1.DeploymentCondition{{Type: appsv1.DeploymentProgressing, Status: corev1.ConditionFalse, Reason: "ProgressDeadlineExceeded", Message: `ReplicaSet "search-new" has timed out progressing.`}}
			case "old-pod-terminating":
				// The scheduler retries when the draining Pod is gone; waiting helps.
				now := metav1.Now()
				oldPod.DeletionTimestamp = &now
			}
			kube := fake.NewSimpleClientset(deployment, current, stale, oldPod, newPod, event)
			source := addedFieldsSource()
			live := source.DeepCopy()
			live.SetUID(deployment.UID)
			if err := RecordAcceptance(live, source); err != nil {
				t.Fatal(err)
			}
			ref := addedFieldsRef()
			bound := 6 * time.Second
			if mode == "old-pod-terminating" {
				bound = 1500 * time.Millisecond
			}
			executor := PlanExecutor{Client: contextAwareSourceClient{&planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: live}}}, Source: planFakeSource{source: source}, Rollouts: KubernetesPlanRollouts{Client: kube}, Verifier: planFakeVerifier{passed: true}, RolloutTimeout: bound}
			started := time.Now()
			deadline := started.Add(bound)
			result, err := executor.Apply(context.Background(), IncidentRequest{Namespace: ref.Namespace, SourceCommit: "accepted", RepairPolicy: "recorded-actions", ResponseDeadline: deadline}, reconcilePlan(ref), false)
			elapsed := time.Since(started)
			report := result.ActionReports[0].SourceReconcile
			if report == nil || report.ObservationError != "" || !strings.Contains(strings.Join(report.ResidualPaths, " "), "rollout.pods[app/old-pod]") {
				t.Fatalf("written object lost its readback: %+v", report)
			}
			if mode == "old-pod-terminating" {
				var partial *SourceReconcilePartialError
				if !errors.As(err, &partial) || result.RolloutPending != nil || elapsed < bound-500*time.Millisecond {
					t.Fatalf("draining rollout was not given its bounded wait: elapsed=%s err=%v", elapsed, err)
				}
				return
			}
			var pending *RolloutPendingError
			if !errors.As(err, &pending) || errors.Is(err, context.DeadlineExceeded) || result.RolloutPending == nil || len(result.RolloutPending.Blockers) == 0 {
				t.Fatalf("stalled rollout was not handed off as typed pending: %v", err)
			}
			if elapsed > bound/2 {
				t.Fatalf("stalled rollout spent the wait: %s", elapsed)
			}
			blocker := result.RolloutPending.Blockers[0]
			reason := result.RolloutPending.Reason + " " + blocker.Reason
			for _, want := range []string{"Deployment/search", "cannot progress", "Pod/old-pod", "ReplicaSet/search-old"} {
				if !strings.Contains(reason, want) {
					t.Fatalf("pending reason %q does not name %q", reason, want)
				}
			}
			if mode == "anti-affinity" && (!strings.Contains(reason, "Pod/new-pod") || !strings.Contains(reason, "anti-affinity")) {
				t.Fatalf("pending reason does not name the unschedulable replacement: %q", reason)
			}
			if result.VerificationPassed || !result.Applied || result.RolledBack || result.ActionReports[0].Status != "applied" {
				t.Fatalf("stall changed write or verification state: %+v", result)
			}
			encoded, encodeErr := result.Proto()
			if encodeErr != nil {
				t.Fatal(encodeErr)
			}
			restored := planApplicationState(encoded).Result
			if restored.RolloutPending == nil || !strings.Contains(restored.RolloutPending.Blockers[0].Reason, "Pod/old-pod") {
				t.Fatal("durable result lost the named blocker")
			}
			repairs := ProgramRepairReceipts([]RecordedPlanApplication{{ID: "stalled", Result: restored}}, nil)
			if len(repairs) != 1 || repairs[0].Success {
				t.Fatalf("stalled rollout earned repair credit: %+v", repairs)
			}
			for _, action := range kube.Actions() {
				if action.GetVerb() != "get" && action.GetVerb() != "list" {
					t.Fatalf("rollout observation mutated: %v", action)
				}
			}
		})
	}
}

// One slow object must not consume the readbacks of the others: every written
// object is read back inside the unchanged deadline.
func TestSourceReadbackBudgetIsPerObject(t *testing.T) {
	for _, tc := range []struct {
		mode string
		slow int
	}{{"slow-rollout", 3}, {"slow-readback", 0}, {"slow-readback", 4}, {"slow-readback", 9}} {
		mode, slow := tc.mode, tc.slow
		t.Run(fmt.Sprintf("%s/%d", mode, slow), func(t *testing.T) {
			const count = 10
			client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{}}
			sources := map[sdk.ObjectRef]*unstructured.Unstructured{}
			var kubeObjects []kruntime.Object
			var plan program.Plan
			for i := range count {
				source := addedFieldsSource()
				source.SetName(fmt.Sprintf("svc-%d", i))
				ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: source.GetNamespace(), Name: source.GetName()}
				live := source.DeepCopy()
				live.SetUID(types.UID(fmt.Sprintf("uid-%d", i)))
				if err := RecordAcceptance(live, source); err != nil {
					t.Fatal(err)
				}
				client.objects[ref], sources[ref] = live, source
				plan.Actions = append(plan.Actions, program.ReconcileFromSource{Ref: ref})
				var deployment appsv1.Deployment
				if err := kruntime.DefaultUnstructuredConverter.FromUnstructured(source.Object, &deployment); err != nil {
					t.Fatal(err)
				}
				deployment.UID, deployment.Generation = live.GetUID(), 1
				deployment.Status = appsv1.DeploymentStatus{ObservedGeneration: 1, Replicas: 1, UpdatedReplicas: 1, AvailableReplicas: 1, ReadyReplicas: 1}
				if i == slow && mode == "slow-rollout" {
					// Old capacity drains slowly; nothing makes it impossible.
					deployment.Status.Replicas, deployment.Status.AvailableReplicas = 2, 1
					controller := true
					one := int32(1)
					stale := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Namespace: ref.Namespace, Name: ref.Name + "-old", UID: "old-rs", OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: ref.Name, UID: deployment.UID, Controller: &controller}}},
						Spec: appsv1.ReplicaSetSpec{Replicas: &one, Template: *deployment.Spec.Template.DeepCopy()}, Status: appsv1.ReplicaSetStatus{Replicas: 1}}
					stale.Spec.Template.Spec.Containers[0].Image = "search:0"
					pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ref.Namespace, Name: ref.Name + "-old-pod", UID: "old-pod", OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: stale.Name, UID: stale.UID, Controller: &controller}}}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}
					kubeObjects = append(kubeObjects, stale, pod)
				}
				kubeObjects = append(kubeObjects, &deployment)
			}
			slowRef := plan.Actions[slow].(program.ReconcileFromSource).Ref
			reader := &hangingReadClient{planFakeClient: client, patched: map[sdk.ObjectRef]bool{}}
			if mode == "slow-readback" {
				reader.hang = slowRef
			}
			bound := 2 * time.Second
			executor := PlanExecutor{Client: reader, Source: mapPlanSource(sources), Rollouts: KubernetesPlanRollouts{Client: fake.NewSimpleClientset(kubeObjects...)}, Verifier: planFakeVerifier{passed: true}, RolloutTimeout: time.Minute}
			started := time.Now()
			result, err := executor.Apply(context.Background(), IncidentRequest{Namespace: slowRef.Namespace, SourceCommit: "accepted", RepairPolicy: "recorded-actions", ResponseDeadline: started.Add(bound)}, plan, false)
			if elapsed := time.Since(started); elapsed > bound+time.Second {
				t.Fatalf("readbacks extended the deadline: %s", elapsed)
			}
			var partial *SourceReconcilePartialError
			if !errors.As(err, &partial) || len(partial.Results) != 1 || result.VerificationPassed {
				t.Fatalf("slow object was not the only residual: %v", err)
			}
			for i, report := range result.ActionReports {
				source := report.SourceReconcile
				if source == nil {
					t.Fatalf("object %d has no readback", i)
				}
				if i == slow {
					if source.Converged || (mode == "slow-rollout" && !strings.Contains(strings.Join(source.ResidualPaths, " "), fmt.Sprintf("svc-%d-old-pod", slow))) || (mode == "slow-readback" && source.ObservationError == "") {
						t.Fatalf("slow object earned convergence: %+v", source)
					}
					continue
				}
				if !source.Converged || source.ObservationError != "" || len(source.ResidualPaths) != 0 {
					t.Fatalf("object %d lost its readback to the slow object: %+v", i, source)
				}
			}
		})
	}
}

type mapPlanSource map[sdk.ObjectRef]*unstructured.Unstructured

func (s mapPlanSource) AcceptedManifest(_ context.Context, _ string, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if object := s[ref]; object != nil {
		return object.DeepCopy(), nil
	}
	return nil, errors.New("missing source")
}

func (s mapPlanSource) Revision(context.Context, sdk.ObjectRef, string) (*unstructured.Unstructured, error) {
	return nil, errors.New("no revision")
}

// hangingReadClient models an API read of one written object that does not
// return until its caller's deadline.
type hangingReadClient struct {
	*planFakeClient
	hang    sdk.ObjectRef
	patched map[sdk.ObjectRef]bool
}

func (c *hangingReadClient) Get(ctx context.Context, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, bounded := ctx.Deadline(); bounded && ref == c.hang && c.patched[ref] {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return c.planFakeClient.Get(ctx, ref)
}

func (c *hangingReadClient) Patch(ctx context.Context, ref sdk.ObjectRef, kind types.PatchType, patch []byte) error {
	c.patched[ref] = true
	return c.planFakeClient.Patch(ctx, ref, kind, patch)
}
