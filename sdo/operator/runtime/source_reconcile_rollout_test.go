package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"sdo.dev/operator/sdk"
)

func reconcileRolloutObjects(t *testing.T) (*appsv1.Deployment, *appsv1.ReplicaSet, *appsv1.ReplicaSet) {
	t.Helper()
	var deployment appsv1.Deployment
	if err := kruntime.DefaultUnstructuredConverter.FromUnstructured(addedFieldsSource().Object, &deployment); err != nil {
		t.Fatal(err)
	}
	deployment.UID = "deployment-uid"
	deployment.ResourceVersion = "10"
	deployment.Generation = 2
	deployment.Status = appsv1.DeploymentStatus{ObservedGeneration: 2, Replicas: 2, UpdatedReplicas: 1, AvailableReplicas: 1, ReadyReplicas: 1}
	controller := true
	one := int32(1)
	current := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Namespace: deployment.Namespace, Name: "search-new", UID: "new-uid", ResourceVersion: "11", OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: deployment.Name, UID: deployment.UID, Controller: &controller}}}, Spec: appsv1.ReplicaSetSpec{Replicas: &one, Template: *deployment.Spec.Template.DeepCopy()}}
	stale := current.DeepCopy()
	stale.Name = "search-old"
	stale.UID = "old-uid"
	stale.Spec.Template.Spec.Containers = append(stale.Spec.Template.Spec.Containers, corev1.Container{Name: "old-sidecar", Image: "proxy:1"})
	current.Labels = map[string]string{appsv1.DefaultDeploymentUniqueLabelKey: "new"}
	current.Annotations = map[string]string{"deployment.kubernetes.io/revision": "2"}
	stale.Labels = map[string]string{appsv1.DefaultDeploymentUniqueLabelKey: "old"}
	stale.Annotations = map[string]string{"deployment.kubernetes.io/revision": "1"}
	stale.Status.Replicas = 1
	return &deployment, current, stale
}

func TestSourceReconcileRetiresUnavailableOldReplicaSetWithinBudget(t *testing.T) {
	deployment, current, stale := reconcileRolloutObjects(t)
	controller := true
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "unavailable", Namespace: stale.Namespace, Labels: map[string]string{appsv1.DefaultDeploymentUniqueLabelKey: "old"}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: stale.Name, UID: stale.UID, Controller: &controller}}}, Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}}}}
	client := fake.NewSimpleClientset(deployment, current, stale, pod)
	// Model the Deployment controller's status after the scale-down. The real
	// progress writer must produce this mutation before the real waiter can pass.
	client.PrependReactor("patch", "replicasets", func(action ktesting.Action) (bool, kruntime.Object, error) {
		patch := action.(ktesting.PatchAction)
		if patch.GetPatchType() != types.MergePatchType {
			t.Fatal("scale must use conditional merge patch")
		}
		var fields map[string]any
		_ = json.Unmarshal(patch.GetPatch(), &fields)
		metadata, _ := fields["metadata"].(map[string]any)
		if metadata["uid"] != "old-uid" || metadata["resourceVersion"] != "11" {
			t.Fatalf("scale lost identity guards: %s", patch.GetPatch())
		}
		// Tracker emulates the API's patch and the next controller status update.
		stale.Spec.Replicas = new(int32)
		if err := client.Tracker().Update(appsv1.SchemeGroupVersion.WithResource("replicasets"), stale, stale.Namespace); err != nil {
			return true, nil, err
		}
		deployment.Status.Replicas = 1
		deployment.Status.AvailableReplicas = 1
		deployment.Status.ReadyReplicas = 1
		if err := client.Tracker().Update(appsv1.SchemeGroupVersion.WithResource("deployments"), deployment, deployment.Namespace); err != nil {
			return true, nil, err
		}
		return true, stale.DeepCopy(), nil
	})
	source := addedFieldsSource()
	live := source.DeepCopy()
	live.SetUID(deployment.UID)
	if err := RecordAcceptance(live, source); err != nil {
		t.Fatal(err)
	}
	ref := addedFieldsRef()
	objectClient := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: live}}
	executor := PlanExecutor{Client: objectClient, Source: planFakeSource{source: source}, Rollouts: KubernetesPlanRollouts{Client: client}, Verifier: planFakeVerifier{passed: true}, RolloutTimeout: 100 * time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := executor.Apply(ctx, IncidentRequest{Namespace: ref.Namespace, SourceCommit: "accepted", RepairPolicy: "recorded-actions"}, reconcilePlan(ref), false)
	if err != nil || !result.VerificationPassed {
		t.Fatalf("old rollout did not converge in budget: %+v %v", result, err)
	}
	report := result.ActionReports[0].SourceReconcile
	if report == nil || !report.Converged || len(report.RolloutSteps) != 1 || report.RolloutSteps[0].Status != "scaled_to_zero" {
		t.Fatalf("missing progress receipt: %+v", report)
	}
	contract, err := result.Proto()
	if err != nil {
		t.Fatal(err)
	}
	if len(planApplicationState(contract).Result.ActionReports[0].SourceReconcile.RolloutSteps) != 1 {
		t.Fatal("durable receipt lost rollout step")
	}
}

func TestSourceReconcileRolloutProtectsControllerCapacityAndOwnership(t *testing.T) {
	for _, mode := range []string{"ready old capacity", "foreign owner", "no replacement", "replacement scaled down", "paused", "unobserved generation"} {
		t.Run(mode, func(t *testing.T) {
			deployment, current, stale := reconcileRolloutObjects(t)
			switch mode {
			case "ready old capacity":
				stale.Status.ReadyReplicas = 1
			case "foreign owner":
				stale.OwnerReferences[0].UID = "foreign-uid"
			case "no replacement":
				current.Spec.Template.Spec.Containers[0].Image = "different:1"
			case "replacement scaled down":
				current.Spec.Replicas = new(int32)
			case "paused":
				deployment.Spec.Paused = true
			case "unobserved generation":
				deployment.Status.ObservedGeneration = 1
			}
			client := fake.NewSimpleClientset(deployment, current, stale)
			steps, err := (KubernetesPlanRollouts{Client: client}).ProgressSourceRollouts(context.Background(), []sdk.ObjectRef{addedFieldsRef()}, map[sdk.ObjectRef]types.UID{addedFieldsRef(): deployment.UID})
			if err != nil || len(steps) > 0 {
				t.Fatalf("unsafe rollout progress: %+v %v", steps, err)
			}
			for _, action := range client.Actions() {
				if action.GetVerb() == "patch" {
					t.Fatal("protected ReplicaSet patched")
				}
			}
		})
	}
}

func TestSourceReconcileRolloutProgressFailureIsTypedPartial(t *testing.T) {
	deployment, current, stale := reconcileRolloutObjects(t)
	client := fake.NewSimpleClientset(deployment, current, stale)
	// An accepted PATCH response is insufficient when admission/controller
	// interference leaves the old replica count unchanged on direct GET.
	client.PrependReactor("patch", "replicasets", func(ktesting.Action) (bool, kruntime.Object, error) { return true, stale.DeepCopy(), nil })
	source := addedFieldsSource()
	live := source.DeepCopy()
	live.SetUID(deployment.UID)
	if err := RecordAcceptance(live, source); err != nil {
		t.Fatal(err)
	}
	ref := addedFieldsRef()
	executor := PlanExecutor{Client: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: live}}, Source: planFakeSource{source: source}, Rollouts: KubernetesPlanRollouts{Client: client}, Verifier: planFakeVerifier{passed: true}, RolloutTimeout: 50 * time.Millisecond}
	result, err := executor.Apply(context.Background(), IncidentRequest{Namespace: ref.Namespace, SourceCommit: "accepted", RepairPolicy: "recorded-actions"}, reconcilePlan(ref), false)
	var partial *SourceReconcilePartialError
	if !errors.As(err, &partial) || !result.Applied || result.RolledBack || result.VerificationPassed {
		t.Fatalf("unconfirmed rollout cleanup classified incorrectly: %+v %v", result, err)
	}
	report := result.ActionReports[0].SourceReconcile
	if result.ActionReports[0].Status != "partial" || report.Converged || len(report.RolloutSteps) != 1 || report.RolloutSteps[0].Error == "" {
		t.Fatalf("missing partial progress receipt: %+v", report)
	}
}

func TestSourceReconcileRolloutSharesDeadline(t *testing.T) {
	deployment, current, stale := reconcileRolloutObjects(t)
	// Keep old ready capacity. The action must report it pending within its
	// existing budget, never force-delete it to manufacture convergence.
	stale.Status.ReadyReplicas = 1
	client := fake.NewSimpleClientset(deployment, current, stale)
	source := addedFieldsSource()
	live := source.DeepCopy()
	live.SetUID(deployment.UID)
	if err := RecordAcceptance(live, source); err != nil {
		t.Fatal(err)
	}
	ref := addedFieldsRef()
	executor := PlanExecutor{Client: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: live}}, Source: planFakeSource{source: source}, Rollouts: KubernetesPlanRollouts{Client: client}, Verifier: planFakeVerifier{passed: true}, RolloutTimeout: time.Minute}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	started := time.Now()
	result, err := executor.Apply(ctx, IncidentRequest{Namespace: ref.Namespace, SourceCommit: "accepted", RepairPolicy: "recorded-actions"}, reconcilePlan(ref), false)
	var pending *RolloutPendingError
	if !errors.As(err, &pending) || result.RolloutPending == nil || result.VerificationPassed || time.Since(started) > time.Second {
		t.Fatalf("progress exceeded original context or claimed completion: %+v %v", result, err)
	}
	for _, action := range client.Actions() {
		if action.GetVerb() == "patch" {
			t.Fatal("ready old capacity was patched")
		}
	}
}

func TestSourceReconcilePollOwnsProgressUntilReplacementAppears(t *testing.T) {
	deployment, current, stale := reconcileRolloutObjects(t)
	client := fake.NewSimpleClientset(deployment, current, stale)
	firstList := true
	client.PrependReactor("list", "replicasets", func(ktesting.Action) (bool, kruntime.Object, error) {
		if firstList {
			firstList = false
			return true, &appsv1.ReplicaSetList{Items: []appsv1.ReplicaSet{*stale.DeepCopy()}}, nil
		}
		return false, nil, nil
	})
	client.PrependReactor("patch", "replicasets", func(ktesting.Action) (bool, kruntime.Object, error) {
		stale.Spec.Replicas = new(int32)
		if err := client.Tracker().Update(appsv1.SchemeGroupVersion.WithResource("replicasets"), stale, stale.Namespace); err != nil {
			return true, nil, err
		}
		deployment.Status.Replicas = 1
		if err := client.Tracker().Update(appsv1.SchemeGroupVersion.WithResource("deployments"), deployment, deployment.Namespace); err != nil {
			return true, nil, err
		}
		return true, stale.DeepCopy(), nil
	})
	source := addedFieldsSource()
	live := source.DeepCopy()
	live.SetUID(deployment.UID)
	if err := RecordAcceptance(live, source); err != nil {
		t.Fatal(err)
	}
	ref := addedFieldsRef()
	executor := PlanExecutor{Client: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: live}}, Source: planFakeSource{source: source}, Rollouts: KubernetesPlanRollouts{Client: client}, Verifier: planFakeVerifier{passed: true}, PollRollouts: true, RolloutTimeout: 2 * time.Second}
	result, err := executor.Apply(context.Background(), IncidentRequest{Namespace: ref.Namespace, SourceCommit: "accepted", RepairPolicy: "recorded-actions"}, reconcilePlan(ref), false)
	if err != nil || !result.VerificationPassed || len(result.ActionReports[0].SourceReconcile.RolloutSteps) != 1 {
		t.Fatalf("source action abandoned progress before replacement observation: pending=%v error=%v", result.RolloutPending, err)
	}
}

func TestSourceReconcileProgressBindsWrittenWorkloadInstance(t *testing.T) {
	deployment, current, stale := reconcileRolloutObjects(t)
	// The progress read sees a new Deployment instance at the same name after
	// the executor's successful write/readback of the original instance.
	client := fake.NewSimpleClientset(deployment, current, stale)
	source := addedFieldsSource()
	live := source.DeepCopy()
	live.SetUID("original-instance")
	if err := RecordAcceptance(live, source); err != nil {
		t.Fatal(err)
	}
	ref := addedFieldsRef()
	executor := PlanExecutor{Client: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: live}}, Source: planFakeSource{source: source}, Rollouts: KubernetesPlanRollouts{Client: client}, Verifier: planFakeVerifier{passed: true}, RolloutTimeout: 30 * time.Millisecond}
	result, err := executor.Apply(context.Background(), IncidentRequest{Namespace: ref.Namespace, SourceCommit: "accepted", RepairPolicy: "recorded-actions"}, reconcilePlan(ref), false)
	if err == nil || result.VerificationPassed {
		t.Fatal("recreated workload received write/rollout credit")
	}
	for _, action := range client.Actions() {
		if action.GetVerb() == "patch" {
			t.Fatal("source action mutated ReplicaSets of a different Deployment instance")
		}
	}
}

func TestSourceReconcileRolloutProtectsFreshPodCapacity(t *testing.T) {
	for _, mode := range []string{"ready pod", "pod becomes ready", "newest revision", "same hash", "missing hash", "fresh RS ready", "current pod hash", "newest creation", "unknown revision"} {
		t.Run(mode, func(t *testing.T) {
			deployment, current, stale := reconcileRolloutObjects(t)
			controller := true
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "serving", Namespace: stale.Namespace, Labels: map[string]string{appsv1.DefaultDeploymentUniqueLabelKey: "old"}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: stale.Name, UID: stale.UID, Controller: &controller}}}}
			switch mode {
			case "ready pod", "pod becomes ready":
				pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
			case "newest revision":
				stale.Annotations["deployment.kubernetes.io/revision"] = "3"
			case "same hash":
				stale.Labels[appsv1.DefaultDeploymentUniqueLabelKey] = "new"
			case "missing hash":
				delete(stale.Labels, appsv1.DefaultDeploymentUniqueLabelKey)
			case "current pod hash":
				pod.Labels[appsv1.DefaultDeploymentUniqueLabelKey] = "new"
			case "newest creation":
				stale.CreationTimestamp = metav1.NewTime(time.Unix(10, 0))
			case "unknown revision":
				delete(stale.Annotations, "deployment.kubernetes.io/revision")
			}
			client := fake.NewSimpleClientset(deployment, current, stale, pod)
			if mode == "fresh RS ready" {
				client.PrependReactor("get", "replicasets", func(action ktesting.Action) (bool, kruntime.Object, error) {
					if action.(ktesting.GetAction).GetName() == stale.Name {
						fresh := stale.DeepCopy()
						fresh.Status.ReadyReplicas = 1
						return true, fresh, nil
					}
					return false, nil, nil
				})
			}
			if mode == "pod becomes ready" {
				reads := 0
				client.PrependReactor("list", "pods", func(ktesting.Action) (bool, kruntime.Object, error) {
					reads++
					if reads == 1 {
						return true, &corev1.PodList{}, nil
					}
					return false, nil, nil
				})
			}
			steps, err := (KubernetesPlanRollouts{Client: client}).ProgressSourceRollouts(context.Background(), []sdk.ObjectRef{addedFieldsRef()}, map[sdk.ObjectRef]types.UID{addedFieldsRef(): deployment.UID})
			if err != nil || len(steps) != 0 {
				t.Fatalf("protected rollout changed: %v %v", steps, err)
			}
			for _, action := range client.Actions() {
				if action.GetVerb() == "patch" {
					t.Fatal("protected capacity scaled down")
				}
			}
		})
	}
}
