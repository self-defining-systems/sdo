package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
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

func TestSourceReconcileReportsOldRolloutWithoutMutation(t *testing.T) {
	for _, mode := range []string{"unready", "ready", "zero with pod", "foreign", "history", "current", "identity changed", "sets unavailable", "pods unavailable"} {
		t.Run(mode, func(t *testing.T) {
			deployment, current, stale := reconcileRolloutObjects(t)
			deployment.Status.Replicas = 1
			controller := true
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "old-pod", Namespace: stale.Namespace, OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: stale.Name, UID: stale.UID, Controller: &controller}}}}
			objects := []kruntime.Object{deployment, current, stale, pod}
			switch mode {
			case "ready":
				pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
			case "zero with pod":
				stale.Spec.Replicas = new(int32)
				stale.Status.Replicas = 0
			case "foreign":
				stale.OwnerReferences[0].UID = "foreign"
			case "history":
				stale.Spec.Replicas = new(int32)
				stale.Status.Replicas = 0
				objects = objects[:3]
			case "current":
				stale.Spec.Template = *current.Spec.Template.DeepCopy()
			}
			client := fake.NewSimpleClientset(objects...)
			if mode == "identity changed" {
				replacement := deployment.DeepCopy()
				replacement.UID = "replacement"
				if err := client.Tracker().Update(appsv1.SchemeGroupVersion.WithResource("deployments"), replacement, replacement.Namespace); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "sets unavailable" || mode == "pods unavailable" {
				resource := "replicasets"
				if mode == "pods unavailable" {
					resource = "pods"
				}
				client.PrependReactor("list", resource, func(ktesting.Action) (bool, kruntime.Object, error) { return true, nil, errors.New("unavailable") })
			}
			source := addedFieldsSource()
			live := source.DeepCopy()
			live.SetUID(deployment.UID)
			if err := RecordAcceptance(live, source); err != nil {
				t.Fatal(err)
			}
			ref := addedFieldsRef()
			executor := PlanExecutor{Client: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: live}}, Source: planFakeSource{source: source}, Rollouts: KubernetesPlanRollouts{Client: client}, Verifier: planFakeVerifier{passed: true}, PollRollouts: true}
			result, err := executor.Apply(context.Background(), IncidentRequest{Namespace: ref.Namespace, SourceCommit: "accepted", RepairPolicy: "recorded-actions"}, reconcilePlan(ref), false)
			report := result.ActionReports[0].SourceReconcile
			if mode == "unready" || mode == "ready" || mode == "zero with pod" {
				var pending *RolloutPendingError
				if !errors.As(err, &pending) || result.RolloutPending == nil || result.ActionReports[0].Status != "applied" || !result.Applied || result.RolledBack || result.VerificationPassed {
					t.Fatalf("missing typed residual: %+v %v", result, err)
				}
				paths := strings.Join(report.ResidualPaths, " ")
				if !strings.Contains(paths, "search-old") || !strings.Contains(paths, "old-pod") || report.Converged || len(report.RolloutSteps) != 0 {
					t.Fatalf("missing read-only rollout residual: %+v", report)
				}
			} else if mode == "identity changed" || mode == "sets unavailable" || mode == "pods unavailable" {
				var partial *SourceReconcilePartialError
				if !errors.As(err, &partial) || report.Converged || report.ObservationError == "" || result.VerificationPassed {
					t.Fatalf("unknown rollout earned credit: %+v %v", report, err)
				}
			} else if report == nil || !report.Converged || len(report.ResidualPaths) != 0 {
				t.Fatalf("unrelated rollout reported: %+v %v", report, err)
			}
			encoded, encodeErr := result.Proto()
			if encodeErr != nil {
				t.Fatal(encodeErr)
			}
			restored := planApplicationState(encoded).Result.ActionReports[0].SourceReconcile
			if strings.Join(restored.ResidualPaths, " ") != strings.Join(report.ResidualPaths, " ") || restored.ObservationError != report.ObservationError {
				t.Fatal("durable result lost residual")
			}
			for _, action := range client.Actions() {
				if action.GetVerb() != "get" && action.GetVerb() != "list" {
					t.Fatalf("rollout mutated: %v", action)
				}
			}
		})
	}
}

// Exercise actual Kubernetes old-template observation through the executor and
// persisted repair projection, including old Pods outliving complete status.
func TestSourceConvergenceWaitsForKubernetesOldObjects(t *testing.T) {
	for _, mode := range []string{"drains", "bounded-residual", "rollout-blocked"} {
		drains := mode == "drains"
		t.Run(mode, func(t *testing.T) {
			deployment, current, stale := reconcileRolloutObjects(t)
			if mode != "rollout-blocked" {
				deployment.Status.Replicas = 1
			}
			controller := true
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "old-pod", Namespace: stale.Namespace, OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: stale.Name, UID: stale.UID, Controller: &controller}}}}
			kube := fake.NewSimpleClientset(deployment, current, stale, pod)
			observations := 0
			kube.PrependReactor("list", "replicasets", func(ktesting.Action) (bool, kruntime.Object, error) {
				observations++
				if drains && observations == 3 {
					stale.Spec.Replicas = new(int32)
					stale.Status.Replicas = 0
					if err := kube.Tracker().Update(appsv1.SchemeGroupVersion.WithResource("replicasets"), stale, stale.Namespace); err != nil {
						t.Fatal(err)
					}
					if err := kube.Tracker().Delete(corev1.SchemeGroupVersion.WithResource("pods"), pod.Namespace, pod.Name); err != nil {
						t.Fatal(err)
					}
				}
				return false, nil, nil
			})
			source := addedFieldsSource()
			live := source.DeepCopy()
			live.SetUID(deployment.UID)
			if err := RecordAcceptance(live, source); err != nil {
				t.Fatal(err)
			}
			ref := addedFieldsRef()
			executor := PlanExecutor{Client: contextAwareSourceClient{&planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: live}}}, Source: planFakeSource{source: source}, Rollouts: KubernetesPlanRollouts{Client: kube}, Verifier: planFakeVerifier{passed: true}, RolloutTimeout: time.Second}
			deadline := time.Now().Add(350 * time.Millisecond)
			result, err := executor.Apply(context.Background(), IncidentRequest{Namespace: ref.Namespace, SourceCommit: "accepted", RepairPolicy: "recorded-actions", ResponseDeadline: deadline}, reconcilePlan(ref), false)
			report := result.ActionReports[0].SourceReconcile
			if drains {
				if err != nil || !report.Converged || !result.VerificationPassed || observations < 3 {
					t.Fatalf("draining rollout failed: report=%v observations=%d err=%v", report, observations, err)
				}
			} else {
				var partial *SourceReconcilePartialError
				if !errors.As(err, &partial) || report.Converged || result.VerificationPassed || time.Now().Before(deadline) || !strings.Contains(strings.Join(report.ResidualPaths, " "), "old-pod") {
					t.Fatalf("deadline lost residual: report=%v err=%v", report, err)
				}
			}
			encoded, encodeErr := result.Proto()
			if encodeErr != nil {
				t.Fatal(encodeErr)
			}
			repairs := ProgramRepairReceipts([]RecordedPlanApplication{{ID: "kubernetes-source", Result: planApplicationState(encoded).Result}}, nil)
			if len(repairs) != 1 || repairs[0].Success != drains {
				t.Fatalf("wrong source repair credit: %+v", repairs)
			}
		})
	}
}

type contextAwareSourceClient struct{ *planFakeClient }

func (c contextAwareSourceClient) Get(ctx context.Context, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return c.planFakeClient.Get(ctx, ref)
}
