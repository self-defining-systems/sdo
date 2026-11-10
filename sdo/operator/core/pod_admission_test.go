package core

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestObservePodAdmissionSubmitsTheTemplateAndRecordsMutations(t *testing.T) {
	yes := true
	template := corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "api", "pod-template-hash": "abc"}},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "api", Image: "registry.example/api:1"}}},
	}
	running := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "api-abc-1", Namespace: "app", Labels: template.Labels,
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "api-abc", Controller: &yes}}},
		// An earlier admission already wrote a limit into the running Pod.
		Spec:   corev1.PodSpec{Containers: []corev1.Container{{Name: "api", Image: "registry.example/api:1", Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("8Mi")}}}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	view := DetectionSnapshot{NamespaceName: "app", PodList: []corev1.Pod{running},
		ReplicaSetList: []appsv1.ReplicaSet{{ObjectMeta: metav1.ObjectMeta{Name: "api-abc", Namespace: "app"}, Spec: appsv1.ReplicaSetSpec{Template: template}}}}
	client := fake.NewSimpleClientset()
	client.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		pod := action.(k8stesting.CreateAction).GetObject().(*corev1.Pod).DeepCopy()
		pod.Spec.Containers[0].Resources.Limits = corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("8Mi")}
		return true, pod, nil
	})
	result, submitted := ObservePodAdmission(context.Background(), client, view)
	if !result.Admitted || result.SourcePod != "api-abc-1" || submitted == nil {
		t.Fatalf("result = %+v", result)
	}
	if len(submitted.Spec.Containers[0].Resources.Limits) != 0 {
		t.Fatal("the submitted Pod must come from the template, not the already-mutated Pod")
	}
	if len(result.Mutations) != 1 || result.Mutations[0].Path != "spec.containers[api].resources.limits.memory" || result.Mutations[0].After != `"8Mi"` {
		t.Fatalf("mutations = %+v", result.Mutations)
	}
}
