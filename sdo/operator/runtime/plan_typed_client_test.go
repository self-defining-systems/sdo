package runtime

import (
	"context"
	"testing"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"

	"sdo.dev/operator/sdk"
)

func TestTypedPlanClientReadsAndPatchesService(t *testing.T) {
	client := fake.NewSimpleClientset(&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"}, Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 80}}}})
	planClient := TypedPlanClient{Client: client}
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	object, err := planClient.Get(context.Background(), ref)
	if err != nil || object.GetName() != "api" {
		t.Fatalf("get: %+v %v", object, err)
	}
	if err := planClient.Patch(context.Background(), ref, types.MergePatchType, []byte(`{"spec":{"ports":[{"port":81}]}}`)); err != nil {
		t.Fatal(err)
	}
	updated, err := client.CoreV1().Services("app").Get(context.Background(), "api", metav1.GetOptions{})
	if err != nil || updated.Spec.Ports[0].Port != 81 {
		t.Fatalf("patch: %+v %v", updated, err)
	}
}

func TestTypedPlanClientStructuredKinds(t *testing.T) {
	client := fake.NewSimpleClientset(
		&networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "app"}},
		&autoscalingv2.HorizontalPodAutoscaler{ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "app"}},
		&policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "app"}},
	)
	for _, ref := range []sdk.ObjectRef{
		{APIVersion: "networking.k8s.io/v1", Kind: "NetworkPolicy", Namespace: "app", Name: "app"},
		{APIVersion: "autoscaling/v2", Kind: "HorizontalPodAutoscaler", Namespace: "app", Name: "app"},
		{APIVersion: "policy/v1", Kind: "PodDisruptionBudget", Namespace: "app", Name: "app"},
	} {
		c := TypedPlanClient{Client: client}
		if _, err := c.Get(context.Background(), ref); err != nil {
			t.Fatal(err)
		}
		if err := c.Patch(context.Background(), ref, types.MergePatchType, []byte(`{"metadata":{"labels":{"fixture":"true"}}}`)); err != nil {
			t.Fatal(err)
		}
	}
	for _, kind := range []string{"Secret", "ServiceAccount", "Role", "Node", "Namespace"} {
		if _, err := (TypedPlanClient{Client: client}).Get(context.Background(), sdk.ObjectRef{APIVersion: "v1", Kind: kind, Namespace: "app", Name: "app"}); err == nil {
			t.Fatalf("read forbidden kind %s", kind)
		}
	}
}
