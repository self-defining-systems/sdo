package runtime

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
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
