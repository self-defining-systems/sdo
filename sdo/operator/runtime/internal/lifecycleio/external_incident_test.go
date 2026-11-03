package lifecycleio

import (
	"context"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"testing"
)

func TestExternalInboxUsesTypedGenericAlert(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	if alert, err := ReadExternalIncident(ctx, client, "control"); err != nil || alert != nil {
		t.Fatal(alert, err)
	}
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "sdo-external-incident", Namespace: "control"}, Data: map[string]string{"incident.json": `{"id":"external-1","namespace":"app","message":"Investigate the namespace"}`}}
	if _, err := client.CoreV1().ConfigMaps("control").Create(ctx, cm, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if alert, err := ReadExternalIncident(ctx, client, "control"); err != nil || alert.GetMessage() != "Investigate the namespace" {
		t.Fatal(alert, err)
	}
	cm.Data["incident.json"] = `{"id":"external-1","namespace":"app","message":"Investigate","hidden_problem":"fault"}`
	if _, err := client.CoreV1().ConfigMaps("control").Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadExternalIncident(ctx, client, "control"); err == nil {
		t.Fatal("untyped ingress context accepted")
	}
}
