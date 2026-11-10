package kubereads_test

import (
	"context"
	"testing"

	admissionv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"sdo.dev/operator/kubereads"
	"sdo.dev/operator/sdk"
)

func podWebhook(name string, objectSelector *metav1.LabelSelector, timeout *int32) admissionv1.MutatingWebhook {
	url := "https://172.18.0.1:9443/mutate?token=secret"
	return admissionv1.MutatingWebhook{
		Name:              name,
		ClientConfig:      admissionv1.WebhookClientConfig{URL: &url},
		Rules:             []admissionv1.RuleWithOperations{{Operations: []admissionv1.OperationType{admissionv1.Create}, Rule: admissionv1.Rule{APIGroups: []string{""}, APIVersions: []string{"v1"}, Resources: []string{"pods"}}}},
		NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"kubernetes.io/metadata.name": "app"}},
		ObjectSelector:    objectSelector,
		TimeoutSeconds:    timeout,
	}
}

func admissionClient() *fake.Clientset {
	five := int32(5)
	labels := map[string]string{"app": "api"}
	return fake.NewSimpleClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "app", Labels: map[string]string{"kubernetes.io/metadata.name": "app"}}},
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"}, Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}}}},
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "worker", Namespace: "app"}, Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "worker"}}}}},
		&admissionv1.MutatingWebhookConfiguration{ObjectMeta: metav1.ObjectMeta{Name: "pod-defaults"}, Webhooks: []admissionv1.MutatingWebhook{
			podWebhook("api.pods.example", &metav1.LabelSelector{MatchLabels: labels}, &five),
			podWebhook("all.pods.example", nil, nil),
		}},
	)
}

func TestAdmissionWebhooksForHonorsObjectSelector(t *testing.T) {
	client := admissionClient()
	ctx := context.Background()
	api, err := kubereads.AdmissionWebhooksFor(ctx, client, "", sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"})
	if err != nil {
		t.Fatal(err)
	}
	if len(api) != 2 {
		t.Fatalf("api webhooks = %+v", api)
	}
	worker, err := kubereads.AdmissionWebhooksFor(ctx, client, "", sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "worker"})
	if err != nil {
		t.Fatal(err)
	}
	if len(worker) != 1 || worker[0].Webhook != "all.pods.example" {
		t.Fatalf("an objectSelector for other Pods must not select worker: %+v", worker)
	}
	byName := map[string]sdk.AdmissionWebhook{}
	for _, w := range api {
		byName[w.Webhook] = w
	}
	if byName["api.pods.example"].TimeoutSeconds != 5 || byName["all.pods.example"].TimeoutSeconds != sdk.DefaultWebhookTimeoutSeconds {
		t.Fatalf("timeouts = %+v", api)
	}
	if byName["api.pods.example"].URL != "https://172.18.0.1:9443" || byName["api.pods.example"].ObjectSelector == nil || !byName["api.pods.example"].Mutating() {
		t.Fatalf("projection = %+v", byName["api.pods.example"])
	}
	namespace, err := kubereads.NamespaceAdmissionWebhooks(ctx, client, "", "app")
	if err != nil || len(namespace) != 2 {
		t.Fatalf("namespace webhooks = %+v, %v", namespace, err)
	}
	pod, err := kubereads.PodAdmissionWebhooks(ctx, client, "", "app", map[string]string{"app": "worker"})
	if err != nil || len(pod) != 1 {
		t.Fatalf("pod webhooks = %+v, %v", pod, err)
	}
}
