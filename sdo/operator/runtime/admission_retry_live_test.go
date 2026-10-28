package runtime

import (
	"context"
	"os"
	"testing"
	"time"

	admissionv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// This opt-in smoke uses an explicitly supplied disposable cluster. It exercises
// the real API server and ReplicaSet controller, without a model or benchmark.
func TestAdmissionRetryLive(t *testing.T) {
	kubeconfig := os.Getenv("SDO_ADMISSION_RETRY_KUBECONFIG")
	if kubeconfig == "" {
		t.Skip("set SDO_ADMISSION_RETRY_KUBECONFIG to a disposable cluster")
	}
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		t.Fatal(err)
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	namespace, err := client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "sdo-admission-retry-", Labels: map[string]string{"sdo.dev/admission-retry-smoke": "true"}}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = client.CoreV1().Namespaces().Delete(context.Background(), namespace.Name, metav1.DeleteOptions{})
	})
	failure := admissionv1.Fail
	sideEffects := admissionv1.SideEffectClassNone
	timeout := int32(1)
	webhook := &admissionv1.ValidatingWebhookConfiguration{ObjectMeta: metav1.ObjectMeta{Name: namespace.Name}, Webhooks: []admissionv1.ValidatingWebhook{{Name: "retry.example.invalid", FailurePolicy: &failure, SideEffects: &sideEffects, TimeoutSeconds: &timeout, AdmissionReviewVersions: []string{"v1"}, NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"kubernetes.io/metadata.name": namespace.Name}}, ClientConfig: admissionv1.WebhookClientConfig{Service: &admissionv1.ServiceReference{Namespace: namespace.Name, Name: "missing"}}, Rules: []admissionv1.RuleWithOperations{{Operations: []admissionv1.OperationType{admissionv1.Create}, Rule: admissionv1.Rule{APIGroups: []string{""}, APIVersions: []string{"v1"}, Resources: []string{"pods"}}}}}}}
	_, err = client.AdmissionregistrationV1().ValidatingWebhookConfigurations().Create(ctx, webhook, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = client.AdmissionregistrationV1().ValidatingWebhookConfigurations().Delete(context.Background(), webhook.Name, metav1.DeleteOptions{})
	})
	// Establish admission failure before starting the controller's retry sequence.
	err = wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 10*time.Second, true, func(ctx context.Context) (bool, error) {
		_, err := client.CoreV1().Pods(namespace.Name).Create(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "probe"}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "pause", Image: "registry.k8s.io/pause:3.10.1"}}}}, metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}})
		return err != nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	replicas := int32(1)
	set := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: "blocked", Namespace: namespace.Name}, Spec: appsv1.ReplicaSetSpec{Replicas: &replicas, Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "retry"}}, Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "retry"}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "pause", Image: "registry.k8s.io/pause:3.10.1"}}}}}}
	if _, err = client.AppsV1().ReplicaSets(namespace.Name).Create(ctx, set, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	var failures int32
	err = wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
		events, err := client.CoreV1().Events(namespace.Name).List(ctx, metav1.ListOptions{})
		if err != nil {
			return false, err
		}
		for _, event := range events.Items {
			if event.Reason == "FailedCreate" && event.InvolvedObject.Name == set.Name {
				failures = event.Count
			}
		}
		return failures >= 14, nil
	})
	if err != nil {
		t.Fatalf("waiting for backoff: failures=%d: %v", failures, err)
	}
	started := time.Now()
	executor := PlanExecutor{Client: TypedPlanClient{Client: client}, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{false}}
	result, err := executor.Apply(ctx, IncidentRequest{Namespace: namespace.Name, RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.DeleteObject{Ref: sdk.ObjectRef{APIVersion: "admissionregistration.k8s.io/v1", Kind: "ValidatingWebhookConfiguration", Name: webhook.Name}, Reason: "remove missing admission backend"}}}, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.ActionsApplied) != 2 {
		t.Fatalf("missing recovery receipt: %+v", result)
	}
	err = wait.PollUntilContextTimeout(ctx, 50*time.Millisecond, 10*time.Second, true, func(ctx context.Context) (bool, error) {
		pods, err := client.CoreV1().Pods(namespace.Name).List(ctx, metav1.ListOptions{LabelSelector: "app=retry"})
		return err == nil && len(pods.Items) == 1, err
	})
	if err != nil {
		t.Fatalf("requeue did not bypass ReplicaSet backoff after %v: %v", time.Since(started), err)
	}
	t.Logf("after %d admission FailedCreate attempts, webhook delete + recorded requeue created Pod in %v", failures, time.Since(started))
	if result.VerificationPassed {
		t.Fatal("requeue was mistaken for independent application recovery")
	}
}
