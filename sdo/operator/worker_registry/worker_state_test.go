package worker_registry

import (
	"context"
	"reflect"
	"sort"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/latefindings"
)

// Every ConfigMap the registry can name for a worker task is worker state.
func TestIsWorkerConfigMapCoversEveryRegistryName(t *testing.T) {
	task := reflectionTestTask(t)
	digest := strings.Repeat("a", 64)
	jobs := []string{JobName(task.TaskId), JobName("health-refresh-x"), TaskJobName(task), standbyJobPrefix + "0123456789abcdef"}
	var names []string
	for _, job := range jobs {
		for _, suffix := range workerJobSuffixes {
			names = append(names, job+suffix)
		}
	}
	names = append(names, incidentExecutionName(digest), latefindings.ConfigMapName("incident-1"))
	for _, name := range names {
		if !IsWorkerConfigMap(name) {
			t.Errorf("registry name %q is not recognized as worker state", name)
		}
	}
	for _, name := range []string{"kube-root-ca.crt", "sdo-agent-registrations", "sdo-controller-maintenance", "sdo-controller-state", "sdo-controller-state-drift-verdicts", "sdo-namespace-onboarding", "sdo-namespace-onboarding-pending", "sdo-request", "application-request", "sdo-"} {
		if IsWorkerConfigMap(name) {
			t.Errorf("%q must survive a worker-state reset", name)
		}
	}
}

func TestResetWorkerStateDeletesOnlyWorkerConfigMaps(t *testing.T) {
	workers := []string{
		"sdo-health-refresh-synthetic-aaaaaaaaaa-request", "sdo-health-refresh-synthetic-aaaaaaaaaa-execution",
		"sdo-demo-app-1-bbbbbbbbbb-request", "sdo-demo-app-1-bbbbbbbbbb-execution", "sdo-demo-app-1-bbbbbbbbbb-result",
		"sdo-standby-0123456789abcdef-request", "sdo-standby-0123456789abcdef-result", "sdo-standby-0123456789abcdef-binding",
		incidentExecutionName(strings.Repeat("c", 64)), latefindings.ConfigMapName("incident-1"),
	}
	kept := []string{"kube-root-ca.crt", "sdo-agent-registrations", "sdo-controller-maintenance", "sdo-namespace-onboarding-pending", "application"}
	var objects []runtime.Object
	for _, name := range append(append([]string{}, workers...), kept...) {
		objects = append(objects, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "demo-sdo"}})
	}
	objects = append(objects, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "sdo-credentials-request", Namespace: "demo-sdo"}})
	client := fake.NewSimpleClientset(objects...)

	deleted, err := ResetWorkerState(context.Background(), client, "demo-sdo")
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(deleted)
	want := append([]string{}, workers...)
	sort.Strings(want)
	if !reflect.DeepEqual(deleted, want) {
		t.Fatalf("deleted %v, want %v", deleted, want)
	}
	for _, action := range client.Actions() {
		if action.GetResource().Resource != "configmaps" {
			t.Fatalf("unexpected API access: %v", action)
		}
	}
	left, err := client.CoreV1().ConfigMaps("demo-sdo").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var remaining []string
	for _, cm := range left.Items {
		remaining = append(remaining, cm.Name)
	}
	sort.Strings(remaining)
	sort.Strings(kept)
	if !reflect.DeepEqual(remaining, kept) {
		t.Fatalf("remaining %v, want %v", remaining, kept)
	}
	if _, err := client.CoreV1().Secrets("demo-sdo").Get(context.Background(), "sdo-credentials-request", metav1.GetOptions{}); err != nil {
		t.Fatalf("Secret removed: %v", err)
	}
	if again, err := ResetWorkerState(context.Background(), client, "demo-sdo"); err != nil || len(again) != 0 {
		t.Fatalf("not idempotent: %v %v", again, err)
	}
	if _, err := ResetWorkerState(context.Background(), client, ""); err == nil {
		t.Fatal("empty namespace accepted")
	}
}
