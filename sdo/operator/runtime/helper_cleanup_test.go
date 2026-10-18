package runtime

import (
	"context"
	"reflect"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

func helperPod(namespace string, name string, labels map[string]string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: labels}}
}

func TestHelperCleanerDeletesOnlyLabelledResponderHelpers(t *testing.T) {
	helper := map[string]string{ResponderHelperLabel: "true"}
	client := fake.NewSimpleClientset([]k8sruntime.Object{
		helperPod("shop", "curl-debug", helper),
		helperPod("shop", "frontend-abc", map[string]string{"app": "frontend"}),
		helperPod("sdo", "sdo-prober", map[string]string{"app.kubernetes.io/managed-by": "sdo"}),
		helperPod("sdo", "dns-check", helper),
		&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "mongo-probe", Namespace: "shop", Labels: helper}},
		&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "sdo-incident-1", Namespace: "sdo"}},
	}...)
	cleaner := KubernetesHelperCleaner{Client: client, Namespaces: []string{"shop", "sdo"}}

	deleted, err := cleaner.CleanupHelpers(context.Background())
	if err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	want := []string{"Job/shop/mongo-probe", "Pod/sdo/dns-check", "Pod/shop/curl-debug"}
	if !reflect.DeepEqual(deleted, want) {
		t.Fatalf("deleted %v, want %v", deleted, want)
	}
	for _, kept := range []struct{ namespace, name string }{{"shop", "frontend-abc"}, {"sdo", "sdo-prober"}} {
		if _, err := client.CoreV1().Pods(kept.namespace).Get(context.Background(), kept.name, metav1.GetOptions{}); err != nil {
			t.Fatalf("unlabelled pod %s/%s must survive: %v", kept.namespace, kept.name, err)
		}
	}
	if _, err := client.BatchV1().Jobs("sdo").Get(context.Background(), "sdo-incident-1", metav1.GetOptions{}); err != nil {
		t.Fatalf("the responder's own Job must survive: %v", err)
	}
}

type recordingCleaner struct {
	calls   int
	deleted []string
}

func (c *recordingCleaner) CleanupHelpers(context.Context) ([]string, error) {
	c.calls++
	return c.deleted, nil
}

func TestControllerCleansHelpersWhenTheResponderCompletesAndRecordsThemInTheClosure(t *testing.T) {
	interval := time.Second
	detector := controllerDetector("health", interval, stateFinding("selector"), stateFinding("selector"), sdk.Finding{})
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	controller, err := NewController(testControllerConfig(), []sdk.Detector{detector},
		staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "demo"}}, dispatcher, time.Unix(0, 0))
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	cleaner := &recordingCleaner{deleted: []string{"Pod/demo/curl-debug"}}
	controller.Helpers = cleaner
	var closed []IncidentClosure
	controller.OnIncidentClosed = func(closure IncidentClosure) { closed = append(closed, closure) }

	start := time.Unix(0, 0)
	for step := 0; step < 2; step++ {
		if err := controller.Step(context.Background(), start.Add(time.Duration(step)*interval), nil); err != nil {
			t.Fatalf("step %d: %v", step, err)
		}
	}
	executePendingEffect(t, controller)
	awaitRequest(t, dispatcher.requests)
	awaitDispatchCompletionQueued(t, controller)
	if cleaner.calls != 0 {
		t.Fatalf("helpers must not be cleaned while the responder may still use them")
	}
	for step := 2; step < 5 && len(closed) == 0; step++ {
		if err := controller.Step(context.Background(), start.Add(time.Duration(step)*interval), nil); err != nil {
			t.Fatalf("step %d: %v", step, err)
		}
	}
	if cleaner.calls != 1 {
		t.Fatalf("helpers must be cleaned exactly once after the responder completes, got %d", cleaner.calls)
	}
	if len(closed) != 1 || !reflect.DeepEqual(closed[0].CleanedHelpers, []string{"Pod/demo/curl-debug"}) {
		t.Fatalf("the closure must record the deleted helpers: %+v", closed)
	}
}
