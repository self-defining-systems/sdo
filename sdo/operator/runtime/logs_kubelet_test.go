package runtime

import (
	"context"
	"os"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/logs"
	"sdo.dev/operator/sdk/sdktest"
)

// Opt-in kubelet contract check. The disposable cluster must have the
// sdo-log-smoke:v1 fixture image, emitting one timestamped error per second
// and exiting every ten seconds. No fixture logs are written to the repo.
func TestLogsRealKubelet(t *testing.T) {
	path := os.Getenv("SDO_LOGS_KUBECONFIG")
	if path == "" {
		t.Skip("set SDO_LOGS_KUBECONFIG for disposable kubelet check")
	}
	cfg, err := clientcmd.BuildConfigFromFlags("", path)
	if err != nil {
		t.Fatal(err)
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pod, err := client.CoreV1().Pods("default").Create(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{GenerateName: "logs-check-"}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "logs", Image: "sdo-log-smoke:v1", ImagePullPolicy: corev1.PullNever}}}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = client.CoreV1().Pods("default").Delete(context.Background(), pod.Name, metav1.DeleteOptions{})
	}()
	d := logDetector{queries: []logs.Query{{ID: "q", Patterns: []logs.Pattern{{ID: "bad", RE2: "error"}}, Previous: true}}}
	o, err := NewLogsObserver(client, []sdk.Detector{d}, LogsObserverOptions{})
	if err != nil {
		t.Fatal(err)
	}
	observed, previous := false, false
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			t.Fatalf("kubelet check timed out; current=%v previous=%v", observed, previous)
		case <-ticker.C:
		}
		pod, err = client.CoreV1().Pods("default").Get(ctx, pod.Name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if len(pod.Status.ContainerStatuses) == 0 || pod.Status.ContainerStatuses[0].State.Running == nil {
			continue
		}
		r := o.collect(ctx, sdktest.Snapshot{NamespaceName: "default", PodList: []corev1.Pod{*pod}})["q"]
		if len(r.Windows) == 0 {
			continue
		}
		cycles := r.Windows[0].Cycles
		c := cycles[len(cycles)-1]
		if c.Error != "" {
			t.Fatal(c.Error)
		}
		if c.Lines > 0 {
			observed = true
		}
		if c.Previous != nil {
			if c.Previous.Error != "" {
				t.Fatal(c.Previous.Error)
			}
			previous = c.Previous.Matches["bad"].Count > 0
		}
		// Immediate duplicate read must not count any line twice.
		r2 := o.collect(ctx, sdktest.Snapshot{NamespaceName: "default", PodList: []corev1.Pod{*pod}})["q"]
		last := r2.Windows[0].Cycles[len(r2.Windows[0].Cycles)-1]
		if last.Lines > 1 {
			t.Fatalf("sinceTime recounted historical lines: %+v", last)
		}
		if observed && previous {
			return
		}
	}
}
