package restart_loop

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sdo.dev/operator/sdk/sdktest"
)

func pod(name string, restarts int32, reason string) corev1.Pod {
	state := corev1.ContainerState{}
	if reason != "" {
		state.Waiting = &corev1.ContainerStateWaiting{Reason: reason}
	}
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "demo"},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
			{Name: "app", RestartCount: restarts, State: state},
		}},
	}
}

func TestFiresOnALoopingContainerWhateverThePodIsCalled(t *testing.T) {
	snapshot := sdktest.Snapshot{NamespaceName: "demo", PodList: []corev1.Pod{pod("renamed-worker-7", 5, "CrashLoopBackOff")}}
	findings, err := New().Detect(context.Background(), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 1 || findings[0].ParameterBindings["target_resource"].Name != "renamed-worker-7" {
		t.Fatalf("want one finding bound to the pod, got %#v", findings)
	}
}

func TestStaysQuietOnNearMisses(t *testing.T) {
	snapshot := sdktest.Snapshot{NamespaceName: "demo", PodList: []corev1.Pod{
		pod("few-restarts", 1, "CrashLoopBackOff"),
		pod("pulling-image", 9, "ImagePullBackOff"),
		pod("running", 9, ""),
	}}
	findings, err := New().Detect(context.Background(), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 0 {
		t.Fatalf("unexpected findings: %#v", findings)
	}
}
