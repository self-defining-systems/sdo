package install

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// A pod that never becomes ready names why: the timeout alone ("context deadline exceeded") cost a whole lane
// build to diagnose, while the pod's scheduling condition and events say it at once (no node fits, a volume pins
// it to a full node, an image cannot be pulled).
func TestWaitPodReadyTimeoutNamesWhyThePodIsNotReady(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset(
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: "sdo", Name: "sdo-repository-sync"},
			Status: corev1.PodStatus{
				Phase: corev1.PodPending,
				Conditions: []corev1.PodCondition{{
					Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: "Unschedulable",
					Message: "0/2 nodes are available: 1 Insufficient memory, 1 node(s) had volume node affinity conflict.",
				}},
			},
		},
		&corev1.Event{
			ObjectMeta:     metav1.ObjectMeta{Namespace: "sdo", Name: "e1"},
			InvolvedObject: corev1.ObjectReference{Kind: "Pod", Name: "sdo-repository-sync", Namespace: "sdo"},
			Type:           corev1.EventTypeWarning, Reason: "FailedScheduling", Message: "waiting for a volume",
		},
	)
	installer := Installer{
		Client:       client,
		SyncTimeout:  50 * time.Millisecond,
		PollInterval: time.Millisecond,
		Wait: func(ctx context.Context, _ time.Duration) error {
			<-ctx.Done() // the pod never changes: the wait ends only at its deadline
			return ctx.Err()
		},
	}
	err := installer.waitPodReady(ctx, "sdo", "sdo-repository-sync")
	if err == nil {
		t.Fatal("a pending pod was reported ready")
	}
	for _, want := range []string{"did not become ready", "Pending", "Insufficient memory", "volume node affinity conflict", "FailedScheduling"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not name %q", err, want)
		}
	}
}
