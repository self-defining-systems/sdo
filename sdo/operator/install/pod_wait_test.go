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

type stepClock struct{ now time.Time }

func (c *stepClock) Now() time.Time { return c.now }

func podWithClaim(ready bool) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "sdo", Name: "sdo-repository-sync"},
		Spec: corev1.PodSpec{Volumes: []corev1.Volume{{
			Name:         "repo",
			VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "repo"}},
		}}},
		Status: corev1.PodStatus{Phase: corev1.PodPending},
	}
	if ready {
		pod.Status.Phase = corev1.PodRunning
		pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	}
	return pod
}

func claim(phase corev1.PersistentVolumeClaimPhase) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: "sdo", Name: "repo"},
		Status:     corev1.PersistentVolumeClaimStatus{Phase: phase},
	}
}

// A provisioner that takes longer than the stall limit to bind a claim is waited on while it works: the limit
// restarts on progress, and the Pod's own start after the bind gets a fresh limit.
func TestWaitPodReadyWaitsPastStallLimitWhileClaimProvisions(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset(podWithClaim(false), claim(corev1.ClaimPending))
	clock := &stepClock{now: time.Unix(1000, 0)}
	polls := 0
	installer := Installer{
		Client:       client,
		SyncTimeout:  30 * time.Second,
		PollInterval: 10 * time.Second,
		Now:          clock.Now,
		Wait: func(context.Context, time.Duration) error {
			clock.now = clock.now.Add(10 * time.Second)
			polls++
			if polls == 100 { // 1000 s pending, far over the 30 s limit
				_ = client.CoreV1().PersistentVolumeClaims("sdo").Delete(ctx, "repo", metav1.DeleteOptions{})
				_, _ = client.CoreV1().PersistentVolumeClaims("sdo").Create(ctx, claim(corev1.ClaimBound), metav1.CreateOptions{})
			}
			if polls == 102 {
				_, _ = client.CoreV1().Pods("sdo").Update(ctx, podWithClaim(true), metav1.UpdateOptions{})
			}
			return nil
		},
	}
	if err := installer.waitPodReady(ctx, "sdo", "sdo-repository-sync"); err != nil {
		t.Fatalf("a slow but progressing Pod failed: %v", err)
	}
}

// A Pod that advances nothing for the stall limit fails with the evidence, as the fixed deadline did.
func TestWaitPodReadyFailsWhenNothingProgressesForStallLimit(t *testing.T) {
	client := fake.NewSimpleClientset(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "sdo", Name: "sdo-repository-sync"},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "sync", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff", Message: "pull denied"}},
			}},
		},
	})
	clock := &stepClock{now: time.Unix(1000, 0)}
	installer := Installer{
		Client:       client,
		SyncTimeout:  30 * time.Second,
		PollInterval: 10 * time.Second,
		Now:          clock.Now,
		Wait: func(context.Context, time.Duration) error {
			clock.now = clock.now.Add(10 * time.Second)
			return nil
		},
	}
	err := installer.waitPodReady(context.Background(), "sdo", "sdo-repository-sync")
	if err == nil {
		t.Fatal("a stalled pod was reported ready")
	}
	for _, want := range []string{"did not become ready", "no progress for 30s", "ImagePullBackOff", "pull denied"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not name %q", err, want)
		}
	}
	if elapsed := clock.now.Sub(time.Unix(1000, 0)); elapsed > 40*time.Second {
		t.Fatalf("waited %s past a 30s stall limit", elapsed)
	}
}

// A claim whose provisioning failed is not waited on: the cluster's own message is the evidence.
func TestWaitPodReadyFailsAtOnceWhenClaimProvisioningFailed(t *testing.T) {
	client := fake.NewSimpleClientset(podWithClaim(false), claim(corev1.ClaimPending), &corev1.Event{
		ObjectMeta:     metav1.ObjectMeta{Namespace: "sdo", Name: "e1"},
		InvolvedObject: corev1.ObjectReference{Kind: "PersistentVolumeClaim", Name: "repo", Namespace: "sdo"},
		Type:           corev1.EventTypeWarning, Reason: "ProvisioningFailed", Message: "no space left for volume",
	})
	installer := Installer{
		Client:       client,
		SyncTimeout:  time.Hour,
		PollInterval: time.Millisecond,
		Wait: func(context.Context, time.Duration) error {
			t.Fatal("waited on a claim whose provisioning failed")
			return nil
		},
	}
	err := installer.waitPodReady(context.Background(), "sdo", "sdo-repository-sync")
	if err == nil || !strings.Contains(err.Error(), "provisioning failed") || !strings.Contains(err.Error(), "no space left for volume") {
		t.Fatalf("error %v does not carry the provisioning failure", err)
	}
}
