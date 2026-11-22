package install

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

var unschedulableNow = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func unschedulablePod(namespace, name, message string, since time.Duration) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
			Conditions: []corev1.PodCondition{{
				Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: corev1.PodReasonUnschedulable, Message: message,
				LastTransitionTime: metav1.NewTime(unschedulableNow.Add(-since)),
			}},
		},
	}
}

const noRoom = "0/13 nodes are available: 1 Insufficient ephemeral-storage, 1 node(s) didn't match PersistentVolume's node affinity, 11 node(s) had untolerated taint(s)."

// A Pod the scheduler refuses for want of a resource does not get better by waiting out the install bound: the
// recorded install failures each spent 10-16 minutes on it. The wait ends once the refusal has stood for the
// bound and says so by name.
func TestWaitPodReadyFailsAtOnceOnAResourceRefusalThatStoodForTheBound(t *testing.T) {
	waits := 0
	installer := Installer{
		Client:       fake.NewSimpleClientset(unschedulablePod("sdo", RepositorySyncPodName, noRoom, 61*time.Second)),
		SyncTimeout:  time.Hour,
		PollInterval: time.Millisecond,
		Now:          func() time.Time { return unschedulableNow },
		Wait:         func(context.Context, time.Duration) error { waits++; return nil },
	}
	err := installer.waitPodReady(context.Background(), "sdo", RepositorySyncPodName)
	var refused *PodUnschedulableError
	if !errors.As(err, &refused) {
		t.Fatalf("a Pod refused for Insufficient resources for 61s did not fail at once with a named error: %v", err)
	}
	if waits != 0 {
		t.Fatalf("the wait polled %d times before failing", waits)
	}
	for _, want := range []string{RepositorySyncPodName, "cannot be scheduled: the scheduler has refused it for lack of a node resource", "Insufficient ephemeral-storage", "61s"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not name %q", err, want)
		}
	}
}

func TestWaitPodReadyKeepsWaitingBeforeTheBoundAndForOtherPendingReasons(t *testing.T) {
	cases := map[string]*corev1.Pod{
		"refusal younger than the bound": unschedulablePod("sdo", RepositorySyncPodName, noRoom, 30*time.Second),
		"unschedulable for a taint, not a resource": unschedulablePod("sdo", RepositorySyncPodName,
			"0/3 nodes are available: 3 node(s) had untolerated taint(s).", 5*time.Minute),
		"image still pulling": {
			ObjectMeta: metav1.ObjectMeta{Namespace: "sdo", Name: RepositorySyncPodName},
			Status: corev1.PodStatus{Phase: corev1.PodPending, ContainerStatuses: []corev1.ContainerStatus{{
				Name: "sync", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"}},
			}}},
		},
	}
	for name, pod := range cases {
		t.Run(name, func(t *testing.T) {
			polls := 0
			installer := Installer{
				Client:       fake.NewSimpleClientset(pod),
				SyncTimeout:  time.Hour,
				PollInterval: time.Millisecond,
				Now:          func() time.Time { return unschedulableNow },
				Wait: func(context.Context, time.Duration) error {
					if polls++; polls >= 3 {
						return context.DeadlineExceeded
					}
					return nil
				},
			}
			err := installer.waitPodReady(context.Background(), "sdo", RepositorySyncPodName)
			var refused *PodUnschedulableError
			if errors.As(err, &refused) {
				t.Fatalf("failed fast on %s: %v", name, err)
			}
			if polls < 3 {
				t.Fatalf("stopped after %d polls: %v", polls, err)
			}
		})
	}
}

func TestInspectExistingNamesAControllerPodTheSchedulerRefusedForTheBound(t *testing.T) {
	cfg := InspectConfig{ControlNamespace: "sdo-system", Namespace: "app", Application: "app", RepositoryPVC: "repository", ControllerImage: "sdo-controller:test"}
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: cfg.ControlNamespace, Name: ControllerJobName, UID: types.UID("job-uid"),
			Annotations: map[string]string{InstallFingerprintAnnotation: "fp", WorkerMountPolicyAnnotation: "proposal-only"},
		},
		Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name: "controller", Image: cfg.ControllerImage, Command: []string{"/usr/local/bin/sdo-operator"},
				Args: []string{"--broker-mode=native", "--worker-mount-mode=proposal-only", "--namespace", cfg.Namespace, "--application", cfg.Application},
			}},
			Volumes: []corev1.Volume{{Name: "repository", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: cfg.RepositoryPVC}}}},
		}}},
		Status: batchv1.JobStatus{Active: 1},
	}
	pod := unschedulablePod(cfg.ControlNamespace, "sdo-controller-abc", noRoom, 90*time.Second)
	pod.OwnerReferences = []metav1.OwnerReference{{Kind: "Job", Name: ControllerJobName, UID: job.UID}}
	installer := Installer{
		Client:             fake.NewSimpleClientset(job, pod),
		Now:                func() time.Time { return unschedulableNow },
		ReadAcceptedCommit: func(context.Context, SyncConfig) (string, error) { return strings.Repeat("a", 40), nil },
	}
	_, err := installer.InspectExisting(context.Background(), cfg)
	var refused *PodUnschedulableError
	if !errors.As(err, &refused) {
		t.Fatalf("a controller Pod refused for Insufficient resources for 90s is still reported as not ready yet: %v", err)
	}
	if !strings.Contains(err.Error(), "existing controller has no ready owned Pod") {
		t.Fatalf("the not-ready wording callers match on is gone: %v", err)
	}
}
