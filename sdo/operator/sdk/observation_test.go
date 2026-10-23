package sdk_test

import (
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

func TestObservationTimePrefersTheRecordedClock(t *testing.T) {
	observed := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	snapshot := sdktest.Snapshot{NamespaceName: "app", ObservedAtTime: &observed, PodList: []corev1.Pod{{
		ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app", CreationTimestamp: metav1.NewTime(observed.Add(time.Hour))},
	}}}
	if got := sdk.ObservationTime(snapshot); !got.Equal(observed) {
		t.Fatalf("recorded observation time must win, got %v", got)
	}
}

func TestObservationTimeFallsBackToTheLatestRecordedObjectTime(t *testing.T) {
	base := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	snapshot := sdktest.Snapshot{
		NamespaceName: "app",
		PodList: []corev1.Pod{{
			ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app", CreationTimestamp: metav1.NewTime(base)},
			Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
				Name: "api",
				LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
					FinishedAt: metav1.NewTime(base.Add(2 * time.Minute)),
				}},
			}}},
		}},
		DeploymentList: []appsv1.Deployment{{ObjectMeta: metav1.ObjectMeta{
			Name: "api", Namespace: "app", CreationTimestamp: metav1.NewTime(base.Add(-time.Hour)),
		}}},
		EventList: []corev1.Event{{
			ObjectMeta:    metav1.ObjectMeta{Name: "api.1", Namespace: "app"},
			LastTimestamp: metav1.NewTime(base.Add(3 * time.Minute)),
		}},
	}
	if got := sdk.ObservationTime(snapshot); !got.Equal(base.Add(3 * time.Minute)) {
		t.Fatalf("fallback must be the latest object timestamp, got %v", got)
	}
	if got := sdk.ObservationTime(sdktest.Snapshot{NamespaceName: "app"}); !got.IsZero() {
		t.Fatalf("an empty snapshot has no observation time, got %v", got)
	}
}
