package deployment

import (
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestStatefulSetRolloutComplete(t *testing.T) {
	three, one := int32(3), int32(1)
	base := func() *appsv1.StatefulSet {
		return &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Generation: 2},
			Spec:       appsv1.StatefulSetSpec{Replicas: &three},
			Status:     appsv1.StatefulSetStatus{ObservedGeneration: 2, ReadyReplicas: 3, UpdatedReplicas: 3, CurrentRevision: "r2", UpdateRevision: "r2"},
		}
	}
	for name, test := range map[string]struct {
		edit func(*appsv1.StatefulSet)
		want bool
	}{
		"complete":                {func(*appsv1.StatefulSet) {}, true},
		"generation not observed": {func(s *appsv1.StatefulSet) { s.Status.ObservedGeneration = 1 }, false},
		"replica not ready":       {func(s *appsv1.StatefulSet) { s.Status.ReadyReplicas = 2 }, false},
		"revision still rolling":  {func(s *appsv1.StatefulSet) { s.Status.CurrentRevision = "r1"; s.Status.UpdatedReplicas = 1 }, false},
		"partition reached": {func(s *appsv1.StatefulSet) {
			s.Spec.UpdateStrategy.RollingUpdate = &appsv1.RollingUpdateStatefulSetStrategy{Partition: &one}
			s.Status.CurrentRevision = "r1"
			s.Status.UpdatedReplicas = 2
		}, true},
		"partition not reached": {func(s *appsv1.StatefulSet) {
			s.Spec.UpdateStrategy.RollingUpdate = &appsv1.RollingUpdateStatefulSetStrategy{Partition: &one}
			s.Status.UpdatedReplicas = 1
		}, false},
		"on delete waits on nothing": {func(s *appsv1.StatefulSet) {
			s.Spec.UpdateStrategy.Type = appsv1.OnDeleteStatefulSetStrategyType
			s.Status.UpdatedReplicas = 0
		}, true},
	} {
		s := base()
		test.edit(s)
		if got := StatefulSetRolloutComplete(s); got != test.want {
			t.Errorf("%s: StatefulSetRolloutComplete = %v, want %v", name, got, test.want)
		}
	}
}

func TestDaemonSetRolloutComplete(t *testing.T) {
	base := func() *appsv1.DaemonSet {
		return &appsv1.DaemonSet{
			ObjectMeta: metav1.ObjectMeta{Generation: 2},
			Status:     appsv1.DaemonSetStatus{ObservedGeneration: 2, DesiredNumberScheduled: 2, UpdatedNumberScheduled: 2, NumberAvailable: 2},
		}
	}
	for name, test := range map[string]struct {
		edit func(*appsv1.DaemonSet)
		want bool
	}{
		"complete":                {func(*appsv1.DaemonSet) {}, true},
		"generation not observed": {func(d *appsv1.DaemonSet) { d.Status.ObservedGeneration = 1 }, false},
		"node not updated":        {func(d *appsv1.DaemonSet) { d.Status.UpdatedNumberScheduled = 1 }, false},
		"node not available":      {func(d *appsv1.DaemonSet) { d.Status.NumberAvailable = 1 }, false},
		"on delete waits on nothing": {func(d *appsv1.DaemonSet) {
			d.Spec.UpdateStrategy.Type = appsv1.OnDeleteDaemonSetStrategyType
			d.Status.UpdatedNumberScheduled = 0
		}, true},
	} {
		d := base()
		test.edit(d)
		if got := DaemonSetRolloutComplete(d); got != test.want {
			t.Errorf("%s: DaemonSetRolloutComplete = %v, want %v", name, got, test.want)
		}
	}
}
