package runtime

import (
	"context"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const hotel = "hotel-reservation"

func hotelObjects() []k8sruntime.Object {
	replicas := int32(1)
	return []k8sruntime.Object{
		&corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: "frontend", Namespace: hotel},
			Spec: corev1.ServiceSpec{
				Selector: map[string]string{"io.kompose.service": "frontend"},
				Ports:    []corev1.ServicePort{{Name: "5000", Port: 5000, TargetPort: intstr.FromInt(5000), Protocol: corev1.ProtocolTCP}},
			},
		},
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "frontend", Namespace: hotel},
			Spec: appsv1.DeploymentSpec{
				Replicas: &replicas,
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"io.kompose.service": "frontend"}},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"io.kompose.service": "frontend"}},
					Spec: corev1.PodSpec{Containers: []corev1.Container{{
						Name: "hotel-reserv-frontend", Image: "deathstarbench/hotel-reservation:latest",
						Command: []string{"frontend"},
						Env: []corev1.EnvVar{
							{Name: "JAEGER_SAMPLE_RATIO", Value: "1"},
							{Name: "DB_PASSWORD", Value: "hunter2"},
						},
					}}},
				},
			},
		},
		// SREGym's decoys exist, mounted, while the application is healthy.
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "failure-admin-geo", Namespace: hotel},
			Data:       map[string]string{"revoke-admin-geo-mongo.sh": "db.revokeRolesFromUser(...)"},
		},
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "failure-admin-rate", Namespace: hotel},
			Data:       map[string]string{"revoke-admin-rate-mongo.sh": "db.revokeRolesFromUser(...)"},
		},
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "mongo-geo-script", Namespace: hotel},
			Data:       map[string]string{"k8s-geo-mongo.sh": "echo init", "extra.sh": "echo extra"},
		},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "mongo-credentials", Namespace: hotel},
			Data:       map[string][]byte{"password": []byte("hunter2")},
		},
		&rbacv1.Role{
			ObjectMeta: metav1.ObjectMeta{Name: "reader", Namespace: hotel},
			Rules:      []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"pods"}, Verbs: []string{"get", "list"}}},
		},
		// SDO's own state is never reported as an application change.
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "sdo-controller-state", Namespace: hotel},
			Data:       map[string]string{"state": "v1"},
		},
	}
}

// eventually polls until the informers have observed a change.
func eventually(t *testing.T, what string, condition func() bool) {
	t.Helper()
	clock := timerClock(nil)
	deadline := clock.Now().Add(5 * time.Second)
	for clock.Now().Before(deadline) {
		if condition() {
			return
		}
		clock.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func findChange(changes *StateChanges, kind string, name string) *StateChange {
	if changes == nil {
		return nil
	}
	for index := range changes.Changes {
		if changes.Changes[index].Kind == kind && changes.Changes[index].Name == name {
			return &changes.Changes[index]
		}
	}
	return nil
}

func field(change *StateChange, name string) *StateFieldChange {
	for index := range change.Fields {
		if change.Fields[index].Field == name {
			return &change.Fields[index]
		}
	}
	return nil
}

func TestStateTrackerRejectsNegativeResyncPeriod(t *testing.T) {
	tracker := newTrackerForTest(t, StateTrackerConfig{ResyncPeriod: -time.Second})
	if err := tracker.Start(context.Background()); err == nil {
		t.Fatal("negative resync period accepted")
	}
}
