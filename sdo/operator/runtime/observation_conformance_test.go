package runtime

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk/program"
)

type conformanceCatalog struct {
	targets []program.DatabaseTarget
	err     error
}

func (c *conformanceCatalog) DatabaseUsers(_ context.Context, target program.DatabaseTarget) ([]program.DatabaseUser, error) {
	c.targets = append(c.targets, target)
	return nil, c.err
}
func TestObservationConformanceCoversEachMatchingDatabaseWorkload(t *testing.T) {
	pods := []corev1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Name: "mongodb-geo", Namespace: "hotel"}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "hotel-reserv-geo-mongo", Image: "mongo:4.4.6"}}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "frontend", Namespace: "hotel"}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "example/frontend:v1"}}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "mongodb-rate", Namespace: "hotel"}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "mongo", Image: "mongo:4.4.6"}}}},
	}
	client := &conformanceCatalog{err: &DatabaseOperationError{Step: DatabaseAuthentication, Reason: databaseRejected}}
	report := CheckObservationConformance(context.Background(), client, pods, nil)
	if report.Passed || len(client.targets) != 2 || len(report.Observations) != 2 {
		t.Fatalf("probe skipped workload: %+v", report)
	}
	for _, row := range report.Observations {
		if row.Primitive != "DatabaseUsers" || row.FailureStage != "authentication" {
			t.Fatalf("failure identity lost: %+v", row)
		}
	}
	client.err = errors.New("SYNTHETIC_PRIVATE_UPSTREAM_BODY")
	report = CheckObservationConformance(context.Background(), client, pods, nil)
	if report.Observations[0].Error == client.err.Error() {
		t.Fatal("probe returned private upstream message")
	}
}

func TestObservationConformanceFindsHotelDatabaseBehindRegistryRewrite(t *testing.T) {
	pods := []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "mongodb-geo-abc", Namespace: "hotel"}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "hotel-reserv-geo-mongo", Image: "registry.example:5000/apps/source-8abcdef@sha256:abc", Ports: []corev1.ContainerPort{{ContainerPort: 27017}}}}}}}
	client := &conformanceCatalog{}
	report := CheckObservationConformance(context.Background(), client, pods, nil)
	if !report.Passed || len(report.Observations) != 1 {
		t.Fatalf("hotel database was silently undiscovered: %+v", report)
	}
}

func TestObservationConformanceRejectsEmptyInventoryAndSelectorMismatch(t *testing.T) {
	if report := CheckObservationConformance(context.Background(), &conformanceCatalog{}, nil, nil); report.Passed || len(report.Observations) != 1 || report.Observations[0].FailureStage != "workload-discovery" {
		t.Fatalf("empty inventory passed: %+v", report)
	}
	pods := []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "mongodb-geo", Namespace: "hotel", Labels: map[string]string{"app": "mongodb-geo"}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "mongo", Image: "mongo:4.4.6"}}}}}
	if _, err := SelectObservationPods(pods, "io.kompose.service=mongodb-geo"); err == nil {
		t.Fatal("selector mismatch passed despite application database")
	}
	frontend := []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "frontend", Namespace: "toy"}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "example/frontend:v1"}}}}}
	if report := CheckObservationConformance(context.Background(), &conformanceCatalog{}, frontend, nil); !report.Passed || len(report.Observations) != 0 {
		t.Fatalf("known non-database application rejected: %+v", report)
	}
}
