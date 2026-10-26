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
