package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"
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
	client.err = &DatabaseOperationError{Step: DatabaseOperationStep("SYNTHETIC_PRIVATE_MESSAGE"), Reason: DatabaseFailureReason("SYNTHETIC_PRIVATE_MESSAGE")}
	report = CheckObservationConformance(context.Background(), client, pods, nil)
	raw, _ := json.Marshal(report)
	if strings.Contains(string(raw), "SYNTHETIC_PRIVATE_MESSAGE") {
		t.Fatal("probe returned unrecognized diagnostic values")
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

func TestDatabaseObservationAuthDiscoveryUsesMountedSourceReferences(t *testing.T) {
	pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "mongodb-geo", Namespace: "hotel"}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "hotel-reserv-geo-mongo", Image: "mongo:4.4.6", VolumeMounts: []corev1.VolumeMount{{Name: "bootstrap", MountPath: "/init"}}}}, Volumes: []corev1.Volume{{Name: "bootstrap", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: "mongo-geo-script"}}}}}}}
	const canary = "SYNTHETIC_CREDENTIAL_NEVER_EMIT"
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "mongo-geo-script", Namespace: "hotel"}, Data: map[string]string{"bootstrap.sh": "ROOT_USER='root'\nROOT_PWD='" + canary + "'\nADMIN_USER='app'\nADMIN_PWD='" + canary + "'\n"}}
	client := TypedPlanClient{Client: kubefake.NewSimpleClientset(cm)}
	target := DatabaseObservationTargets([]corev1.Pod{pod})[0]
	discovered, err := DiscoverDatabaseObservationAuth(context.Background(), client, pod, target)
	if err != nil || discovered.Auth == nil || discovered.Auth.Password.ConfigMap.Assignment != "ROOT_PWD" || discovered.Auth.Username.ConfigMap.Name != "mongo-geo-script" {
		t.Fatalf("mounted authentication source unavailable: %v %v", discovered, err)
	}
	raw, _ := json.Marshal(discovered)
	if strings.Contains(string(raw), canary) {
		t.Fatal("credential value escaped reference discovery")
	}
	for _, action := range client.Client.(*kubefake.Clientset).Actions() {
		if action.GetResource().Resource != "configmaps" || action.GetVerb() != "get" {
			t.Fatalf("unexpected operation: %v", action)
		}
	}
}
