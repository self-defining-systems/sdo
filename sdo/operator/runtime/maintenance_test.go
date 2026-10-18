package runtime

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func maintenanceConfigMap(namespace string, state string, generation string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: MaintenanceConfigMapName, Namespace: namespace},
		Data:       map[string]string{"state": state, "generation": generation},
	}
}

func TestMaintenanceWatcherTreatsMissingConfigMapAsActive(t *testing.T) {
	watcher := MaintenanceWatcher{Client: fake.NewSimpleClientset(), Namespace: "demo-sdo", Name: MaintenanceConfigMapName}
	state, err := watcher.Read(context.Background())
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if state.Paused || state.Generation != "" {
		t.Fatalf("missing maintenance ConfigMap must mean active operation, got %#v", state)
	}
}

func TestMaintenanceWatcherReadsPausedAndRejectsUnknownStates(t *testing.T) {
	client := fake.NewSimpleClientset(maintenanceConfigMap("demo-sdo", "paused", "g1"))
	watcher := MaintenanceWatcher{Client: client, Namespace: "demo-sdo", Name: MaintenanceConfigMapName}
	state, err := watcher.Read(context.Background())
	if err != nil || !state.Paused || state.Generation != "g1" {
		t.Fatalf("unexpected paused state %#v err=%v", state, err)
	}
	if _, err := client.CoreV1().ConfigMaps("demo-sdo").Update(
		context.Background(), maintenanceConfigMap("demo-sdo", "sleeping", "g2"), metav1.UpdateOptions{},
	); err != nil {
		t.Fatalf("update: %v", err)
	}
	if _, err := watcher.Read(context.Background()); err == nil {
		t.Fatal("unknown maintenance state was accepted")
	}
}

func TestMaintenanceWatcherEmitsResumeWithNewGeneration(t *testing.T) {
	client := fake.NewSimpleClientset(maintenanceConfigMap("demo-sdo", "paused", "g1"))
	watcher := MaintenanceWatcher{Client: client, Namespace: "demo-sdo", Name: MaintenanceConfigMapName}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changes := watcher.Watch(ctx, MaintenanceState{Paused: true, Generation: "g1"}, 5*time.Millisecond, nil)

	if _, err := client.CoreV1().ConfigMaps("demo-sdo").Update(
		ctx, maintenanceConfigMap("demo-sdo", "active", "g2"), metav1.UpdateOptions{},
	); err != nil {
		t.Fatalf("update: %v", err)
	}
	select {
	case state := <-changes:
		if state.Paused || state.Generation != "g2" {
			t.Fatalf("unexpected change %#v", state)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("resume was not observed")
	}
	record := MaintenanceState{Generation: "g2"}.Record()
	if record["controller_maintenance"] != "active" || record["maintenance_generation"] != "g2" {
		t.Fatalf("unexpected maintenance log record %#v", record)
	}
}
