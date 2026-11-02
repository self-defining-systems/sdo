package memory

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk/sdktest"
)

func TestIncidentHealthyReferenceSurvivesRepairAndRestart(t *testing.T) {
	root := t.TempDir()
	healthy := sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "consul", Namespace: "app"}, Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 8300}, {Port: 8400}, {Port: 8500}}}}}}
	if err := CaptureHealthyBaseline(root, "accepted", healthy, "app"); err != nil {
		t.Fatal(err)
	}
	// Bind reads the durable record, so no in-memory controller state is required.
	if err := BindIncidentHealthyBaseline(root, "accepted", "incident"); err != nil {
		t.Fatal(err)
	}
	dir, err := IncidentCorpusDir(root, "incident")
	if err != nil {
		t.Fatal(err)
	}
	reference, err := os.ReadFile(filepath.Join(dir, "healthy", "reference"))
	if err != nil || string(reference) != "accepted\n" {
		t.Fatalf("healthy reference identity missing: %q %v", reference, err)
	}
	original, err := os.ReadFile(filepath.Join(dir, "healthy", "healthy.json"))
	if err != nil {
		t.Fatal(err)
	}
	repaired := healthy
	repaired.ServiceList = append([]corev1.Service(nil), healthy.ServiceList...)
	repaired.ServiceList[0].Spec.Ports = []corev1.ServicePort{{Port: 8500}}
	if err := CaptureHealthyBaseline(root, "accepted", repaired, "app"); err != nil {
		t.Fatal(err)
	}
	if err := BindIncidentHealthyBaseline(root, "accepted", "incident"); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "healthy", "healthy.json"))
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != string(original) {
		t.Fatal("post-repair state replaced pre-incident healthy reference")
	}
	if err := BindIncidentHealthyBaseline(root, "other-source", "new-incident"); err == nil {
		t.Fatal("baseline from another accepted source revision reused")
	}
	if err := BindIncidentHealthyBaseline(root, "accepted", "next-incident"); err != nil {
		t.Fatal(err)
	}
	next, err := IncidentCorpusDir(root, "next-incident")
	if err != nil {
		t.Fatal(err)
	}
	raw, err = os.ReadFile(filepath.Join(next, "healthy", "healthy.json"))
	if err != nil {
		t.Fatal(err)
	}
	var snapshot sdktest.Snapshot
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		t.Fatal(err)
	}
	if len(snapshot.ServiceList[0].Spec.Ports) != 1 {
		t.Fatal("new incident did not adopt its own preceding healthy state")
	}
}

func TestClosureHealthyBaselineFillsOnlyAMissingPreIncidentReference(t *testing.T) {
	root := t.TempDir()
	view := sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "consul", Namespace: "app"}}}}
	if err := CaptureIncidentSnapshot(root, "incident", "fixed", view, "app"); err != nil {
		t.Fatal(err)
	}
	if err := BindClosureHealthyBaseline(root, "incident"); err != nil {
		t.Fatal(err)
	}
	dir, _ := IncidentCorpusDir(root, "incident")
	fixed, _ := os.ReadFile(filepath.Join(dir, "fixed.json"))
	healthy, err := os.ReadFile(filepath.Join(dir, "healthy", "healthy.json"))
	if err != nil || string(healthy) != string(fixed) {
		t.Fatalf("closure snapshot not bound as healthy sample: %v", err)
	}
	if reference, _ := os.ReadFile(filepath.Join(dir, "healthy", "reference")); string(reference) != "closure\n" {
		t.Fatalf("reference=%q", reference)
	}
	// A pre-incident reference wins over the closure snapshot.
	other := t.TempDir()
	if err := CaptureHealthyBaseline(other, "accepted", sdktest.Snapshot{NamespaceName: "app"}, "app"); err != nil {
		t.Fatal(err)
	}
	if err := BindIncidentHealthyBaseline(other, "accepted", "incident"); err != nil {
		t.Fatal(err)
	}
	if err := CaptureIncidentSnapshot(other, "incident", "fixed", view, "app"); err != nil {
		t.Fatal(err)
	}
	dir, _ = IncidentCorpusDir(other, "incident")
	before, _ := os.ReadFile(filepath.Join(dir, "healthy", "healthy.json"))
	if err := BindClosureHealthyBaseline(other, "incident"); err != nil {
		t.Fatal(err)
	}
	after, _ := os.ReadFile(filepath.Join(dir, "healthy", "healthy.json"))
	if string(before) != string(after) {
		t.Fatal("closure snapshot replaced the pre-incident reference")
	}
	// No fixed capture (unverified closure) leaves the reference absent.
	if err := BindClosureHealthyBaseline(t.TempDir(), "never-closed"); err == nil {
		t.Fatal("baseline bound without a closure capture")
	}
}
