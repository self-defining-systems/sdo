package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"os"
	"path/filepath"
	"sdo.dev/operator/core"
	"strings"
	"testing"
	"time"
)

func TestSourceInventoryReadOnlyReport(t *testing.T) {
	declared, live := hotelInventory()
	index := NewSourceInventory("app", "accepted", declared)
	live = append(live, latentInventoryObjects()...)
	path := filepath.Join(t.TempDir(), "source-drift.json")
	var output bytes.Buffer
	if err := WriteSourceInventoryReport(&output, path, index, live); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "5 unexplained objects") || !strings.Contains(output.String(), path) {
		t.Fatal(output.String())
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var report struct {
		Healthy  bool              `json:"healthy"`
		Findings []json.RawMessage `json:"findings"`
	}
	if err := json.Unmarshal(data, &report); err != nil || report.Healthy || len(report.Findings) != 5 {
		t.Fatalf("report=%s error=%v", data, err)
	}
	if err := WriteSourceInventoryReport(&output, "relative.json", index, live); err == nil {
		t.Fatal("accepted an ambiguous output path")
	}
}

func TestSourceInventoryEvaluateOnceDoesNotWriteToKubernetes(t *testing.T) {
	source, _ := foreignSource(t)
	client := fake.NewSimpleClientset(&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "extra-controller", Namespace: "app"}})
	provider := &core.KubernetesSnapshotProvider{Client: client, Namespace: "app"}
	var output, stderr bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	path := filepath.Join(t.TempDir(), "full.json")
	err := evaluateOnce(ctx, nil, []string{"--namespace", "app", "--app-root", source.Repository, "--deployed-manifest-path", "app.yaml", "--source-drift-report", path}, &output, &stderr, provider)
	if err != nil {
		t.Fatalf("read-only evaluation: %v (%s)", err, stderr.String())
	}
	if !strings.Contains(output.String(), "1 unexplained objects") {
		t.Fatal(output.String())
	}
	for _, action := range client.Actions() {
		if action.GetResource().Resource == "secrets" {
			t.Fatal("read Secret data")
		}
		if action.GetVerb() != "get" && action.GetVerb() != "list" && action.GetVerb() != "watch" {
			t.Fatalf("read-only check mutated Kubernetes: %+v", action)
		}
	}
}
