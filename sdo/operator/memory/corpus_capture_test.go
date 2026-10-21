package memory

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk/sdktest"
)

func TestCaptureIncidentCorpusKeepsFirstRealSnapshot(t *testing.T) {
	root := t.TempDir()
	view := sdktest.Snapshot{NamespaceName: "app", PodList: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "broken", Namespace: "app"}}}}
	if err := CaptureIncidentSnapshot(root, "incident", "open", view, "app"); err != nil {
		t.Fatal(err)
	}
	view.PodList[0].Name = "changed"
	if err := CaptureIncidentSnapshot(root, "incident", "open", view, "app"); err != nil {
		t.Fatal(err)
	}
	if err := CaptureIncidentSnapshot(root, "incident", "fixed", view, "app"); err != nil {
		t.Fatal(err)
	}
	path, err := IncidentCorpusDir(root, "incident")
	if err != nil {
		t.Fatal(err)
	}
	open, err := os.ReadFile(filepath.Join(path, "open.json"))
	if err != nil || !strings.Contains(string(open), "broken") || strings.Contains(string(open), "changed") {
		t.Fatalf("open snapshot changed: %s, %v", open, err)
	}
	fixed, err := os.ReadFile(filepath.Join(path, "fixed.json"))
	if err != nil || !strings.Contains(string(fixed), "changed") {
		t.Fatalf("fixed snapshot missing: %s, %v", fixed, err)
	}
	if err := CaptureIncidentSnapshot(root, "incident", "healthy", view, "app"); err == nil {
		t.Fatal("arbitrary capture phase accepted")
	}
}

func TestCaptureIncidentCorpusKeepsSeparateOpenMoments(t *testing.T) {
	root := t.TempDir()
	view := sdktest.Snapshot{NamespaceName: "app", PodList: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "detect", Namespace: "app"}}}}
	for _, phase := range []string{"open@detect", "open@settle", "open@pre-apply"} {
		if err := CaptureIncidentSnapshot(root, "incident", phase, view, "app"); err != nil {
			t.Fatal(err)
		}
		view.PodList[0].Name = phase
	}
	dir, err := IncidentCorpusDir(root, "incident")
	if err != nil {
		t.Fatal(err)
	}
	for _, phase := range []string{"open@detect", "open@settle", "open@pre-apply"} {
		if _, err := os.Stat(filepath.Join(dir, phase+".json")); err != nil {
			t.Fatal(err)
		}
	}
}
