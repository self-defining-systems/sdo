package memory

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/internal/capturefixture"
	"sdo.dev/operator/sdk/sdktest"
)

func TestValidateCapturedCorpusTreeRejectsForgedSnapshot(t *testing.T) {
	root := t.TempDir()
	view := sdktest.Snapshot{NamespaceName: "app"}
	if err := CaptureIncidentSnapshot(root, "incident", "open", view, "app"); err != nil {
		t.Fatal(err)
	}
	if err := CaptureIncidentSnapshot(root, "incident", "fixed", view, "app"); err != nil {
		t.Fatal(err)
	}
	caseDir := filepath.Join(t.TempDir(), ".sdo", "corpus", "case")
	if err := os.MkdirAll(caseDir, 0o755); err != nil {
		t.Fatal(err)
	}
	trusted, err := IncidentCorpusDir(root, "incident")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"open.json", "fixed.json", "recordings.json"} {
		data, err := os.ReadFile(filepath.Join(trusted, name))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(caseDir, name), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	open, _ := os.ReadFile(filepath.Join(trusted, "open.json"))
	fixed, _ := os.ReadFile(filepath.Join(trusted, "fixed.json"))
	labels := fmt.Sprintf(`{"provenance":{"incident_id":"incident","open_sha256":"%x","fixed_sha256":"%x"}}`, sha256.Sum256(open), sha256.Sum256(fixed))
	if err := os.WriteFile(filepath.Join(caseDir, "labels.json"), []byte(labels), 0o644); err != nil {
		t.Fatal(err)
	}
	candidate := filepath.Dir(filepath.Dir(filepath.Dir(caseDir)))
	changed := []string{".sdo/corpus/case/open.json"}
	if err := ValidateCapturedCorpusTree(root, candidate, "incident", changed); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(caseDir, "open.json"), capturefixture.Raw(t, "forged capture", []byte(`{"namespace":"forged"}`)), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := ValidateCapturedCorpusTree(root, candidate, "incident", changed); err == nil || !strings.Contains(err.Error(), "captured") {
		t.Fatalf("forged corpus accepted: %v", err)
	}
}

func TestValidateCapturedCorpusTreeUsesLabeledOpenPhase(t *testing.T) {
	root := t.TempDir()
	view := sdktest.Snapshot{NamespaceName: "app", PodList: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "detect", Namespace: "app"}}}}
	for _, phase := range []string{"open@detect", "open@settle", "fixed"} {
		if err := CaptureIncidentSnapshot(root, "incident", phase, view, "app"); err != nil {
			t.Fatal(err)
		}
		view.PodList[0].Name = phase
	}
	trusted, err := IncidentCorpusDir(root, "incident")
	if err != nil {
		t.Fatal(err)
	}
	caseDir := filepath.Join(t.TempDir(), ".sdo", "corpus", "case")
	if err := os.MkdirAll(caseDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, source := range map[string]string{"open.json": "open@detect.json", "fixed.json": "fixed.json", "recordings.json": "open@detect-recordings.json"} {
		data, err := os.ReadFile(filepath.Join(trusted, source))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(caseDir, name), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	open, _ := os.ReadFile(filepath.Join(caseDir, "open.json"))
	fixed, _ := os.ReadFile(filepath.Join(caseDir, "fixed.json"))
	labels := fmt.Sprintf(`{"provenance":{"incident_id":"incident","open_at":"detect","open_sha256":"%x","fixed_sha256":"%x"}}`, sha256.Sum256(open), sha256.Sum256(fixed))
	if err := os.WriteFile(filepath.Join(caseDir, "labels.json"), []byte(labels), 0o644); err != nil {
		t.Fatal(err)
	}
	candidate := filepath.Dir(filepath.Dir(filepath.Dir(caseDir)))
	if err := ValidateCapturedCorpusTree(root, candidate, "incident", []string{".sdo/corpus/case/open.json"}); err != nil {
		t.Fatal(err)
	}
	labels = strings.Replace(labels, `"open_at":"detect"`, `"open_at":"settle"`, 1)
	if err := os.WriteFile(filepath.Join(caseDir, "labels.json"), []byte(labels), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := ValidateCapturedCorpusTree(root, candidate, "incident", []string{".sdo/corpus/case/open.json"}); err == nil {
		t.Fatal("wrong selected phase accepted")
	}
}

func TestValidateCapturedCorpusDirectoryRejectsEmptyIncidentIdentity(t *testing.T) {
	for _, test := range []struct{ name, labelID, contextID string }{
		{"empty caller incident", "", "incident"},
		{"empty provisioned incident", "", ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidate := t.TempDir()
			dir := filepath.Join(candidate, ".sdo/corpus/case")
			if err := os.MkdirAll(dir, 0755); err != nil {
				t.Fatal(err)
			}
			labels := fmt.Sprintf(`{"provenance":{"incident_id":%q}}`, test.labelID)
			if err := os.WriteFile(filepath.Join(dir, "labels.json"), []byte(labels), 0644); err != nil {
				t.Fatal(err)
			}
			err := ValidateCapturedCorpusDirectory(t.TempDir(), candidate, test.contextID, []string{".sdo/corpus/case/labels.json"})
			if err == nil {
				t.Fatal("empty incident identity bypassed trusted capture comparison")
			}
		})
	}
}
