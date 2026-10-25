package runtime

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"os"
	"path/filepath"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk/sdktest"
	"testing"
)

func TestEvidenceGapsRecognizeCapturedSymptomObjects(t *testing.T) {
	root := t.TempDir()
	view := sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "api"}}}}
	for _, phase := range []string{"open@detect", "fixed"} {
		if err := memory.CaptureIncidentSnapshot(root, "symptom", phase, view, "app"); err != nil {
			t.Fatal(err)
		}
	}
	capture, err := memory.IncidentCorpusDir(root, "symptom")
	if err != nil {
		t.Fatal(err)
	}
	work := []*contracts.ReflectionCauseWork{{Resources: []*contracts.ObjectRef{{Kind: "Service", Namespace: "app", Name: "api"}}}}
	if gaps := appendEvidenceGaps(work, capture); gaps != 0 {
		t.Fatalf("captured Service mislabeled as %d evidence gaps: %v", gaps, work)
	}
	if err := os.Remove(filepath.Join(capture, "open@detect.json")); err != nil {
		t.Fatal(err)
	}
	work[0].Items = nil
	if gaps := appendEvidenceGaps(work, capture); gaps != 1 || work[0].Items[0].EvidencePhase != "open" {
		t.Fatalf("missing opening observation accepted: %v", work)
	}
}
