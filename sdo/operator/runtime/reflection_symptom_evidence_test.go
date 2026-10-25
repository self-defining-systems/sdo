package runtime

import (
	"os"
	"path/filepath"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk/sdktest"
)

func TestEvidenceGapsRecognizeCapturedSymptomObjects(t *testing.T) {
	for _, opening := range []string{"open@detect", "open@settle", "open@pre-apply"} {
		t.Run(opening, func(t *testing.T) {
			root := t.TempDir()
			view := sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "api"}}}}
			for _, phase := range []string{opening, "fixed"} {
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
			if err := os.Remove(filepath.Join(capture, opening+".json")); err != nil {
				t.Fatal(err)
			}
			work[0].Items = nil
			if gaps := appendEvidenceGaps(work, capture); gaps != 1 || work[0].Items[0].EvidencePhase != "open" {
				t.Fatalf("missing opening observation accepted: %v", work)
			}
		})
	}
}
