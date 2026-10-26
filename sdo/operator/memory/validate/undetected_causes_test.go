package validate

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk/sdktest"
)

func TestUndetectedRepairRequiresMeasuredObjectTransition(t *testing.T) {
	at := time.Unix(100, 0)
	ref := &contracts.ObjectRef{ApiVersion: "v1", Kind: "Service", Namespace: "demo", Name: "front"}
	outcome := &contracts.OutcomeRecord{Classification: "failed", RepairActions: []*contracts.RepairActionReceipt{{ActionId: "recorded-patch", Success: true, Resources: []*contracts.ObjectRef{ref}, StartedAt: timestamppb.New(at.Add(time.Second)), CompletedAt: timestamppb.New(at.Add(2 * time.Second))}}}
	capture := t.TempDir()
	write := func(phase string, port int32, observed time.Time) {
		t.Helper()
		snapshot := sdktest.Snapshot{NamespaceName: "demo", ObservedAtTime: &observed, ServiceList: []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "front", Namespace: "demo", UID: "stable"}, Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 80, TargetPort: intstr.FromInt32(port)}}}}}}
		raw, _ := json.Marshal(snapshot)
		if err := os.WriteFile(filepath.Join(capture, phase+".json"), raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	write("open@pre-apply", 8081, at)
	write("fixed", 8080, at.Add(3*time.Second))
	outcome.UndetectedCauses = CaptureUndetectedCauses(outcome, capture)
	causes := VerifiedReflectionCauses(outcome)
	if len(causes) != 1 || causes[0].GetLearningBasis() != "observed_transition" || len(outcome.Findings) != 0 {
		t.Fatalf("undetected repair unavailable or fabricated findings: %v", causes)
	}
	root := t.TempDir()
	copyTree(t, "../../../../tests/fixtures/sdo/playbook_program_app/.sdo", filepath.Join(root, ".sdo"))
	for phase, filename := range map[string]string{"open@pre-apply": "open.json", "fixed": "fixed.json"} {
		raw, err := os.ReadFile(filepath.Join(capture, phase+".json"))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, ".sdo/corpus/targetport-mismatch", filename), raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	decision := &contracts.ReflectionDecision{LearningDecision: "updated", PlaybookIds: []string{"service-targetport-mismatch"}, SdkGaps: []*contracts.SdkGap{{FaultClass: "database", Primitive: "catalog", Reason: "unavailable", ScratchScript: "inspect", Resources: []*contracts.ObjectRef{{Kind: "Deployment", Namespace: "demo", Name: "database"}}}}}
	ds, err := ReflectionLearningDiagnostics(root, outcome, decision)
	if err != nil || len(ds) != 0 {
		t.Fatalf("learnable undetected cause blocked by peer gap: %v %v", ds, err)
	}
	for _, mode := range []string{"unchanged", "stale", "failed-receipt", "detected"} {
		t.Run(mode, func(t *testing.T) {
			write("fixed", 8080, at.Add(3*time.Second))
			outcome.RepairActions[0].Success = true
			outcome.Findings = nil
			switch mode {
			case "unchanged":
				write("fixed", 8081, at.Add(3*time.Second))
			case "stale":
				write("fixed", 8080, at)
			case "failed-receipt":
				outcome.RepairActions[0].Success = false
			case "detected":
				outcome.Findings = []*contracts.Finding{{PrimaryResource: ref, Status: "active"}}
			}
			if got := CaptureUndetectedCauses(outcome, capture); len(got) != 0 {
				t.Fatalf("unsupported learning evidence accepted: %v", got)
			}
		})
	}
}
