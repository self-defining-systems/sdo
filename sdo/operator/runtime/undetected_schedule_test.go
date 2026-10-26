package runtime

import (
	"math/rand"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/internal/capturefixture"
	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/sdk/sdktest"
)

// Capture persistence, receipt persistence and restart may arrive in any order.
// Authority comes from the recorded observation times, never file arrival order.
func TestSimScheduleUndetectedLearningRequiresCompleteTransition(t *testing.T) {
	sweepSimSeedsN(t, 4, func(t *testing.T, seed uint64) {
		at := time.Unix(100, 0)
		ref := &contracts.ObjectRef{ApiVersion: "v1", Kind: "Service", Namespace: "demo", Name: "front"}
		outcome := &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "undetected-schedule", Classification: "failed"}
		capture := t.TempDir()
		events := []string{"open", "repair", "fixed", "restart"}
		rand.New(rand.NewSource(int64(seed))).Shuffle(len(events), func(i, j int) { events[i], events[j] = events[j], events[i] })
		seen := map[string]bool{}
		for _, event := range events {
			switch event {
			case "repair":
				outcome.RepairActions = []*contracts.RepairActionReceipt{{ActionId: "recorded-patch", Success: true, Resources: []*contracts.ObjectRef{ref}, StartedAt: timestamppb.New(at.Add(time.Second)), CompletedAt: timestamppb.New(at.Add(2 * time.Second))}}
			case "open", "fixed":
				observed, port, phase := at, int32(9999), "open@pre-apply"
				if event == "fixed" {
					observed, port, phase = at.Add(3*time.Second), 8080, "fixed"
				}
				snapshot := sdktest.Snapshot{NamespaceName: "demo", ObservedAtTime: &observed, ServiceList: []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "front", Namespace: "demo", UID: "same"}, Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 80, TargetPort: intstr.FromInt32(port)}}}}}}
				capturefixture.WriteBoundary(t, filepath.Join(capture, phase+".json"), snapshot, "demo")
			case "restart":
				raw, err := protojson.Marshal(outcome)
				if err != nil {
					t.Fatal(err)
				}
				restored := new(contracts.OutcomeRecord)
				if err := protojson.Unmarshal(raw, restored); err != nil {
					t.Fatal(err)
				}
				outcome = restored
			}
			seen[event] = true
			outcome.UndetectedCauses = validate.CaptureUndetectedCauses(outcome, capture)
			ready := seen["open"] && seen["repair"] && seen["fixed"]
			if (len(validate.VerifiedReflectionCauses(outcome)) == 1) != ready {
				t.Fatalf("seed=%d events=%v after=%s: transition authority precedes its evidence or was lost", seed, events, event)
			}
		}
		projected := partialReflectionOutcome(outcome)
		if len(projected.RepairActions) != 1 || len(validate.VerifiedReflectionCauses(projected)) != 1 || len(projected.Findings) != 0 {
			t.Fatalf("seed=%d: partial reflection lost observed-transition evidence", seed)
		}
	})
}
