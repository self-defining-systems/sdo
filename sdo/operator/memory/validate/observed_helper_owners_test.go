package validate

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk/sdktest"
)

func TestObservedRepairOwnersMarksTopLevelObjectsAsHelpersAndChainsTheRest(t *testing.T) {
	controller := true
	observed := time.Date(2026, 11, 12, 3, 0, 0, 0, time.UTC)
	snapshot := sdktest.Snapshot{NamespaceName: "app", ObservedAtTime: &observed, PodList: []corev1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Name: "stray", Namespace: "app"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "api-7f9c", Namespace: "app", OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "api-7f9c", Controller: &controller}}}},
	}}
	capture := t.TempDir()
	raw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(capture, "open@detect.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	t0 := time.Date(2026, 11, 12, 3, 1, 0, 0, time.UTC)
	outcome := &contracts.OutcomeRecord{RepairActions: []*contracts.RepairActionReceipt{
		writeAt("api-delete", "Pod", "stray", t0, t0.Add(time.Second)),
		writeAt("api-delete", "Pod", "api-7f9c", t0, t0.Add(time.Second)),
		writeAt("api-delete", "Pod", "never-observed", t0, t0.Add(time.Second)),
	}}
	ObservedRepairOwners(outcome, capture)
	stray, owned, unseen := outcome.RepairActions[0], outcome.RepairActions[1], outcome.RepairActions[2]
	if stray.GetKind() != "api-helper-delete" || len(stray.GetOwners()) != 0 {
		t.Fatalf("an object with no owner in the open capture is a helper: %v", stray)
	}
	if owned.GetKind() != "api-delete" || len(owned.GetOwners()) != 1 {
		t.Fatalf("an owned Pod keeps its kind and gets its chain: %v", owned)
	}
	if unseen.GetKind() != "api-delete" {
		t.Fatalf("an object no open phase observed stays unattributed: %v", unseen)
	}
}
