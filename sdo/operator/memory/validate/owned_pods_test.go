package validate

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk/sdktest"
)

func controlledBy(kind, name string) []metav1.OwnerReference {
	controller := true
	return []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: kind, Name: name, Controller: &controller}}
}

// The repair rolls the Deployment: the opening Pod is gone and a new Pod of a
// new ReplicaSet runs. Only the Deployment carries an opening finding.
func writeRolloutCapture(t *testing.T, at time.Time, openPods, fixedPods []corev1.Pod) string {
	t.Helper()
	capture := t.TempDir()
	sets := []appsv1.ReplicaSet{
		{ObjectMeta: metav1.ObjectMeta{Name: "search-old", Namespace: "app", OwnerReferences: controlledBy("Deployment", "search")}},
		{ObjectMeta: metav1.ObjectMeta{Name: "other-old", Namespace: "app", OwnerReferences: controlledBy("Deployment", "other")}},
	}
	for phase, pods := range map[string][]corev1.Pod{"open@detect": openPods, "fixed": fixedPods} {
		observed := at
		if phase == "fixed" {
			observed = at.Add(3 * time.Second)
		}
		view := sdktest.Snapshot{NamespaceName: "app", ObservedAtTime: &observed, PodList: pods, ReplicaSetList: sets, DeploymentList: []appsv1.Deployment{{ObjectMeta: metav1.ObjectMeta{Name: "search", Namespace: "app"}}}}
		// Boundaries are written the way the controller writes them.
		snapshot, err := sdktest.CaptureSnapshot(view, "app")
		if err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(capture, phase+".json"), raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return capture
}

func pod(name, owner string) corev1.Pod {
	return corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app", OwnerReferences: controlledBy("ReplicaSet", owner)}}
}

func recreatedPod(name, owner, uid string) corev1.Pod {
	p := pod(name, owner)
	p.UID = types.UID(uid)
	return p
}

// A StatefulSet-style recreation keeps the Pod name; only the UID shows that
// the opening Pod is gone.
func TestSameNameRecreatedOwnedPodDoesNotBlockVerifiedWorkloadRepair(t *testing.T) {
	at := time.Unix(100, 0)
	deployment := &contracts.ObjectRef{ApiVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "search"}
	recreated := &contracts.ObjectRef{ApiVersion: "v1", Kind: "Pod", Namespace: "app", Name: "search-0"}
	outcome := &contracts.OutcomeRecord{
		Classification:      "success",
		ConfirmedRootCauses: []*contracts.ConfirmedRootCause{{Resources: []*contracts.ObjectRef{deployment, recreated}}},
		Findings:            []*contracts.Finding{{DetectorId: "workload", Status: "active", Fingerprint: "search", PrimaryResource: deployment}},
		RepairActions:       []*contracts.RepairActionReceipt{{Success: true, Resources: []*contracts.ObjectRef{deployment}, StartedAt: timestamppb.New(at.Add(time.Second)), CompletedAt: timestamppb.New(at.Add(2 * time.Second))}},
		DetectorHistory:     []*contracts.DetectorEvaluation{{DetectorId: "workload", Status: "clear", EvaluatedAt: timestamppb.New(at.Add(3 * time.Second))}},
	}
	DropReplacedOwnedPods(outcome, writeRolloutCapture(t, at, []corev1.Pod{recreatedPod("search-0", "search-old", "before")}, []corev1.Pod{recreatedPod("search-0", "search-old", "after")}))
	if causes := VerifiedReflectionCauses(outcome); len(causes) != 1 || len(causes[0].GetResources()) != 1 {
		t.Fatalf("same-name recreated Pod of the repaired Deployment blocked verification: %v", causes)
	}
}

func TestReplacedOwnedPodDoesNotBlockVerifiedWorkloadRepair(t *testing.T) {
	at := time.Unix(100, 0)
	deployment := &contracts.ObjectRef{ApiVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "search"}
	replaced := &contracts.ObjectRef{ApiVersion: "v1", Kind: "Pod", Namespace: "app", Name: "search-old-abcde"}
	outcome := &contracts.OutcomeRecord{
		Classification:      "success",
		ConfirmedRootCauses: []*contracts.ConfirmedRootCause{{Resources: []*contracts.ObjectRef{deployment, replaced}}},
		Findings:            []*contracts.Finding{{DetectorId: "workload", Status: "active", Fingerprint: "search", PrimaryResource: deployment}},
		RepairActions:       []*contracts.RepairActionReceipt{{Success: true, Resources: []*contracts.ObjectRef{deployment}, StartedAt: timestamppb.New(at.Add(time.Second)), CompletedAt: timestamppb.New(at.Add(2 * time.Second))}},
		DetectorHistory:     []*contracts.DetectorEvaluation{{DetectorId: "workload", Status: "clear", EvaluatedAt: timestamppb.New(at.Add(3 * time.Second))}},
	}
	rollout := writeRolloutCapture(t, at, []corev1.Pod{pod("search-old-abcde", "search-old")}, []corev1.Pod{pod("search-new-fghij", "search-new")})

	if causes := VerifiedReflectionCauses(outcome); len(causes) != 0 {
		t.Fatalf("the replaced Pod has no finding of its own and must not verify unaided: %v", causes)
	}
	got := proto.Clone(outcome).(*contracts.OutcomeRecord)
	DropReplacedOwnedPods(got, rollout)
	causes := VerifiedReflectionCauses(got)
	if len(causes) != 1 || len(causes[0].GetResources()) != 1 || !equalCauseResource(causes[0].GetResources()[0], deployment) {
		t.Fatalf("replaced Pod of the repaired Deployment blocked verification: %v", causes)
	}

	for name, tc := range map[string]struct {
		change   func(*contracts.OutcomeRecord)
		open     []corev1.Pod
		fixed    []corev1.Pod
		resource *contracts.ObjectRef
	}{
		"unrelated pod still blocks": {
			resource: &contracts.ObjectRef{Kind: "Pod", Namespace: "app", Name: "other-old-abcde"},
			open:     []corev1.Pod{pod("other-old-abcde", "other-old")},
		},
		"surviving owned pod still blocks": {
			resource: replaced,
			open:     []corev1.Pod{pod("search-old-abcde", "search-old")},
			fixed:    []corev1.Pod{pod("search-old-abcde", "search-old")},
		},
		"pod without observed owner still blocks": {
			resource: replaced,
			open:     []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "search-old-abcde", Namespace: "app"}}},
		},
		"uncleared owner still fails": {
			resource: replaced,
			open:     []corev1.Pod{pod("search-old-abcde", "search-old")},
			change: func(o *contracts.OutcomeRecord) {
				o.DetectorHistory[0].Status = "firing"
				o.DetectorHistory[0].Fingerprints = []string{"search"}
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			o := proto.Clone(outcome).(*contracts.OutcomeRecord)
			o.ConfirmedRootCauses[0].Resources = []*contracts.ObjectRef{deployment, tc.resource}
			if tc.change != nil {
				tc.change(o)
			}
			DropReplacedOwnedPods(o, writeRolloutCapture(t, at, tc.open, tc.fixed))
			if causes := VerifiedReflectionCauses(o); len(causes) != 0 {
				t.Fatalf("unverified cause authorized: %v", causes)
			}
		})
	}
}
