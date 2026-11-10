package podread_test

import (
	"encoding/json"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk/podread"
)

func ownedPod(kind, owner string, labels map[string]string) corev1.Pod {
	yes := true
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "member", Labels: labels, OwnerReferences: []metav1.OwnerReference{{Kind: kind, Name: owner, Controller: &yes}}},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "example.invalid/store:1"}}},
	}
}

// A Go-installed reader knows a store only by the application map's workload
// ("Kind/name"), so a target can select the Pods that workload controls.
func TestTargetSelectsByOwningWorkload(t *testing.T) {
	for name, test := range map[string]struct {
		pod      corev1.Pod
		workload string
		selected bool
	}{
		"stateful set member":       {ownedPod("StatefulSet", "store", nil), "StatefulSet/store", true},
		"other stateful set":        {ownedPod("StatefulSet", "store-b", nil), "StatefulSet/store", false},
		"deployment through its rs": {ownedPod("ReplicaSet", "store-5d9f", map[string]string{"pod-template-hash": "5d9f"}), "Deployment/store", true},
		"rs of another deployment":  {ownedPod("ReplicaSet", "store-b-5d9f", map[string]string{"pod-template-hash": "5d9f"}), "Deployment/store", false},
		"kind must match":           {ownedPod("DaemonSet", "store", nil), "StatefulSet/store", false},
		"bare pod":                  {corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "store"}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main"}}}}, "StatefulSet/store", false},
	} {
		t.Run(name, func(t *testing.T) {
			target := podread.Target{Workload: test.workload}
			if err := target.Validate(); err != nil {
				t.Fatal(err)
			}
			container, ok := target.Select(test.pod)
			if ok != test.selected || (ok && container != "main") {
				t.Fatalf("Select = %q, %v", container, ok)
			}
		})
	}
	for _, bad := range []string{"store", "Service/store", "StatefulSet/", "/store"} {
		if err := (podread.Target{Workload: bad}).Validate(); err == nil {
			t.Errorf("workload %q accepted", bad)
		}
	}
}

// The readings of the last healthy baseline travel with the current result, so
// a remote detector and a replayed capture see the same reference.
func TestResultCarriesItsHealthyBaseline(t *testing.T) {
	result := podread.Result{Readings: []podread.Reading{{Pod: "p", Stdout: "now"}}, Baseline: []podread.Reading{{Pod: "p", Stdout: "then"}}}
	data, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	var back podread.Result
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatal(err)
	}
	if len(back.Baseline) != 1 || back.Baseline[0].Stdout != "then" {
		t.Fatalf("baseline = %+v", back.Baseline)
	}
}
