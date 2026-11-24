package memoryhost

import (
	"context"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"testing"
)

func TestMirrorDecodingKeepsRPCCopiesAndHistoricalVersionsPrivate(t *testing.T) {
	ctx := context.Background()
	host := newMirrorTestHost(t, "accepted")
	expected := "original"
	manager := NewManager(0)
	defer manager.Close()
	publish := func(value string) {
		s := sdktest.Snapshot{NamespaceName: "app", JobList: []batchv1.Job{{ObjectMeta: metav1.ObjectMeta{Name: "job", Namespace: "app", Labels: map[string]string{"value": value}}}}, PodList: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app", Labels: map[string]string{"value": value}}}}, SecretList: []sdk.SecretMetadata{{Name: "metadata-only", Namespace: "app", Labels: map[string]string{"value": value}}}}
		if err := manager.Publish(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	publish(expected)
	if err := manager.Prepare(ctx, host); err != nil {
		t.Fatal(err)
	}
	if err := manager.Activate(ctx, host, "accepted", "digest"); err != nil {
		t.Fatal(err)
	}
	for _, sequence := range []uint64{1, 1, 2, 1, 2} {
		if sequence == 2 {
			expected = "updated"
			publish(expected)
		} else {
			expected = "original"
		}
		snapshot, err := host.snapshot(sequence)
		if err != nil {
			t.Fatal(err)
		}
		if snapshot.PodList[0].Labels["value"] != expected || snapshot.SecretList[0].Labels["value"] != expected || snapshot.JobList[0].Labels["value"] != expected {
			t.Fatal("snapshot mutation crossed RPC/version boundary")
		}
		snapshot.PodList[0].Labels["value"] = "mutated"
		snapshot.SecretList[0].Labels["value"] = "mutated"
		snapshot.JobList[0].Labels["value"] = "mutated"
	}
}
