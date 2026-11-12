package hostio

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

// The host's live view embeds an empty replay view for snapshot-backed reads.
// A cluster read must be answered live, never as an unrecorded call.
func TestLiveViewReadsLogsWithOptionsLive(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "worker-0", Namespace: "app"}}
	view := NewLiveReadView(context.Background(), sdktest.Snapshot{NamespaceName: "app"}, ReadBackends{Client: fake.NewSimpleClientset(pod)})
	text, err := view.LogsWithOptions(sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "worker-0"}, corev1.PodLogOptions{Container: "app"})
	var unrecorded *sdktest.UnrecordedError
	if errors.As(err, &unrecorded) {
		t.Fatalf("live log read answered from the empty replay recording: %v", err)
	}
	if err != nil || text == "" {
		t.Fatalf("LogsWithOptions = %q, %v", text, err)
	}
}
