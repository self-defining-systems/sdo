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

// A store read grounds its target in the application map, which the host's
// empty replay never recorded: the host must answer it from the map it was
// built with, or every app-state reader fails with "unrecorded AppMap call".
func TestLiveViewAnswersTheApplicationMapItWasBuiltWith(t *testing.T) {
	m, err := ApplicationMapFromYAML("components:\n  - name: db\n    workload: StatefulSet/db\n")
	if err != nil || m == nil {
		t.Fatalf("ApplicationMapFromYAML = %v, %v", m, err)
	}
	view := NewLiveReadView(context.Background(), sdktest.Snapshot{NamespaceName: "app"}, ReadBackends{ApplicationMap: m})
	got, err := view.AppMap()
	if err != nil || len(got.Components) != 1 || got.Components[0].Name != "db" {
		t.Fatalf("AppMap = %+v, %v", got, err)
	}
	if none, err := ApplicationMapFromYAML(""); none != nil || err != nil {
		t.Fatalf("empty map = %v, %v", none, err)
	}
}
