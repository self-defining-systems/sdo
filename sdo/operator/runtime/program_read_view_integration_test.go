//go:build integration

package runtime

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk/inputs"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func TestLiveReadViewUsesSnapshotAndReadOnlyPrimitives(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	pod := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "api-pod"}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" || r.URL.Path != "/api/v1/namespaces/app/pods/api-pod/log" {
			t.Errorf("unexpected read %s %s", r.Method, r.URL.Path)
		}
		_, _ = w.Write([]byte("captured logs"))
	}))
	defer server.Close()
	client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	prober := &fakePrimitiveProber{}
	view := LiveReadView{Carrier: inputs.New(sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"}, Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 80}}}}}}), Client: client, Prober: prober, Context: context.Background(), Baseline: &StateChanges{Changes: []StateChange{{Kind: "Service", Name: "api", Change: StateChangeModified, Fields: []StateFieldChange{{Field: "spec.ports", Before: "80", After: "81"}}}}}}
	if len(view.Services()) != 1 {
		t.Fatal("snapshot was not exposed")
	}
	if got, err := view.Logs(pod, "", time.Minute); err != nil || got != "captured logs" {
		t.Fatalf("pod logs: %q %v", got, err)
	}
	if got, err := view.ProbeTCP(ref, 80); err != nil || !got.Success {
		t.Fatalf("tcp: %+v %v", got, err)
	}
	if got, err := view.ProbeHTTP(ref, program.HTTPProbe{Method: "GET", Path: "/"}); err != nil || !got.Success {
		t.Fatalf("http: %+v %v", got, err)
	}
	if diff, err := view.DiffAgainstBaseline(ref); err != nil || !diff.Changed || !strings.Contains(diff.Text, "spec.ports") {
		t.Fatalf("baseline diff: %+v %v", diff, err)
	}
	if len(prober.calls) != 2 {
		t.Fatalf("unexpected probes: %v", prober.calls)
	}
}

