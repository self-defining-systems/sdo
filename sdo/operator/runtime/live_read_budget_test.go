package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"net/http"
	"net/http/httptest"
	"sdo.dev/operator/sdk/sdktest"
	"sync/atomic"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/program"
)

// Model the live normalization transport missing from the original tool-speed
// benchmark. Discovery is shared by kind, but SSA answers remain fresh for each
// object in every new snapshot.
func TestReadSnapshotDiscoveryBudget(t *testing.T) {
	var discoveries, normalizations atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet {
			discoveries.Add(1)
			_ = json.NewEncoder(w).Encode(metav1.APIResourceList{GroupVersion: "v1", APIResources: []metav1.APIResource{{Name: "pods", Kind: "Pod", Namespaced: true}, {Name: "services", Kind: "Service", Namespaced: true}, {Name: "deployments", Kind: "Deployment", Namespaced: true}}})
			return
		}
		if r.Method != http.MethodPatch || r.URL.Query().Get("dryRun") != "All" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL)
		}
		normalizations.Add(1)
		_, _ = io.Copy(w, r.Body)
	})
	client, err := kubernetes.NewForConfig(&rest.Config{Host: "https://synthetic.invalid", QPS: 10000, Burst: 10000, Transport: budgetRoundTripper(func(r *http.Request) (*http.Response, error) {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, r)
		return response.Result(), nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	loop, _ := speedLoop()
	loop.ReadView = func(view sdk.DetectionContext) program.ReadView {
		return LiveReadView{Carrier: inputs.New(view), Client: client, Source: speedSource{}, SourceCommit: "accepted"}
	}
	for cycle := int32(1); cycle <= 2; cycle++ {
		response := loop.Handle(context.Background(), ToolRequest{Command: "diagnose"})
		if response.Error != "" {
			t.Fatal(response.Error)
		}
		if got := discoveries.Load(); got != 2*cycle {
			t.Errorf("discovery must be once per API version per fresh snapshot: got %d want %d", got, 2*cycle)
		}
		if got := normalizations.Load(); got != 150*cycle {
			t.Errorf("live SSA must remain once per object per fresh snapshot: got %d", got)
		}
	}
}

type projectionSnapshot struct {
	sdktest.Snapshot
	projections *atomic.Int32
}

func (s projectionSnapshot) Pods() []corev1.Pod { s.projections.Add(1); return s.Snapshot.Pods() }

func (s projectionSnapshot) Object(ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	return sdk.FindObject(sdk.TypedObjects(s), ref)
}

func TestLiveSnapshotObjectProjectionBudget(t *testing.T) {
	var projections atomic.Int32
	snapshot := sdktest.Snapshot{NamespaceName: "app"}
	for i := 0; i < 80; i++ {
		snapshot.PodList = append(snapshot.PodList, corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("pod-%d", i), Namespace: "app"}})
	}
	for cycle := int32(1); cycle <= 2; cycle++ {
		view := NewCycleReadView(LiveReadView{Carrier: inputs.New(projectionSnapshot{Snapshot: snapshot, projections: &projections}), Source: speedSource{}, SourceCommit: "accepted"})
		for _, pod := range snapshot.PodList {
			ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: pod.Name}
			_, _ = view.DiffAgainstSource(ref)
		}
		if got := projections.Load(); got != cycle {
			t.Fatalf("project typed snapshot once per cycle, got %d for %d cycles", got, cycle)
		}
	}
}

type budgetRoundTripper func(*http.Request) (*http.Response, error)

func (f budgetRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
