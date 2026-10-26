package runtime

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type fakePrimitiveProber struct {
	mu    sync.Mutex
	calls []string
}

type changingBaseline struct{ changes *StateChanges }

func (b *changingBaseline) Observe(time.Time, bool)         {}
func (b *changingBaseline) Changes(time.Time) *StateChanges { return b.changes }

func (p *fakePrimitiveProber) Probe(_ context.Context, ref sdk.ObjectRef, port int32, request *program.HTTPProbe) (program.ProbeResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if request == nil {
		p.calls = append(p.calls, "tcp")
	} else {
		p.calls = append(p.calls, "http")
	}
	return program.ProbeResult{Success: ref.Name == "api" && port == 80}, nil
}

func TestLiveReadViewDiffsSnapshotAgainstAcceptedManifest(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	view := LiveReadView{DetectionContext: sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"}, Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 81}}}}}}, SourceCommit: "accepted", Source: planFakeSource{source: &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Service", "metadata": map[string]any{"namespace": "app", "name": "api"}, "spec": map[string]any{"ports": []any{map[string]any{"port": float64(80)}}}}}}}
	diff, err := view.DiffAgainstSource(ref)
	if err != nil || !diff.Changed || !strings.Contains(diff.Text, "81") {
		t.Fatalf("source diff: %+v %v", diff, err)
	}
}

func TestLiveReadViewIgnoresKubernetesDefaultsAbsentFromSource(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	view := LiveReadView{DetectionContext: sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"}, Spec: corev1.ServiceSpec{ClusterIP: "10.0.0.1", Ports: []corev1.ServicePort{{Port: 80, Protocol: corev1.ProtocolTCP}}}}}}, SourceCommit: "accepted", Source: planFakeSource{source: &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Service", "metadata": map[string]any{"namespace": "app", "name": "api"}, "spec": map[string]any{"ports": []any{map[string]any{"port": float64(80)}}}}}}}
	diff, err := view.DiffAgainstSource(ref)
	if err != nil || diff.Changed {
		t.Fatalf("Kubernetes defaults are not source drift: %+v %v", diff, err)
	}
}

func TestLiveReadViewNormalizesSourceWithServerSideDryRun(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	called := false
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		called = request.Method == http.MethodPatch && request.URL.Query().Get("fieldManager") == "sdo-deployer" && request.URL.Query().Get("dryRun") == metav1.DryRunAll && request.URL.Query().Get("force") == "true" && request.Header.Get("Content-Type") == "application/apply-patch+yaml"
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(&corev1.Service{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Service"}, ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"}, Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 80, Protocol: corev1.ProtocolTCP}}}})
	}))
	defer server.Close()
	client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	view := LiveReadView{DetectionContext: sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"}, Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 80, Protocol: corev1.ProtocolTCP}}}}}}, Client: client, SourceCommit: "accepted", Source: planFakeSource{source: &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Service", "metadata": map[string]any{"namespace": "app", "name": "api"}, "spec": map[string]any{"ports": []any{map[string]any{"port": float64(80)}}}}}}}
	diff, err := view.DiffAgainstSource(ref)
	if err != nil || diff.Changed || !called {
		t.Fatalf("source was not normalized by dry run: %+v, %v, called=%v", diff, err, called)
	}
}

func TestLiveReadViewDiffsDeclaredLabelsAgainstSource(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "settings"}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(&corev1.ConfigMap{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"}, ObjectMeta: metav1.ObjectMeta{Name: "settings", Namespace: "app", Labels: map[string]string{"app": "accepted"}}})
	}))
	defer server.Close()
	client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	view := LiveReadView{DetectionContext: sdktest.Snapshot{NamespaceName: "app", ConfigMapList: []corev1.ConfigMap{{ObjectMeta: metav1.ObjectMeta{Name: "settings", Namespace: "app", Labels: map[string]string{"app": "changed"}}}}}, Client: client, SourceCommit: "accepted", Source: planFakeSource{source: &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"namespace": "app", "name": "settings", "labels": map[string]any{"app": "accepted"}}}}}}
	diff, err := view.DiffAgainstSource(ref)
	if err != nil || !diff.Changed {
		t.Fatalf("declared label change was not source drift: %+v %v", diff, err)
	}
}

func TestLiveReadViewIgnoresDeploymentRevisionStamp(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "settings"}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(&corev1.ConfigMap{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"}, ObjectMeta: metav1.ObjectMeta{Name: "settings", Namespace: "app"}})
	}))
	defer server.Close()
	client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	view := LiveReadView{DetectionContext: sdktest.Snapshot{NamespaceName: "app", ConfigMapList: []corev1.ConfigMap{{ObjectMeta: metav1.ObjectMeta{Name: "settings", Namespace: "app", Annotations: map[string]string{"sdo.dev/source-tree": "abc"}}}}}, Client: client, SourceCommit: "accepted", Source: planFakeSource{source: &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"namespace": "app", "name": "settings"}}}}}
	diff, err := view.DiffAgainstSource(ref)
	if err != nil || diff.Changed {
		t.Fatalf("deployer's revision stamp was treated as drift: %+v %v", diff, err)
	}
}

func TestLiveReadViewReportsDeletedAcceptedObject(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	view := LiveReadView{DetectionContext: clusterEvidenceSnapshot{Snapshot: sdktest.Snapshot{NamespaceName: "app"}, err: apierrors.NewNotFound(schema.GroupResource{Resource: "services"}, ref.Name)}, SourceCommit: "accepted", Source: planFakeSource{source: &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Service", "metadata": map[string]any{"namespace": "app", "name": "api"}, "spec": map[string]any{"ports": []any{map[string]any{"port": float64(80)}}}}}}}
	diff, err := view.DiffAgainstSource(ref)
	if err != nil || !diff.Changed {
		t.Fatalf("deleted source object was not drift: %+v, %v", diff, err)
	}
}

func TestLiveReadViewRefreshesHealthyBaselineAfterRepair(t *testing.T) {
	ref := sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "api"}
	baseline := &changingBaseline{changes: &StateChanges{Changes: []StateChange{{Kind: "Service", Name: "api", Change: StateChangeModified}}}}
	view := LiveReadView{DetectionContext: sdktest.Snapshot{NamespaceName: "app"}, BaselineSource: baseline}
	if diff, err := view.DiffAgainstBaseline(ref); err != nil || !diff.Changed {
		t.Fatalf("before repair: %+v %v", diff, err)
	}
	baseline.changes = &StateChanges{}
	if diff, err := view.DiffAgainstBaseline(ref); err != nil || diff.Changed {
		t.Fatalf("after repair: %+v %v", diff, err)
	}
}

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
	view := LiveReadView{DetectionContext: sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"}, Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 80}}}}}}, Client: client, Prober: prober, Context: context.Background(), Baseline: &StateChanges{Changes: []StateChange{{Kind: "Service", Name: "api", Change: StateChangeModified, Fields: []StateFieldChange{{Field: "spec.ports", Before: "80", After: "81"}}}}}}
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

type unavailableCapabilityProgram struct{ method string }

func (p unavailableCapabilityProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "capability"}
}
func (p unavailableCapabilityProgram) Diagnose(_ context.Context, v program.ReadView, _ program.Bindings) (program.Verdict, error) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	switch p.method {
	case "prober":
		_, _ = v.ProbeTCP(ref, 80)
	case "baseline":
		_, _ = v.DiffAgainstBaseline(ref)
	}
	return program.Verdict{Result: program.NoMatch}, nil
}
func TestUnavailableSweepCapabilityCannotClearFault(t *testing.T) {
	for _, method := range []string{"prober", "baseline"} {
		t.Run(method, func(t *testing.T) {
			verdict := program.RunDiagnose(context.Background(), unavailableCapabilityProgram{method: method}, LiveReadView{DetectionContext: sdktest.Snapshot{NamespaceName: "app"}}, program.Bindings{Objects: program.ObjectBindings{}}, time.Second)
			if verdict.Result != program.Unknown {
				t.Fatalf("unavailable %s cleared fault: %+v", method, verdict)
			}
		})
	}
}
