package runtime

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"sdo.dev/operator/sdk/inputs"

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

func (b *changingBaseline) Observe(time.Time, bool) {}

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
	view := LiveReadView{Carrier: inputs.New(sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"}, Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 81}}}}}}), SourceCommit: "accepted", Source: planFakeSource{source: &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Service", "metadata": map[string]any{"namespace": "app", "name": "api"}, "spec": map[string]any{"ports": []any{map[string]any{"port": float64(80)}}}}}}}
	diff, err := view.DiffAgainstSource(ref)
	if err != nil || !diff.Changed || !strings.Contains(diff.Text, "81") {
		t.Fatalf("source diff: %+v %v", diff, err)
	}
}

func TestLiveReadViewIgnoresKubernetesDefaultsAbsentFromSource(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	source := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Service", "metadata": map[string]any{"namespace": "app", "name": "api"}, "spec": map[string]any{"ports": []any{map[string]any{"port": float64(80)}}}}}
	accepted := func(service corev1.Service) corev1.Service {
		service.Annotations = withAcceptance(t, &service, source)
		return service
	}
	view := LiveReadView{Carrier: inputs.New(sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{accepted(corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"}, Spec: corev1.ServiceSpec{ClusterIP: "10.0.0.1", Ports: []corev1.ServicePort{{Port: 80, Protocol: corev1.ProtocolTCP}}}})}}), SourceCommit: "accepted", Source: planFakeSource{source: source}}
	diff, err := view.DiffAgainstSource(ref)
	if err != nil || diff.Changed {
		t.Fatalf("Kubernetes defaults are not source drift: %+v %v", diff, err)
	}
}

func TestLiveReadViewNormalizesSourceWithServerSideDryRun(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	source := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Service", "metadata": map[string]any{"namespace": "app", "name": "api"}, "spec": map[string]any{"ports": []any{map[string]any{"port": float64(80)}}}}}
	accepted := func(service corev1.Service) corev1.Service {
		service.Annotations = withAcceptance(t, &service, source)
		return service
	}
	called := false
	server := newHandlerTransport(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if serveTestDiscovery(writer, request) {
			return
		}
		called = request.Method == http.MethodPatch && request.URL.Query().Get("fieldManager") == "sdo-deployer" && request.URL.Query().Get("dryRun") == metav1.DryRunAll && request.URL.Query().Get("force") == "true" && request.Header.Get("Content-Type") == "application/apply-patch+yaml"
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(&corev1.Service{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Service"}, ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"}, Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 80, Protocol: corev1.ProtocolTCP}}}})
	}))

	client, err := kubernetes.NewForConfig(&rest.Config{Host: "http://in-memory", Transport: server})
	if err != nil {
		t.Fatal(err)
	}
	view := LiveReadView{Carrier: inputs.New(sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{accepted(corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"}, Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 80, Protocol: corev1.ProtocolTCP}}}})}}), Client: client, SourceCommit: "accepted", Source: planFakeSource{source: source}}
	diff, err := view.DiffAgainstSource(ref)
	if err != nil || diff.Changed || !called {
		t.Fatalf("source was not normalized by dry run: %+v, %v, called=%v", diff, err, called)
	}
}

func TestLiveReadViewDiffsDeclaredLabelsAgainstSource(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "settings"}
	server := newHandlerTransport(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if serveTestDiscovery(writer, request) {
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(&corev1.ConfigMap{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"}, ObjectMeta: metav1.ObjectMeta{Name: "settings", Namespace: "app", Labels: map[string]string{"app": "accepted"}}})
	}))

	client, err := kubernetes.NewForConfig(&rest.Config{Host: "http://in-memory", Transport: server})
	if err != nil {
		t.Fatal(err)
	}
	view := LiveReadView{Carrier: inputs.New(sdktest.Snapshot{NamespaceName: "app", ConfigMapList: []corev1.ConfigMap{{ObjectMeta: metav1.ObjectMeta{Name: "settings", Namespace: "app", Labels: map[string]string{"app": "changed"}}}}}), Client: client, SourceCommit: "accepted", Source: planFakeSource{source: &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"namespace": "app", "name": "settings", "labels": map[string]any{"app": "accepted"}}}}}}
	diff, err := view.DiffAgainstSource(ref)
	if err != nil || !diff.Changed {
		t.Fatalf("declared label change was not source drift: %+v %v", diff, err)
	}
}

func TestLiveReadViewIgnoresDeploymentRevisionStamp(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "settings"}
	server := newHandlerTransport(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if serveTestDiscovery(writer, request) {
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(&corev1.ConfigMap{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"}, ObjectMeta: metav1.ObjectMeta{Name: "settings", Namespace: "app"}})
	}))

	client, err := kubernetes.NewForConfig(&rest.Config{Host: "http://in-memory", Transport: server})
	if err != nil {
		t.Fatal(err)
	}
	view := LiveReadView{Carrier: inputs.New(sdktest.Snapshot{NamespaceName: "app", ConfigMapList: []corev1.ConfigMap{{ObjectMeta: metav1.ObjectMeta{Name: "settings", Namespace: "app", Annotations: map[string]string{"sdo.dev/source-tree": "abc"}}}}}), Client: client, SourceCommit: "accepted", Source: planFakeSource{source: &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"namespace": "app", "name": "settings"}}}}}
	diff, err := view.DiffAgainstSource(ref)
	if err != nil || diff.Changed {
		t.Fatalf("deployer's revision stamp was treated as drift: %+v %v", diff, err)
	}
}

func TestLiveReadViewReportsDeletedAcceptedObject(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	view := LiveReadView{Carrier: inputs.New(clusterEvidenceSnapshot{Snapshot: sdktest.Snapshot{NamespaceName: "app"}, err: apierrors.NewNotFound(schema.GroupResource{Resource: "services"}, ref.Name)}), SourceCommit: "accepted", Source: planFakeSource{source: &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Service", "metadata": map[string]any{"namespace": "app", "name": "api"}, "spec": map[string]any{"ports": []any{map[string]any{"port": float64(80)}}}}}}}
	diff, err := view.DiffAgainstSource(ref)
	if err != nil || !diff.Changed {
		t.Fatalf("deleted source object was not drift: %+v, %v", diff, err)
	}
}

func TestLiveReadViewRefreshesHealthyBaselineAfterRepair(t *testing.T) {
	ref := sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "api"}
	baseline := &changingBaseline{changes: &StateChanges{Changes: []StateChange{{Kind: "Service", Name: "api", Change: StateChangeModified}}}}
	view := LiveReadView{Carrier: inputs.New(sdktest.Snapshot{NamespaceName: "app"}), BaselineSource: baseline}
	if diff, err := view.DiffAgainstBaseline(ref); err != nil || !diff.Changed {
		t.Fatalf("before repair: %+v %v", diff, err)
	}
	baseline.changes = &StateChanges{}
	if diff, err := view.DiffAgainstBaseline(ref); err != nil || diff.Changed {
		t.Fatalf("after repair: %+v %v", diff, err)
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
			verdict := program.RunDiagnose(context.Background(), unavailableCapabilityProgram{method: method}, LiveReadView{Carrier: inputs.New(sdktest.Snapshot{NamespaceName: "app"})}, program.Bindings{Objects: program.ObjectBindings{}}, time.Second)
			if verdict.Result != program.Unknown {
				t.Fatalf("unavailable %s cleared fault: %+v", method, verdict)
			}
		})
	}
}
