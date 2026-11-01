package runtime

import (
	"context"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/memoryhost"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/sdktest"
	"testing"
)

func TestHostDiffInputsNeverPatch(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "toy", Name: "api"}
	snapshot := sdktest.Snapshot{NamespaceName: "toy", ServiceList: []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "toy"}, Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 81}}}}}}
	client := fake.NewSimpleClientset()
	view := LiveReadView{Context: context.Background(), Carrier: inputs.New(snapshot), Client: client, SourceCommit: "accepted", Source: planFakeSource{source: &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Service", "metadata": map[string]any{"name": "api", "namespace": "toy"}, "spec": map[string]any{"ports": []any{map[string]any{"port": float64(80)}}}}}}, Baseline: &StateChanges{Changes: []StateChange{{Kind: "Service", Name: "api", Change: StateChangeModified}}}}
	calls := view.MemoryDiffInputs([]sdk.ObjectRef{ref})
	wire, err := memoryhost.EncodeReceipts(calls)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := memoryhost.DecodeReceipts(wire)
	if err != nil {
		t.Fatal(err)
	}
	host := &sdktest.ReplayReadView{Snapshot: snapshot, Calls: decoded}
	source, err := host.DiffAgainstSource(ref)
	if err != nil || !source.Changed {
		t.Fatal(source, err)
	}
	baseline, err := host.DiffAgainstBaseline(ref)
	if err != nil || !baseline.Changed {
		t.Fatal(baseline, err)
	}
	if len(client.Actions()) != 0 {
		t.Fatal("read-only diff input called Kubernetes", client.Actions())
	}
}
