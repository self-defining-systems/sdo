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
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"strings"
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

type sourceDriftDetector struct{ timingDetector }

func (sourceDriftDetector) Detect(_ context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "toy", Name: "api"}
	diff, err := view.DiffAgainstSource(ref)
	if err != nil {
		return nil, err
	}
	if !diff.Changed {
		return nil, nil
	}
	return []sdk.Finding{{RuleID: "drift", Summary: "service differs from accepted source", PrimaryResource: ref}}, nil
}

type opaqueReadView struct{ program.ReadView }

func TestRemoteDetectorReadsSourceDiffsTheControllerFroze(t *testing.T) {
	cycle := func(live program.ReadView) *CycleReadView { return NewCycleReadView(live) }
	for name, tc := range map[string]struct {
		wrap    func(program.ReadView) sdk.DetectionContext
		wantErr string
	}{
		"cycle view": {wrap: func(live program.ReadView) sdk.DetectionContext { return cycle(live) }},
		"cycle view after onboarding": {wrap: func(live program.ReadView) sdk.DetectionContext {
			return NewCycleReadView(onboardingReadView{ReadView: cycle(live)})
		}},
		"frozen metrics over the cycle view": {wrap: func(live program.ReadView) sdk.DetectionContext {
			return metricsContext{Carrier: inputs.New(cycle(live))}
		}},
		"traffic over frozen logs over the cycle view": {wrap: func(live program.ReadView) sdk.DetectionContext {
			return trafficContext{Carrier: inputs.New(inputs.New(cycle(live)).WithLogs(nil))}
		}},
		"view that hides its source": {wrap: func(live program.ReadView) sdk.DetectionContext {
			return NewCycleReadView(opaqueReadView{cycle(live)})
		}, wantErr: "unrecorded DiffAgainstSource"},
	} {
		t.Run(name, func(t *testing.T) { remoteDetectorReadsSourceDiffs(t, tc.wrap, tc.wantErr) })
	}
}

func remoteDetectorReadsSourceDiffs(t *testing.T, wrap func(program.ReadView) sdk.DetectionContext, wantErr string) {
	ctx := context.Background()
	spec := sdk.DetectorSpec{ID: "source-drift", Watches: []sdk.WatchKind{{APIVersion: "v1", Kind: "Service"}}}
	s, err := memoryhost.NewServer("c", "d", []sdk.Detector{sourceDriftDetector{timingDetector{DetectorSpec: spec}}})
	if err != nil {
		t.Fatal(err)
	}
	ready, _ := s.Ready(ctx, nil)
	rpc, _ := memoryTestRPC(t, s)
	remote, err := NewRemoteMemory(ready, mirroredTestHost(t, ctx, rpc, "c", "d", "toy"))
	if err != nil {
		t.Fatal(err)
	}
	snapshot := sdktest.Snapshot{NamespaceName: "toy", ServiceList: []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "toy"}, Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 81}}}}}}
	source := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Service", "metadata": map[string]any{"name": "api", "namespace": "toy"}, "spec": map[string]any{"ports": []any{map[string]any{"port": float64(80)}}}}}
	live := LiveReadView{Context: ctx, Carrier: inputs.New(snapshot), Client: fake.NewSimpleClientset(), SourceCommit: "accepted", Source: planFakeSource{source: source}}
	// The controller hands detectors the cycle view the snapshot provider builds around its live view.
	findings, err := remote.Detectors[0].Detect(ctx, wrap(live))
	if wantErr != "" {
		if err == nil || !strings.Contains(err.Error(), wantErr) {
			t.Fatal(findings, err)
		}
		return
	}
	if err != nil || len(findings) != 1 || findings[0].RuleID != "drift" {
		t.Fatal(findings, err)
	}
}
