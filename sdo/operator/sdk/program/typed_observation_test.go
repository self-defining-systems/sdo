package program_test

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type typedReadDetector struct{ read func(program.ReadView) }

func (typedReadDetector) Spec() sdk.DetectorSpec { return sdk.DetectorSpec{ID: "typed-read"} }
func (d typedReadDetector) Detect(_ context.Context, s sdk.DetectionContext) ([]sdk.Finding, error) {
	d.read(program.ReadFrom(s))
	return nil, nil
}

func TestUnavailableTypedReadsCannotClearDetectionOrDiagnose(t *testing.T) {
	pod := sdk.ObjectRef{Kind: "Pod", Namespace: "demo", Name: "api"}
	for _, tc := range []struct {
		name string
		read func(program.ReadView)
	}{
		{"logs", func(v program.ReadView) {
			_, _ = v.LogsWithOptions(pod, corev1.PodLogOptions{Container: "app", Previous: true})
		}},
		{"pod usage", func(v program.ReadView) { _, _ = v.PodUsage(pod) }},
		{"node usage", func(v program.ReadView) { _, _ = v.NodeUsage(sdk.ObjectRef{Kind: "Node", Name: "worker"}) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := sdktest.Snapshot{NamespaceName: "demo"}
			p := diagnostic{fn: func(_ context.Context, v program.ReadView, _ program.Bindings) (program.Verdict, error) {
				tc.read(v)
				return program.Verdict{Result: program.NoMatch}, nil
			}}
			b := program.Bindings{Objects: program.ObjectBindings{"service": serviceRef}}
			if got := program.RunDiagnose(context.Background(), p, program.ReadFrom(snapshot), b, time.Second); got.Result != program.Unknown {
				t.Fatalf("ignored missing read cleared Diagnose: %+v", got)
			}
			if _, err := sdk.DetectObserved(context.Background(), typedReadDetector{tc.read}, snapshot); err == nil {
				t.Fatal("ignored missing read cleared detector")
			}
		})
	}
}

func TestUpstreamLogOptionsRemainBoundedWithoutMutatingCaller(t *testing.T) {
	pointer := func(v int64) *int64 { return &v }
	now := metav1.NewTime(time.Date(2026, 10, 26, 0, 0, 0, 0, time.UTC))
	for _, options := range []corev1.PodLogOptions{
		{Follow: true}, {SinceTime: &now, SinceSeconds: pointer(1)},
		{SinceSeconds: pointer(0)}, {SinceSeconds: pointer(-1)},
		{TailLines: pointer(-1)}, {LimitBytes: pointer(0)}, {LimitBytes: pointer(-1)}, {LimitBytes: pointer(65537)},
	} {
		if _, err := program.BoundedLogOptions(options); err == nil {
			t.Errorf("invalid upstream options accepted: %+v", options)
		}
	}
	original := corev1.PodLogOptions{Container: "app", Previous: true, Timestamps: true, TailLines: pointer(0), SinceTime: &now}
	bounded, err := program.BoundedLogOptions(original)
	if err != nil || bounded.LimitBytes == nil || *bounded.LimitBytes != 65536 || original.LimitBytes != nil {
		t.Fatalf("default bounds changed caller: %+v %+v %v", original, bounded, err)
	}
	*bounded.TailLines = 3
	if *original.TailLines != 0 {
		t.Fatal("bounded options alias caller pointers")
	}
}
