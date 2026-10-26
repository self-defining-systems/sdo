package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"sdo.dev/operator/core"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/workloadhealth"
)

func TestPodAdmissionBlocksVerificationUntilAPIRecovers(t *testing.T) {
	for _, scale := range []float64{1, 0.1} {
		t.Run(fmt.Sprint(scale), func(t *testing.T) { testPodAdmissionRecovery(t, scale) })
	}
}

func testPodAdmissionRecovery(t *testing.T, scale float64) {
	client := fake.NewSimpleClientset(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "ready-api"}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "api", Image: "example/api:1"}}}, Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}})
	var probeErr error = apierrors.NewInternalError(errors.New("admission TLS certificate invalid"))
	calls := 0
	client.PrependReactor("create", "pods", func(action ktesting.Action) (bool, kruntime.Object, error) {
		calls++
		return true, action.(ktesting.CreateAction).GetObject(), probeErr
	})
	detector := workloadhealth.NewPodAdmissionDetector(sdk.DetectorSpec{ID: "pod-admission", OriginatingCommit: "synthetic-test", Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge, Batching: sdk.BatchingPolicy{Severity: sdk.SeverityCritical}, Interval: time.Second, Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1}, Watches: []sdk.WatchKind{{APIVersion: "v1", Kind: "Pod"}}})
	detectors := scaleDetectors([]sdk.Detector{detector}, scale)
	cache, err := NewKubernetesCache(KubernetesCacheConfig{Namespace: "app", Client: client}, detectors)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache.Start(ctx)
	if err := cache.WaitForSync(ctx); err != nil {
		t.Fatal(err)
	}
	// Exercise wrappers used by the controller and responder. The frozen
	// observation must survive traffic, metrics and the cycle's read view.
	base := TrafficSnapshotProvider{Base: cache}
	provider := ReadSnapshotProvider{Base: base, View: func(snapshot sdk.DetectionContext) program.ReadView {
		return program.ReadFrom(metricsContext{DetectionContext: snapshot, ScopedObservationSource: scopedSource(snapshot)})
	}}
	loop := NewResponderLoop(IncidentRequest{IncidentID: "incident", Namespace: "app"}, nil, provider, detectors, PlanExecutor{}, 1)
	loop.ReadView = program.ReadFrom
	for _, command := range []string{"status", "verify"} {
		result := loop.Handle(ctx, ToolRequest{Command: command})
		if result.Healthy || len(result.Findings) != 1 || !strings.Contains(result.Findings[0].Evidence, "certificate invalid") {
			t.Fatalf("%s cleared blocked admission: %+v", command, result)
		}
	}
	probeErr = context.DeadlineExceeded
	result := loop.Handle(ctx, ToolRequest{Command: "verify"})
	if result.Healthy {
		t.Fatalf("timeout cleared admission: %+v", result)
	}
	probeErr = nil
	result = loop.Handle(ctx, ToolRequest{Command: "verify"})
	if !result.Healthy || len(result.Findings) != 0 {
		t.Fatalf("repaired API did not clear: %+v", result)
	}
	if calls != 4 {
		t.Fatalf("expected a fresh dry-run per health snapshot, got %d", calls)
	}
	pods, err := client.CoreV1().Pods("app").List(ctx, metav1.ListOptions{})
	if err != nil || len(pods.Items) != 1 {
		t.Fatalf("probe persisted Pods: %v %v", pods, err)
	}
	snapshot, err := provider.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	captured, err := sdktest.CaptureSnapshot(snapshot, "app")
	if err != nil {
		t.Fatal(err)
	}
	observation, ok := captured.PodAdmission()
	if !ok || !observation.Admitted {
		t.Fatalf("capture lost observation: %+v", captured)
	}
}

func TestEvaluateOnceChecksPodAdmission(t *testing.T) {
	client := fake.NewSimpleClientset()
	client.PrependReactor("create", "pods", func(action ktesting.Action) (bool, kruntime.Object, error) {
		return true, nil, apierrors.NewInternalError(errors.New("admission unavailable"))
	})
	detector := workloadhealth.NewPodAdmissionDetector(sdk.DetectorSpec{ID: "pod-admission", OriginatingCommit: "synthetic-test", Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge, Batching: sdk.BatchingPolicy{Severity: sdk.SeverityCritical}, Interval: time.Second, Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1}})
	var stdout, stderr strings.Builder
	err := evaluateOnce(context.Background(), []sdk.Detector{detector}, []string{"--namespace=app", "--accept-seed"}, &stdout, &stderr, &core.KubernetesSnapshotProvider{Namespace: "app", Client: client})
	if err == nil || !strings.Contains(stdout.String(), "admission unavailable") {
		t.Fatalf("one-shot health missed rejection: err=%v stdout=%s stderr=%s", err, &stdout, &stderr)
	}
}
