package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"sdo.dev/operator/core"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/workloadhealth"
)

func TestEvaluateOnceChecksPodAdmission(t *testing.T) {
	client := fake.NewSimpleClientset()
	client.PrependReactor("create", "pods", func(action ktesting.Action) (bool, kruntime.Object, error) {
		return true, nil, apierrors.NewInternalError(errors.New("admission unavailable"))
	})
	detector := workloadhealth.NewPodAdmissionDetector(sdk.DetectorSpec{ID: "pod-admission", OriginatingCommit: "synthetic-test", Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge, Batching: sdk.BatchingPolicy{Severity: sdk.SeverityCritical}, Interval: time.Second, Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1}})
	var stdout, stderr strings.Builder
	err := evaluateOnce(context.Background(), []sdk.Detector{detector}, []string{"--namespace=app", "--accept-seed"}, &stdout, &stderr, &core.KubernetesSnapshotProvider{Namespace: "app", Client: client}, nil, nil)
	if err == nil || !strings.Contains(stdout.String(), "admission unavailable") {
		t.Fatalf("one-shot health missed rejection: err=%v stdout=%s stderr=%s", err, &stdout, &stderr)
	}
}
