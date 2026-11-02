package runtime

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func TestControllerCapturesHealthyRefusedPortsBeforeIncident(t *testing.T) {
	ctx := context.Background()
	provider := &changingProvider{snapshot: sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "consul", Namespace: "app"}, Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 8300}, {Port: 8400}}}}}}}
	root := t.TempDir()
	config := testControllerConfig()
	config.Namespace = "app"
	config.CorpusSessionRoot = root
	config.FiringThreshold = 1
	detector := &sequenceDetector{spec: sdk.DetectorSpec{ID: "health", Interval: time.Second}, samples: [][]sdk.Finding{nil, {{DetectorID: "health", RuleID: "fault", Summary: "synthetic fault", Evidence: "synthetic fixture", Status: sdk.FindingActive, Severity: sdk.SeverityCritical, PrimaryResource: sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "api"}}}}}
	c, err := NewController(config, []sdk.Detector{detector}, provider, &recordingDispatcher{}, time.Unix(100, 0))
	if err != nil {
		t.Fatal(err)
	}
	c.OnError = func(err error) { t.Log(err) }
	c.drift = &DriftControllerConfig{View: func(s sdk.DetectionContext) program.ReadView {
		return LiveReadView{Carrier: inputs.New(s), Prober: &routingTestProber{}}
	}}
	if err := c.EvaluateAll(ctx, time.Unix(100, 0)); err != nil {
		t.Fatal(err)
	}
	c.config.SourceCommit = "accepted-memory-only-advance"
	if err := c.EvaluateAll(ctx, time.Unix(101, 0)); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	request := cloneIncidentRequest(c.currentIncidentRequest)
	c.mu.Unlock()
	if request == nil {
		t.Fatalf("incident did not open: history=%+v samples=%d", c.history, detector.calls)
	}
	c.Wait()
	dir, err := memory.IncidentCorpusDir(root, request.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "healthy", "healthy.json"))
	if err != nil {
		t.Fatalf("controller baseline missing: %v", err)
	}
	var baseline sdktest.Snapshot
	if err := json.Unmarshal(raw, &baseline); err != nil {
		t.Fatal(err)
	}
	if len(baseline.ServiceList) != 1 || len(baseline.ServiceList[0].Spec.Ports) != 2 {
		t.Fatalf("healthy service altered: %+v", baseline.ServiceList)
	}
	for _, port := range []int32{8300, 8400} {
		result, err := baseline.RuntimeReadView().ProbeTCP(sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "consul"}, port)
		if err != nil || result.Success {
			t.Fatalf("healthy refused port %d lost: %+v %v", port, result, err)
		}
	}
}
