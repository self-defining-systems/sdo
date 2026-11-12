package runtime

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"

	"sdo.dev/operator/evaluation"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

func classedDetector(id string, class sdk.DetectorClass, firing bool) *sequenceDetector {
	finding := sdk.Finding{}
	if firing {
		finding = stateFinding(id)
	}
	detector := controllerDetector(id, time.Second, finding)
	detector.spec.Class = class
	detector.spec.Persistence = sdk.PersistencePolicy{Firing: 1, Clearing: 1}
	detector.spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityWarning, Debounce: time.Second}
	detector.spec.OriginatingCommit = "synthetic"
	detector.spec.OriginatingIncident = "synthetic-incident"
	detector.spec.Playbooks = []string{"synthetic"}
	detector.spec.Owner = sdk.DetectorOwnerResponder
	if class == sdk.DetectorClassHealth {
		detector.spec.Owner = sdk.DetectorOwnerHealthJudge
	}
	return detector
}

func evaluateOnceClassed(t *testing.T, detectors []sdk.Detector, extra ...string) (evaluation.Verdict, error) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	args := append([]string{"--evaluate-once", "--namespace", "shop"}, extra...)
	err := RunWithOptions(context.Background(), detectors, RuntimeOptions{
		Args: args, Provider: staticProvider{snapshot: syntheticShopSnapshot()}, Client: fake.NewSimpleClientset(),
		Stdout: &stdout, Stderr: &stderr,
	})
	if err != nil {
		return evaluation.Verdict{}, err
	}
	verdict, err := evaluation.Decode(stdout.Bytes())
	if err != nil {
		t.Fatalf("decode %q: %v", stdout.String(), err)
	}
	return verdict, nil
}

// A learned incident detector (a drift detector comparing live workloads with
// the accepted source) fires while an advance legitimately changes the live
// state. The gate judges health-class detectors only: the incident detector
// neither holds the advance nor hides a firing health detector.
func TestEvaluateOnceGatesOnTheSelectedClassesOnly(t *testing.T) {
	drift := classedDetector("deployment-image-drift", sdk.DetectorClassIncident, true)
	healthy := classedDetector("workload-health", sdk.DetectorClassHealth, false)
	gating := evaluation.FormatClasses(evaluation.GatingClasses)

	if got, err := evaluateOnceClassed(t, []sdk.Detector{healthy, drift}); err != nil || got.Status != evaluation.Firing {
		t.Fatalf("every class: %+v, %v; want firing (the control: the drift detector fires)", got, err)
	}
	if got, err := evaluateOnceClassed(t, []sdk.Detector{healthy, drift}, "--classes", gating); err != nil || got.Status != evaluation.Clear || len(got.Findings) != 0 {
		t.Fatalf("gating classes: %+v, %v; want clear: an incident detector must not hold an advance", got, err)
	}
	broken := classedDetector("workload-health", sdk.DetectorClassHealth, true)
	if got, err := evaluateOnceClassed(t, []sdk.Detector{broken, classedDetector("deployment-image-drift", sdk.DetectorClassIncident, false)}, "--classes", gating); err != nil || got.Status != evaluation.Firing {
		t.Fatalf("firing health detector: %+v, %v; want firing", got, err)
	}
	if _, err := evaluateOnceClassed(t, []sdk.Detector{healthy}, "--classes", "mystery"); err == nil || !strings.Contains(err.Error(), "mystery") {
		t.Fatalf("unknown class: %v", err)
	}
}

func TestEvaluateOnceRefusesUnreadableImagePins(t *testing.T) {
	healthy := classedDetector("workload-health", sdk.DetectorClassHealth, false)
	if _, err := evaluateOnceClassed(t, []sdk.Detector{healthy}, "--image-pins", filepath.Join(t.TempDir(), "absent.json")); err == nil || !strings.Contains(err.Error(), "image-pins") {
		t.Fatalf("absent pins file: %v", err)
	}
	bad := filepath.Join(t.TempDir(), "pins.json")
	if err := os.WriteFile(bad, []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := evaluateOnceClassed(t, []sdk.Detector{healthy}, "--image-pins", bad); err == nil || !strings.Contains(err.Error(), "image-pins") {
		t.Fatalf("malformed pins file: %v", err)
	}
}

// The gate leaves ordinary detection alone: after the advance, a real fault
// seen by a Kubernetes condition detector still opens an incident through the
// controller.
func TestConditionDetectorStillOpensAnIncidentThroughOrdinaryDetection(t *testing.T) {
	fault := classedDetector("workload-health", sdk.DetectorClassHealth, true)
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	controller, err := NewController(testControllerConfig(), []sdk.Detector{fault}, staticProvider{snapshot: sdktest.Snapshot{}}, dispatcher, time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	controller.now = func() time.Time { return time.Unix(1, 0).UTC() }
	for sample := 0; sample < 2; sample++ {
		if err := controller.Step(context.Background(), time.Unix(int64(sample), 0), nil); err != nil {
			t.Fatal(err)
		}
	}
	executePendingEffect(t, controller)
	request := awaitRequest(t, dispatcher.requests)
	if len(request.Findings) == 0 || request.Findings[0].DetectorID != "workload-health" {
		t.Fatalf("incident request = %+v, want the condition detector's finding", request)
	}
}
