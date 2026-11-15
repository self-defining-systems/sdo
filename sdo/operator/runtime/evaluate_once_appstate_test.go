package runtime

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"

	"sdo.dev/operator/evaluation"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/appstate/carried"
)

// requiringReader is a reader whose detection needs its own carried state, as
// a designer-authored reader detector does.
type requiringReader struct{ fakeReader }

func (r requiringReader) Spec() sdk.DetectorSpec {
	spec := r.fakeReader.Spec()
	spec.Owner = sdk.DetectorOwnerHealthJudge
	spec.Persistence = sdk.PersistencePolicy{Firing: 1, Clearing: 1}
	spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityWarning, Debounce: time.Second}
	spec.OriginatingCommit = "synthetic"
	spec.Playbooks = []string{"synthetic"}
	return spec
}

func (r requiringReader) Detect(ctx context.Context, snapshot sdk.DetectionContext) ([]sdk.Finding, error) {
	_, err := carried.Require(snapshot, r.id)
	return nil, err
}

// One-shot evaluation runs the app-state readers before judging, so a reader
// detector is evaluated against its own first read instead of failing the
// whole evaluation as unavailable.
func TestEvaluateOnceReadsAppStateBeforeJudging(t *testing.T) {
	reader := requiringReader{fakeReader{id: "settings", interval: 30 * time.Second, read: func(context.Context) (carried.Values, error) {
		return valuesOf("maintenance_mode", "false"), nil
	}}}
	var stdout, stderr bytes.Buffer
	err := RunWithOptions(context.Background(), []sdk.Detector{reader}, RuntimeOptions{
		Args: []string{"--evaluate-once", "--namespace", "shop"}, Provider: staticProvider{snapshot: syntheticShopSnapshot()}, Client: fake.NewSimpleClientset(),
		Stdout: &stdout, Stderr: &stderr,
	})
	if err != nil {
		t.Fatalf("evaluation failed: %v (stderr %q)", err, stderr.String())
	}
	verdict, err := evaluation.Decode(stdout.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if verdict.Status != evaluation.Clear {
		t.Fatalf("status %s: %+v", verdict.Status, verdict)
	}
}

// --watch keeps judging after the warm-up and prints one verdict per line; a
// detector that errors is that detector's unavailable entry while the others
// are still judged, and each line says when its judging began.
func TestEvaluateOnceWatchJudgesRepeatedlyAndKeepsOtherDetectorsOnError(t *testing.T) {
	broken := requiringReader{fakeReader{id: "settings", interval: 30 * time.Second, read: func(context.Context) (carried.Values, error) {
		return carried.Values{}, errors.New("store unreachable")
	}}}
	firing := classedDetector("workload-health", sdk.DetectorClassHealth, true)
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	var stdout, stderr bytes.Buffer
	err := RunWithOptions(ctx, []sdk.Detector{broken, firing}, RuntimeOptions{
		Args: []string{"--evaluate-once", "--namespace", "shop", "--watch", "100ms"}, Provider: staticProvider{snapshot: syntheticShopSnapshot()}, Client: fake.NewSimpleClientset(),
		Stdout: &stdout, Stderr: &stderr,
	})
	if err != nil {
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	if len(lines) < 2 {
		t.Fatalf("expected repeated verdicts, got %d: %q", len(lines), stdout.String())
	}
	var previous time.Time
	for _, line := range lines {
		verdict, err := evaluation.Decode([]byte(line))
		if err != nil {
			t.Fatalf("decode %q: %v", line, err)
		}
		if verdict.Status != evaluation.Firing || len(verdict.Findings) == 0 {
			t.Fatalf("the healthy detector's finding must survive the other's error: %+v", verdict)
		}
		if len(verdict.Unavailable) != 1 || verdict.Unavailable[0].DetectorID != "settings" {
			t.Fatalf("the erroring detector must be unavailable by name: %+v (whole %+v)", verdict.Unavailable, verdict)
		}
		if !verdict.StartedAt.After(previous) {
			t.Fatalf("started_at must advance: %s after %s", verdict.StartedAt, previous)
		}
		previous = verdict.StartedAt
	}
}

type dialingDetector struct{ *sequenceDetector }

func (dialingDetector) ConsumesServiceReachability() bool { return true }

// A run outside the cluster cannot resolve Service names, so detectors that
// dial them are left out and named, instead of raising on every Service.
func TestEvaluateOnceOutsideClusterLeavesOutDetectorsThatDialServiceNames(t *testing.T) {
	dialing := dialingDetector{classedDetector("service-reachability", sdk.DetectorClassHealth, true)}
	healthy := classedDetector("workload-health", sdk.DetectorClassHealth, false)
	verdict, err := evaluateOnceClassed(t, []sdk.Detector{dialing, healthy}, "--outside-cluster")
	if err != nil {
		t.Fatal(err)
	}
	if len(verdict.Findings) != 0 {
		t.Fatalf("the dialing detector must not run: %+v", verdict.Findings)
	}
	if len(verdict.Unavailable) != 1 || verdict.Unavailable[0].DetectorID != "service-reachability" || !strings.HasPrefix(verdict.Unavailable[0].Reason, evaluation.OutsideClusterReason) {
		t.Fatalf("unavailable: %+v", verdict.Unavailable)
	}
}
