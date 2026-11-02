package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	clocktesting "k8s.io/utils/clock/testing"
	"path/filepath"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"strings"
	"testing"
	"time"
)

func TestResolveRepositoryCommitsHonorsExplicitDeployedCommit(t *testing.T) {
	source, deployed, err := resolveRepositoryCommits(t.TempDir(), "source-sha", "deployed-sha")
	if err != nil {
		t.Fatalf("resolve explicit commits: %v", err)
	}
	if source != "source-sha" || deployed != "deployed-sha" {
		t.Fatalf("unexpected explicit commits source=%q deployed=%q", source, deployed)
	}
}

func TestEvaluateOnceEncodesClearFindingsAsArray(t *testing.T) {
	detector := controllerDetector("health", time.Second, sdk.Finding{})
	detector.spec.Class = sdk.DetectorClassHealth
	detector.spec.Owner = sdk.DetectorOwnerHealthJudge
	detector.spec.Persistence = sdk.PersistencePolicy{Firing: 1, Clearing: 1}
	detector.spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityCritical}
	detector.spec.OriginatingCommit = "health-objective"
	var stdout bytes.Buffer

	err := RunWithOptions(context.Background(), []sdk.Detector{detector}, RuntimeOptions{
		Args:     []string{"--evaluate-once", "--namespace", "demo"},
		Stdout:   &stdout,
		Provider: staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "demo"}},
	})
	if err != nil {
		t.Fatalf("evaluate once: %v", err)
	}
	if got := stdout.String(); got != "{\"findings\":[],\"status\":\"clear\"}\n" {
		t.Fatalf("clear evaluation must encode a JSON array, got %q", got)
	}
}

func TestEvaluateOnceRoutesFreshSnapshotThroughController(t *testing.T) {
	detector := controllerDetector("health", time.Second, stateFinding("health-objective"))
	detector.spec.Class = sdk.DetectorClassHealth
	detector.spec.Owner = sdk.DetectorOwnerHealthJudge
	detector.spec.Persistence = sdk.PersistencePolicy{Firing: 1, Clearing: 1}
	detector.spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityCritical}
	detector.spec.OriginatingCommit = "health-objective"
	var stdout bytes.Buffer

	err := RunWithOptions(context.Background(), []sdk.Detector{detector}, RuntimeOptions{
		Args:     []string{"--evaluate-once", "--namespace", "demo", "--application", "demo"},
		Stdout:   &stdout,
		Provider: staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "demo"}},
	})
	if err != nil {
		t.Fatalf("evaluate once: %v", err)
	}
	var output struct {
		Status   string        `json:"status"`
		Findings []sdk.Finding `json:"findings"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &output); err != nil {
		t.Fatalf("decode output %q: %v", stdout.String(), err)
	}
	if output.Status != "firing" || len(output.Findings) != 1 {
		t.Fatalf("unexpected controller evaluation: %#v", output)
	}
}

func TestEvaluateOnceFailsClosedOnDetectorError(t *testing.T) {
	detector := controllerDetector("health", time.Second, sdk.Finding{})
	detector.spec.Class = sdk.DetectorClassHealth
	detector.spec.Owner = sdk.DetectorOwnerHealthJudge
	detector.spec.Persistence = sdk.PersistencePolicy{Firing: 1, Clearing: 1}
	detector.spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityCritical}
	detector.spec.OriginatingCommit = "health-objective"
	detector.errors = map[int]error{0: context.DeadlineExceeded}

	err := RunWithOptions(context.Background(), []sdk.Detector{detector}, RuntimeOptions{
		Args:     []string{"--evaluate-once", "--namespace", "demo"},
		Provider: staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "demo"}},
	})
	if err == nil {
		t.Fatal("detector error was treated as a clear health verdict")
	}
}

func TestStopTimerForRuntimeEventDetectsExpiredRuntime(t *testing.T) {
	clock := clocktesting.NewFakeClock(time.Unix(123, 0))
	activeTimer := clock.NewTimer(time.Hour)
	defer activeTimer.Stop()
	if stopTimerForRuntimeEvent(context.Background(), activeTimer) {
		t.Fatal("active runtime was treated as expired")
	}

	expired, cancel := context.WithCancel(context.Background())
	cancel()
	expiredTimer := clock.NewTimer(time.Hour)
	defer expiredTimer.Stop()
	if !stopTimerForRuntimeEvent(expired, expiredTimer) {
		t.Fatal("expired runtime was allowed to process another event")
	}
}

func TestParseResponderEnvironmentRejectsReservedAndMalformedValues(t *testing.T) {
	values, err := parseResponderEnvironment([]string{"SDO_SREGYM_API_BASE=http://host:8123", "Z=value=with=equals"})
	if err != nil {
		t.Fatalf("parse responder environment: %v", err)
	}
	if values["Z"] != "value=with=equals" {
		t.Fatalf("value was truncated: %#v", values)
	}
	for _, invalid := range [][]string{{"MALFORMED"}, {"SDO_NAMESPACE=other"}, {"bad-name=value"}} {
		if _, err := parseResponderEnvironment(invalid); err == nil {
			t.Fatalf("accepted invalid responder environment %#v", invalid)
		}
	}
}

func TestRunRejectsRemovedRestartFlag(t *testing.T) {
	err := RunWithOptions(context.Background(), nil, RuntimeOptions{Args: []string{
		"--namespace", "demo", "--app-root", t.TempDir(),
		"--exit-after-closure", "--restart-after-closure",
	}, Stderr: &bytes.Buffer{}})
	if err == nil || !strings.Contains(err.Error(), "flag provided but not defined") {
		t.Fatalf("expected mutually exclusive closure modes, got %v", err)
	}
}

func TestRunRejectsInvalidControlNamespace(t *testing.T) {
	err := RunWithOptions(context.Background(), nil, RuntimeOptions{Args: []string{
		"--namespace", "demo", "--control-namespace", "Not_Valid", "--app-root", t.TempDir(),
	}, Stderr: &bytes.Buffer{}})
	if err == nil || !strings.Contains(err.Error(), "invalid control namespace") {
		t.Fatalf("expected invalid control namespace error, got %v", err)
	}
}

func TestAttachFiringTelemetryDefaultsByDispatcherMode(t *testing.T) {
	mount := t.TempDir()
	controller := telemetryController(t, nil, &recordingDispatcher{requests: make(chan IncidentRequest, 1)},
		incidentDetector("cause", stateFinding("cause")))
	if err := attachFiringTelemetry(controller, "", false, mount); err != nil || controller.firingSink != nil {
		t.Fatalf("local mode must not enable telemetry by default: %v", err)
	}
	if err := attachFiringTelemetry(controller, "off", true, mount); err != nil || controller.firingSink != nil {
		t.Fatalf("off must disable telemetry: %v", err)
	}
	if err := attachFiringTelemetry(controller, "", true, mount); err != nil || controller.firingSink == nil {
		t.Fatalf("job mode must enable telemetry: %v", err)
	}
	step(t, controller, 0)
	step(t, controller, 1)
	path := filepath.Join(mount, ".sdo-runtime", "telemetry", "detector-firings.jsonl")
	if len(readStream(t, path)) == 0 {
		t.Fatal("job-mode telemetry was not written under the repository mount")
	}
}

func TestResponderStandbyColdOnlyConfiguration(t *testing.T) {
	if standbyEnabled(map[string]string{"SDO_RESPONDER_STANDBY": "0"}) {
		t.Fatal("cold-only configuration still provisions standby")
	}
	if !standbyEnabled(nil) {
		t.Fatal("default standby disabled")
	}
}
