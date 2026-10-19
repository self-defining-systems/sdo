package traffic_test

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/traffic"
)

var observedAt = time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)

func detectorSpec() sdk.DetectorSpec {
	return sdk.DetectorSpec{
		ID: "traffic-health", Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge,
		Watches: []sdk.WatchKind{traffic.Watch}, Interval: 10 * time.Second,
		Persistence: sdk.PersistencePolicy{Firing: 2, Clearing: 2},
		Batching:    sdk.BatchingPolicy{Severity: sdk.SeverityCritical, Debounce: 500 * time.Millisecond},
		Playbooks:   []string{"health-objective"}, OriginatingCommit: "lifecycle-bootstrap",
	}
}

func healthWorkload(t *testing.T) traffic.Workload {
	t.Helper()
	workload, err := traffic.ParseWorkloadJSON([]byte(`{
	  "apiVersion": "sdo.dev/v1alpha1", "kind": "TrafficWorkload", "name": "health", "purpose": "health-probe",
	  "scenarios": [{"id": "search", "weight": 2}, {"id": "login"}, {"id": "reserve"}]
	}`))
	if err != nil {
		t.Fatalf("parse workload: %v", err)
	}
	return workload
}

func descriptor(id string, dependsOn ...string) traffic.Descriptor {
	return traffic.Descriptor{
		ID: id, Target: traffic.Target{Service: "frontend", Port: 5000, Scheme: "http"}, DependsOn: dependsOn,
		SideEffect: traffic.SideEffectRead, Steps: []string{id},
		Detects: []traffic.FaultClass{traffic.FaultUnreachable, traffic.FaultErrorStatus},
	}
}

func ok(offset time.Duration) traffic.Sample {
	return traffic.Sample{At: observedAt.Add(-offset), Latency: 3 * time.Millisecond, Status: 200, Outcome: traffic.OutcomeOK, Requests: 1}
}

func failed(offset time.Duration, status int, message string) traffic.Sample {
	return traffic.Sample{
		At: observedAt.Add(-offset), Latency: 2 * time.Millisecond, Status: status, Outcome: traffic.OutcomeError,
		Step: "search", Request: "GET /hotels?inDate=2015-04-09", Error: message, Iteration: 42, Requests: 1,
	}
}

func timedOut(offset time.Duration) traffic.Sample {
	return traffic.Sample{At: observedAt.Add(-offset), Latency: 2 * time.Second, Outcome: traffic.OutcomeTimeout,
		Error: "context deadline exceeded", Requests: 1}
}

func samples(values ...traffic.Sample) []traffic.Sample { return values }

func observed(scenario traffic.Descriptor, values ...traffic.Sample) traffic.ScenarioObservations {
	return traffic.ScenarioObservations{Scenario: scenario, Qualified: true, Samples: values}
}

func healthy(id string) traffic.ScenarioObservations {
	return observed(descriptor(id), ok(5*time.Second), ok(4*time.Second), ok(3*time.Second), ok(2*time.Second), ok(time.Second))
}

// hotelSnapshot is the healthy hotel-reservation namespace with SREGym's
// never-used "revoke admin" decoy ConfigMaps present.
func hotelSnapshot(windows map[string]traffic.Window) sdktest.Snapshot {
	decoy := func(name string) corev1.ConfigMap {
		return corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "hotel-reservation"},
			Data:       map[string]string{"revoke-admin-mongo.sh": "mongo admin --eval 'db.revokeRolesFromUser(\"admin\", [\"readWrite\"])'"},
		}
	}
	return sdktest.Snapshot{
		NamespaceName: "hotel-reservation",
		ConfigMapList: []corev1.ConfigMap{decoy("failure-admin-geo"), decoy("failure-admin-rate")},
		Traffic:       windows,
	}
}

func window(workload traffic.Workload, scenarios ...traffic.ScenarioObservations) map[string]traffic.Window {
	return map[string]traffic.Window{workload.Name: {Workload: workload, ObservedAt: observedAt, Scenarios: scenarios}}
}

func detect(t *testing.T, snapshot sdktest.Snapshot) []sdk.Finding {
	t.Helper()
	findings, err := traffic.NewDetector(detectorSpec(), "health").Detect(context.Background(), snapshot)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	return findings
}

func TestDetectorDeclaresItsWorkload(t *testing.T) {
	detector := traffic.NewDetector(detectorSpec(), "health")
	consumer, ok := detector.(traffic.Consumer)
	if !ok || len(consumer.TrafficWorkloads()) != 1 || consumer.TrafficWorkloads()[0] != "health" {
		t.Fatalf("detector must declare the workload it evaluates, got %#v", detector)
	}
	if detector.Spec().ID != "traffic-health" {
		t.Fatalf("detector must keep the registered spec, got %+v", detector.Spec())
	}
}

func TestDetectorDefaultsHealthMinDurationWhenUnset(t *testing.T) {
	spec := detectorSpec()
	spec.Persistence.MinDuration = 0
	detector := traffic.NewDetector(spec, "health")
	if got := detector.Spec().Persistence.MinDuration; got != traffic.DefaultHealthMinDuration {
		t.Fatalf("health detector must default MinDuration, got %s want %s", got, traffic.DefaultHealthMinDuration)
	}
}

func TestDetectorKeepsAnExplicitMinDuration(t *testing.T) {
	spec := detectorSpec()
	spec.Persistence.MinDuration = 3 * time.Second
	detector := traffic.NewDetector(spec, "health")
	if got := detector.Spec().Persistence.MinDuration; got != 3*time.Second {
		t.Fatalf("explicit MinDuration must not be overridden, got %s", got)
	}
}

func TestDetectorLeavesNonHealthMinDurationUnset(t *testing.T) {
	spec := detectorSpec()
	spec.Class = sdk.DetectorClassIncident
	spec.Owner = sdk.DetectorOwnerResponder
	spec.Persistence.MinDuration = 0
	detector := traffic.NewDetector(spec, "health")
	if got := detector.Spec().Persistence.MinDuration; got != 0 {
		t.Fatalf("an incident detector must not get the health default, got %s", got)
	}
}

func TestDetectorIsSilentWithoutProbeObservations(t *testing.T) {
	if findings := detect(t, hotelSnapshot(nil)); len(findings) != 0 {
		t.Fatalf("no observations must mean no judgement, got %v", findings)
	}
}

func TestDetectorDoesNotFireOnHealthyTrafficWithDecoysPresent(t *testing.T) {
	snapshot := hotelSnapshot(window(healthWorkload(t), healthy("search"), healthy("login"), healthy("reserve")))
	if findings := detect(t, snapshot); len(findings) != 0 {
		t.Fatalf("healthy scenarios must not fire even with decoy ConfigMaps present, got %v", findings)
	}
}

func TestDetectorToleratesASingleBlip(t *testing.T) {
	blip := observed(descriptor("search"),
		ok(5*time.Second), ok(4*time.Second), failed(3*time.Second, 503, "upstream reset"), ok(2*time.Second), ok(time.Second))
	snapshot := hotelSnapshot(window(healthWorkload(t), blip, healthy("login"), healthy("reserve")))
	if findings := detect(t, snapshot); len(findings) != 0 {
		t.Fatalf("one failed iteration in five must not violate the default SLO, got %v", findings)
	}
}

// A Service whose selector matches no pod refuses every connection: every
// scenario through it fails and is named with its evidence and request path.
func TestDetectorLocalizesEveryFailingScenarioForAWrongServiceSelector(t *testing.T) {
	refused := func(id string, dependsOn ...string) traffic.ScenarioObservations {
		return observed(descriptor(id, dependsOn...), ok(6*time.Second), ok(5*time.Second),
			failed(3*time.Second, 0, "dial tcp 10.96.10.10:5000: connect: connection refused"),
			failed(2*time.Second, 0, "dial tcp 10.96.10.10:5000: connect: connection refused"),
			failed(time.Second, 0, "dial tcp 10.96.10.10:5000: connect: connection refused"))
	}
	snapshot := hotelSnapshot(window(healthWorkload(t),
		refused("search", "search", "geo", "rate", "profile"), refused("login", "user"), refused("reserve", "reservation")))
	findings := detect(t, snapshot)
	if len(findings) != 3 {
		t.Fatalf("expected one finding per failing scenario, got %d: %v", len(findings), findings)
	}
	seen := map[string]sdk.Finding{}
	for _, finding := range findings {
		seen[finding.RuleID] = finding
		if finding.DetectorID != "traffic-health" || finding.Status != sdk.FindingActive || finding.Severity != sdk.SeverityCritical {
			t.Fatalf("finding identity: %+v", finding)
		}
		if finding.PrimaryResource != (sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "hotel-reservation", Name: "frontend"}) {
			t.Fatalf("finding must point at the scenario's target Service, got %+v", finding.PrimaryResource)
		}
		for _, want := range []string{"connection refused", "iteration 42", "GET /hotels", "seed"} {
			if !strings.Contains(finding.Evidence, want) {
				t.Fatalf("finding evidence lacks %q: %q", want, finding.Evidence)
			}
		}
		if !strings.Contains(finding.Summary, "3/5") || len(finding.Playbooks) != 1 {
			t.Fatalf("finding must name the violation and playbooks, got %+v", finding)
		}
	}
	search := seen["scenario-slo.search"]
	if len(search.RelatedResources) != 4 || search.RelatedResources[1].Name != "geo" {
		t.Fatalf("dependencies must be related resources for localization, got %+v", search.RelatedResources)
	}
	if !strings.Contains(search.Evidence, "frontend → search → geo → rate → profile") {
		t.Fatalf("evidence must show the request path, got %q", search.Evidence)
	}
	for _, rule := range []string{"scenario-slo.search", "scenario-slo.login", "scenario-slo.reserve"} {
		if _, ok := seen[rule]; !ok {
			t.Fatalf("missing finding %s", rule)
		}
	}
}

func TestDetectorReportsStatusCodesAndErrorBodies(t *testing.T) {
	broken := observed(descriptor("login"),
		failed(3*time.Second, 500, "HTTP 500: rpc error: code = Unavailable desc = connection error"),
		failed(2*time.Second, 500, "HTTP 500: rpc error: code = Unavailable desc = connection error"),
		failed(time.Second, 200, "HTTP 200 body lacks \"Login successfully!\""))
	snapshot := hotelSnapshot(window(healthWorkload(t), healthy("search"), broken, healthy("reserve")))
	findings := detect(t, snapshot)
	if len(findings) != 1 {
		t.Fatalf("expected the login finding only, got %v", findings)
	}
	finding := findings[0]
	for _, want := range []string{"HTTP 500", "rpc error: code = Unavailable", "Login successfully!"} {
		if !strings.Contains(finding.Summary+" "+finding.Evidence, want) {
			t.Fatalf("finding lacks %q: summary %q evidence %q", want, finding.Summary, finding.Evidence)
		}
	}
	counts, ok := finding.Metadata["status_counts"].(map[string]int)
	if !ok || counts["500"] != 2 || counts["200"] != 1 {
		t.Fatalf("finding metadata must count status codes, got %#v", finding.Metadata["status_counts"])
	}
}

func TestDetectorFiresOnTimeoutsAndLatency(t *testing.T) {
	hanging := observed(descriptor("search"), ok(4*time.Second), timedOut(3*time.Second), timedOut(2*time.Second), timedOut(time.Second))
	slow := observed(descriptor("login"),
		traffic.Sample{At: observedAt.Add(-3 * time.Second), Latency: 1900 * time.Millisecond, Status: 200, Outcome: traffic.OutcomeOK},
		traffic.Sample{At: observedAt.Add(-2 * time.Second), Latency: 1800 * time.Millisecond, Status: 200, Outcome: traffic.OutcomeOK},
		traffic.Sample{At: observedAt.Add(-time.Second), Latency: 1700 * time.Millisecond, Status: 200, Outcome: traffic.OutcomeOK})
	findings := detect(t, hotelSnapshot(window(healthWorkload(t), hanging, slow, healthy("reserve"))))
	if len(findings) != 2 {
		t.Fatalf("expected timeout and latency findings, got %v", findings)
	}
	joined := findings[0].Summary + findings[1].Summary
	if !strings.Contains(joined, "timeout rate") || !strings.Contains(joined, "p90 latency") {
		t.Fatalf("findings must name the violated SLO dimension, got %q", joined)
	}
}

func TestDetectorIgnoresStaleInvalidAndUnqualifiedSamples(t *testing.T) {
	stale := observed(descriptor("search"),
		failed(5*time.Minute, 503, "old outage"), failed(4*time.Minute, 503, "old outage"), failed(3*time.Minute, 503, "old outage"),
		ok(2*time.Second), ok(time.Second))
	invalid := traffic.Sample{At: observedAt.Add(-time.Second), Outcome: traffic.OutcomeInvalid, Error: "build: FromPrevious"}
	generatorBug := observed(descriptor("reserve"), invalid, invalid, invalid, ok(time.Second))
	// A scenario that never succeeded since observation began is not a
	// qualified health signal: its generator may not match this deployment.
	neverWorked := traffic.ScenarioObservations{Scenario: descriptor("login"), Samples: samples(
		failed(3*time.Second, 404, "not found"), failed(2*time.Second, 404, "not found"), failed(time.Second, 404, "not found"))}
	if findings := detect(t, hotelSnapshot(window(healthWorkload(t), stale, neverWorked, generatorBug))); len(findings) != 0 {
		t.Fatalf("stale, invalid, and unqualified samples must not fire, got %v", findings)
	}
}

func TestDetectorClearsOnceRecentIterationsRecover(t *testing.T) {
	recovered := observed(descriptor("search"),
		failed(6*time.Second, 0, "connection refused"), failed(5*time.Second, 0, "connection refused"),
		ok(3*time.Second), ok(2*time.Second), ok(time.Second))
	if findings := detect(t, hotelSnapshot(window(healthWorkload(t), recovered, healthy("login"), healthy("reserve")))); len(findings) != 0 {
		t.Fatalf("three recovered iterations of five must clear the scenario, got %v", findings)
	}
}

func TestDetectorReportsAWorkloadTheProberCouldNotRun(t *testing.T) {
	snapshot := hotelSnapshot(map[string]traffic.Window{"health": {LoadError: "prober unreachable"}})
	_, err := traffic.NewDetector(detectorSpec(), "health").Detect(context.Background(), snapshot)
	if err == nil || !strings.Contains(err.Error(), "prober unreachable") {
		t.Fatalf("a load error must surface as a detector error, got %v", err)
	}
}
