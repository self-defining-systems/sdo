package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/traffic"
)

func recoveryTrafficSnapshot(t *testing.T, now time.Time, latency time.Duration) sdktest.Snapshot {
	t.Helper()
	workload, err := traffic.ParseWorkloadJSON([]byte(`{"apiVersion":"sdo.dev/v1alpha1","kind":"TrafficWorkload","name":"health","purpose":"health-probe","scenarios":[{"id":"read","slo":{"latencyPercentile":50,"maxLatency":"250ms"}}]}`))
	if err != nil {
		t.Fatal(err)
	}
	samples := make([]traffic.Sample, 5)
	for i := range samples {
		samples[i] = traffic.Sample{At: now.Add(time.Duration(i-4) * time.Second), Outcome: traffic.OutcomeOK, Status: 200, Latency: latency}
	}
	return sdktest.Snapshot{NamespaceName: "app", Traffic: map[string]traffic.Window{"health": {Workload: workload, ObservedAt: now, Scenarios: []traffic.ScenarioObservations{{Scenario: traffic.Descriptor{ID: "read", Target: traffic.Target{Service: "api", Port: 80}}, Qualified: true, Samples: samples}}}}}
}

// Every health ID and non-clear status shares the same fresh-observation rule;
// no traffic-name exception, reflection delay, or scheduled old green is proof.
func TestRecoveryFreshHealthSweepBeforeEarlyClosure(t *testing.T) {
	for _, id := range []string{"traffic-health", "health-objective", "other-health"} {
		for _, status := range []string{"clear", "active", "pending", "error"} {
			t.Run(id+"/"+status, func(t *testing.T) {
				c, _, step := earlyWaitController(t, stateFinding("health"), stateFinding("health"), sdk.Finding{})
				peer := &namedRecoveryDetector{windowDetector: &windowDetector{confirmed: true}, id: id}
				c.detectors[id] = peer
				c.healthDetectorIDs = append(c.healthDetectorIDs, id)
				c.scheduler = NewScheduler(c.detectorList(), time.Unix(2, 0))
				for i := 2; i < 17; i++ {
					step(i)
				}
				switch status {
				case "active":
					peer.set(true, true)
				case "pending":
					peer.set(false, false)
				case "error":
					peer.err = errors.New("synthetic observer unavailable")
				}
				step(17)
				if c.IncidentOpen() != (status != "clear") {
					t.Fatalf("early proof ignored fresh %s on %s", status, id)
				}
				if status != "clear" {
					if _, ok := c.PendingIncidentClosure(); ok {
						t.Fatal("early/model-free reflection path received a false recovery proof")
					}
				}
			})
		}
	}
}

type namedRecoveryDetector struct {
	*windowDetector
	id string
}

func (d *namedRecoveryDetector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: d.id, Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge, Interval: time.Hour, Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1}, Batching: sdk.BatchingPolicy{Severity: sdk.SeverityWarning}, OriginatingCommit: "synthetic"}
}

func (d *namedRecoveryDetector) Detect(ctx context.Context, s sdk.DetectionContext) ([]sdk.Finding, error) {
	findings, err := d.windowDetector.Detect(ctx, s)
	for i := range findings {
		findings[i].DetectorID = d.id
	}
	return findings, err
}

// A detector may retain its result slice. Controller normalization must not
// mutate the producer's evidence or race a concurrent read-only consumer.
func TestRecoveryObservationRetainsDetectorOwnedEvidence(t *testing.T) {
	finding := stateFinding("health")
	finding.DetectorID = ""
	finding.Fingerprint = ""
	detector := &retainedRecoveryDetector{namedRecoveryDetector: &namedRecoveryDetector{windowDetector: &windowDetector{confirmed: true}, id: "health"}, findings: []sdk.Finding{finding}}
	c, err := NewController(testControllerConfig(), []sdk.Detector{detector}, staticProvider{snapshot: sdktest.Snapshot{}}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Step(context.Background(), time.Unix(0, 0), nil); err != nil {
		t.Fatal(err)
	}
	if detector.findings[0].DetectorID != "" || detector.findings[0].Fingerprint != "" {
		t.Fatal("normalization mutated the detector's retained evidence")
	}
}

type retainedRecoveryDetector struct {
	*namedRecoveryDetector
	findings []sdk.Finding
}

func (d *retainedRecoveryDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	return d.findings, nil
}

