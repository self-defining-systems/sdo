package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/servicehealth"
)

// A hand-written healthy objective/endpoints plus a red third detector
// reproduces partial seed acceptance without importing benchmark fixtures.
func TestSeedAcceptanceEvaluatesWholeBundle(t *testing.T) {
	for _, red := range []string{"", "service-reachability", "workload-health", "incident-extra"} {
		t.Run(red, func(t *testing.T) {
			var detectors []sdk.Detector
			for _, id := range []string{"health-objective", "service-endpoints", "service-reachability", "workload-health", "incident-extra"} {
				finding := sdk.Finding{}
				if id == red {
					finding = stateFinding(id)
					finding.DetectorID = id
				}
				d := controllerDetector(id, time.Second, finding)
				d.spec.Class = sdk.DetectorClassHealth
				d.spec.Owner = sdk.DetectorOwnerHealthJudge
				if id == "incident-extra" {
					d.spec.Class = sdk.DetectorClassIncident
					d.spec.Owner = sdk.DetectorOwnerResponder
					d.spec.OriginatingIncident = "fixture-incident"
				}
				d.spec.Persistence = sdk.PersistencePolicy{Firing: 1, Clearing: 1}
				d.spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityCritical}
				d.spec.OriginatingCommit = "seed"
				detectors = append(detectors, d)
			}
			var stdout bytes.Buffer
			err := RunWithOptions(context.Background(), detectors, RuntimeOptions{
				Args:   []string{"--evaluate-once", "--accept-seed", "--namespace", "demo"},
				Stdout: &stdout, Provider: staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "demo"}},
			})
			for _, detector := range detectors {
				if detector.(*sequenceDetector).calls != 1 {
					t.Fatalf("detector %s calls=%d: %v", detector.Spec().ID, detector.(*sequenceDetector).calls, err)
				}
			}
			if red == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), "seed acceptance") {
				t.Fatalf("red bundle accepted: %v", err)
			}
			var result struct {
				Findings []sdk.Finding `json:"findings"`
			}
			if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
			if len(result.Findings) != 1 || result.Findings[0].DetectorID != red {
				t.Fatalf("lost rejected finding: %s", stdout.String())
			}
		})
	}
}

func TestSeedAcceptanceRequiresReachabilityEvidence(t *testing.T) {
	d := servicehealth.NewReachabilityDetector(sdk.DetectorSpec{ID: "service-reachability", Interval: time.Second})
	err := RunWithOptions(context.Background(), []sdk.Detector{d}, RuntimeOptions{
		Args:     []string{"--evaluate-once", "--accept-seed", "--namespace", "demo"},
		Provider: staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "demo"}},
	})
	if err == nil || !strings.Contains(err.Error(), "requires --prober-url") {
		t.Fatalf("missing reachability accepted: %v", err)
	}
}
