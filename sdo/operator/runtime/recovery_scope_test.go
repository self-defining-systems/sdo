package runtime

import (
	"context"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

// Only the alerts the incident opened on hold its recovery proof, at any
// severity. Another finding, a clue or a later opener, is not a contradiction
// of this repair: it stays tracked and, if it opens, opens its own incident.
func TestRecoveryProofWaitsOnlyOnOpeningAlerts(t *testing.T) {
	for _, scenario := range []struct {
		name     string
		severity sdk.FindingSeverity
		own      bool
		closes   bool
	}{
		{name: "unrelated-warning", severity: sdk.SeverityWarning, closes: true},
		{name: "unrelated-critical", severity: sdk.SeverityCritical, closes: true},
		{name: "own-warning", severity: sdk.SeverityWarning, own: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			ctx := context.Background()
			now := time.Unix(1000, 0).UTC()
			health := func(id string, findings ...sdk.Finding) *sequenceDetector {
				d := controllerDetector(id, time.Second, findings...)
				d.spec.Class, d.spec.Owner = sdk.DetectorClassHealth, sdk.DetectorOwnerHealthJudge
				d.spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityWarning}
				d.spec.OriginatingCommit = "synthetic"
				d.spec.Persistence = sdk.PersistencePolicy{Firing: 1, Clearing: 1}
				return d
			}
			slow := sdk.Finding{
				DetectorID: "links", RuleID: "link-reachability.web.store.5432", Status: sdk.FindingActive, Severity: scenario.severity,
				Summary: "link web -> store:5432 is slow", Evidence: "synthetic dials",
				PrimaryResource: sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "demo", Name: "store"},
			}
			repaired := health("workloads", sdk.Finding{})
			links := health("links", slow)
			cfg := testControllerConfig()
			cfg.ClosureStabilityWindow = time.Second
			c, err := NewController(cfg, []sdk.Detector{repaired, links}, staticProvider{}, &recordingDispatcher{}, now)
			if err != nil {
				t.Fatal(err)
			}
			c.now = func() time.Time { return now }
			opened := []sdk.Finding{{
				DetectorID: "workloads", RuleID: "scaled-to-zero", Status: sdk.FindingActive, Severity: sdk.SeverityCritical,
				Summary: "web has no replicas", Evidence: "synthetic",
				PrimaryResource: sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "demo", Name: "web"},
			}}
			if scenario.own {
				opened = append(opened, slow)
			}
			for i := range opened {
				opened[i].Fingerprint = FindingFingerprint(opened[i])
			}
			c.incidentOpen, c.responderDone = true, true
			c.dispatchState = contracts.DispatchState_DISPATCH_STATE_COMPLETED
			c.currentIncidentRequest = &IncidentRequest{IncidentID: "synthetic", Application: "demo", Findings: opened, ResponseDeadline: now.Add(time.Minute)}
			c.incidentFindingKeys = findingKeys(opened)
			c.incidentDetectedAt, c.incidentDispatchedAt, c.responderCompletedAt = now.Add(-3*time.Second), now.Add(-2*time.Second), now.Add(-time.Second)
			c.verificationDeadline = now.Add(time.Minute)
			result := completedResult("synthetic")
			result.RepairActions = []RepairActionReceipt{{ActionID: "synthetic-scale", Kind: "kubectl-scale", Success: true}}
			c.currentIncidentResult = &result
			var closed *IncidentClosure
			c.OnIncidentClosed = func(closure IncidentClosure) { closed = &closure }
			for range 5 {
				if err := c.Step(ctx, now, nil); err != nil {
					t.Fatal(err)
				}
				now = now.Add(time.Second)
			}
			if !scenario.closes {
				if closed != nil && closed.Result.Status == IncidentCompleted {
					t.Fatalf("recovery verified while %s finding was active", scenario.name)
				}
				return
			}
			if closed == nil || closed.Result.Status != IncidentCompleted || len(closed.Result.UnresolvedFindings) > 0 {
				t.Fatalf("an unrelated slow link held the recovery proof: %+v", closed)
			}
			if !c.tracker.HasActiveKeys(findingKeys([]sdk.Finding{slow})) {
				t.Fatal("the unrelated finding stopped being tracked; it must still open its own incident")
			}
		})
	}
}
