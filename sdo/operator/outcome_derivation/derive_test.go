package outcome_derivation

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	outcomeclass "sdo.dev/operator/contracts/outcome"
)

func outOfReachClosure(reach string, healthy bool) *contracts.IncidentClosure {
	at := func(seconds int) *timestamppb.Timestamp {
		return timestamppb.New(time.Date(2026, 11, 10, 12, 0, seconds, 0, time.UTC))
	}
	store := &contracts.ObjectRef{ApiVersion: "v1", Kind: "Service", Namespace: "app", Name: "store"}
	closure := &contracts.IncidentClosure{
		Request: &contracts.IncidentRequest{IncidentId: "incident-1", Findings: []*contracts.Finding{{
			DetectorId: "traffic-links", RuleId: "link-reachability.web.store.5432", Status: "active", Severity: "critical",
			Summary: "link web -> store:5432 is slow", Evidence: "p50 2s", PrimaryResource: store,
		}}},
		Result: &contracts.IncidentResult{IncidentId: "incident-1", Status: "completed", ConfirmedRootCauses: []*contracts.ConfirmedRootCause{{
			Summary: "delay on the network path to store", Resources: []*contracts.ObjectRef{store}, CauseClass: "network-delay", Reach: reach,
			Evidence: []*contracts.RootCauseEvidence{{Kind: "detector-finding", Source: "traffic-links", Observation: "p50 2s against 5ms healthy"}},
		}}},
		DetectedAt: at(0), DispatchedAt: at(1), ResponderCompletedAt: at(2),
	}
	if healthy {
		closure.FinalDetectorStates = []*contracts.DetectorEvaluation{{DetectorId: "traffic-links", Status: "clear", EvaluatedAt: at(3)}}
		closure.VerifiedAt = at(3)
	}
	return closure
}

// SDO never escalates: no closure derives the historical escalated class,
// whatever the agent reported, and a dispatch failure stays failed.
func TestNoClosureDerivesEscalated(t *testing.T) {
	for name, mutate := range map[string]func(*contracts.IncidentClosure){
		"agent reported completed": func(*contracts.IncidentClosure) {},
		"agent reported failed":    func(c *contracts.IncidentClosure) { c.Result.Status = "failed" },
		"dispatch failed": func(c *contracts.IncidentClosure) {
			dispatchError := "responder Job failed"
			c.DispatchError = &dispatchError
		},
	} {
		t.Run(name, func(t *testing.T) {
			closure := outOfReachClosure(outcomeclass.ReachOutOfReach, false)
			mutate(closure)
			derived, err := Derive(context.Background(), closure, "", "codex", "model", true)
			if err != nil {
				t.Fatal(err)
			}
			if got := derived.Record.GetClassification(); got != outcomeclass.Failed {
				t.Fatalf("classification = %q, want %q", got, outcomeclass.Failed)
			}
		})
	}
}

// An unrecovered closure is a failure, even with an out-of-reach cause, and
// an out-of-reach cause never overrides verified health.
func TestOutOfReachNeverOverridesVerifiedHealth(t *testing.T) {
	for name, tc := range map[string]struct {
		reach   string
		healthy bool
		want    string
	}{
		"in scope, not recovered":     {outcomeclass.ReachInScope, false, outcomeclass.Failed},
		"out of reach, not recovered": {outcomeclass.ReachOutOfReach, false, outcomeclass.Failed},
		"out of reach, recovered":     {outcomeclass.ReachOutOfReach, true, outcomeclass.ClearedWithoutAction},
	} {
		t.Run(name, func(t *testing.T) {
			derived, err := Derive(context.Background(), outOfReachClosure(tc.reach, tc.healthy), "", "codex", "model", true)
			if err != nil {
				t.Fatal(err)
			}
			if got := derived.Record.GetClassification(); got != tc.want {
				t.Fatalf("classification = %q, want %q", got, tc.want)
			}
		})
	}
}

// The agent's findings are stored verbatim whatever its status; they never change the class.
func TestAgentFindingsAreCopiedIntoTheOutcomeForAnyStatus(t *testing.T) {
	closure := outOfReachClosure(outcomeclass.ReachOutOfReach, false)
	closure.Result.Status = "failed"
	closure.Result.Findings = []*contracts.IncidentFinding{{Kind: "ruled_out", Statement: "cache", SeenIn: "keys", Verdict: "refuted"}}
	derived, err := Derive(context.Background(), closure, "", "codex", "model", true)
	if err != nil {
		t.Fatal(err)
	}
	got := derived.Record.GetAgentFindings()
	if len(got) != 1 || got[0].GetStatement() != "cache" || derived.Record.GetClassification() != outcomeclass.Failed {
		t.Fatalf("findings = %v class = %q", got, derived.Record.GetClassification())
	}
}

// With the recording off, the agent's reported changes stand in for its unknown writes: success needs one that took
// effect after detection and before the alerts cleared. With the recording on they are ignored.
func TestAgentReportedChangesDecideSuccessOnlyWithTheRecordingOff(t *testing.T) {
	at := func(seconds int) *timestamppb.Timestamp {
		return timestamppb.New(time.Date(2026, 11, 10, 12, 0, seconds, 0, time.UTC))
	}
	for name, tc := range map[string]struct {
		changes  []*contracts.AgentChange
		recorded bool
		want     string
	}{
		"change before the clear":       {changes: []*contracts.AgentChange{{Summary: "x", Target: "y", At: at(2)}}, want: outcomeclass.Success},
		"no change":                     {want: outcomeclass.ClearedWithoutAction},
		"change after the clear":        {changes: []*contracts.AgentChange{{Summary: "x", Target: "y", At: at(9)}}, want: outcomeclass.ClearedWithoutAction},
		"change before the detection":   {changes: []*contracts.AgentChange{{Summary: "x", Target: "y", At: at(-5)}}, want: outcomeclass.ClearedWithoutAction},
		"recording on ignores reported": {changes: []*contracts.AgentChange{{Summary: "x", Target: "y", At: at(2)}}, recorded: true, want: outcomeclass.ClearedWithoutAction},
	} {
		t.Run(name, func(t *testing.T) {
			closure := outOfReachClosure(outcomeclass.ReachOutOfReach, true)
			closure.HealthClearedAt = at(3)
			closure.Result.AgentChanges = tc.changes
			derived, err := Derive(context.Background(), closure, "", "codex", "model", tc.recorded)
			if err != nil {
				t.Fatal(err)
			}
			if got := derived.Record.GetClassification(); got != tc.want {
				t.Fatalf("classification = %q, want %q", got, tc.want)
			}
			if len(derived.Record.GetAgentChanges()) != len(tc.changes) {
				t.Fatalf("outcome agent_changes = %v", derived.Record.GetAgentChanges())
			}
		})
	}
}
