package runtime

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func TestReflectionWorkRejectsUnverifiedDraftsBeforeAuthoring(t *testing.T) {
	at := time.Unix(100, 0)
	verified := &contracts.ObjectRef{ApiVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "web"}
	claimed := &contracts.ObjectRef{ApiVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "peer"}
	outcome := &contracts.OutcomeRecord{IncidentId: "shell-chain", Classification: "success", ConfirmedRootCauses: []*contracts.ConfirmedRootCause{{Summary: "verified repair", Resources: []*contracts.ObjectRef{verified}}, {Summary: "claimed repair without independent observation", Resources: []*contracts.ObjectRef{claimed}}}, Findings: []*contracts.Finding{{DetectorId: "workload", RuleId: "probe", Severity: "warn", Summary: "invalid probe", Evidence: "synthetic controller-observed failing probe", Status: "active", PrimaryResource: verified}}, FinalHealthDetectorState: []*contracts.DetectorEvaluation{{DetectorId: "workload", Status: "clear", EvaluatedAt: timestamppb.New(at.Add(3 * time.Second))}}}
	for i, ref := range []*contracts.ObjectRef{verified, claimed} {
		outcome.RepairActions = append(outcome.RepairActions, &contracts.RepairActionReceipt{ActionId: []string{"observed-web", "observed-peer"}[i], Success: true, Resources: []*contracts.ObjectRef{ref}, StartedAt: timestamppb.New(at), CompletedAt: timestamppb.New(at.Add(time.Second))})
	}
	work := (NativeIncidentBroker{}).prepareCauseWork(context.Background(), outcome, IncidentWorkspace{IncidentID: outcome.IncidentId, Worktree: t.TempDir(), BaseCommit: "source"})
	if len(work.Causes) != 1 || work.Causes[0].CauseIndex != 0 {
		t.Fatalf("unverified repair offered as executable drafting work: %+v", work)
	}
	if len(work.Unassigned) != 1 || work.Unassigned[0].Reason != "evidence_gap" || len(work.Unassigned[0].Resources) != 1 || work.Unassigned[0].Resources[0].Name != "peer" {
		t.Fatalf("missing early evidence gap for unverified cause: %+v", work)
	}
}
