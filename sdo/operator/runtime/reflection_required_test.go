package runtime

import (
	"context"
	"google.golang.org/protobuf/types/known/timestamppb"
	"os"
	"path/filepath"
	"testing"
	"time"

	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/runtime/lifecyclecore"
)

func TestReadmeOnlyVerifiedReflectionCommitsWithARecordedGap(t *testing.T) {
	root := nativeRepository(t)
	copyFastloopTree(t, "../memory/validate/testdata/good/.sdo", filepath.Join(root, ".sdo"))
	if err := os.Chmod(filepath.Join(root, ".sdo/playbooks/base/scripts/fix.sh"), 0755); err != nil {
		t.Fatal(err)
	}
	nativeGit(t, root, "add", "-f", ".sdo")
	nativeGit(t, root, "commit", "-qm", "accepted memory")
	repository, err := broker.New(root, t.TempDir(), func(context.Context, string, []string) error {
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	outcome := &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "first", Classification: "success", ConfirmedRootCauses: []*contracts.ConfirmedRootCause{{Summary: "routing fault", Resources: []*contracts.ObjectRef{t1Ref("api")}}}}
	// The publication obligation applies to an independently repaired opening
	// finding, not merely to a responder claim in an overall successful result.
	at := time.Unix(100, 0)
	outcome.Findings = []*contracts.Finding{{DetectorId: "routing", RuleId: "target-port", Status: "active", Severity: "critical", Summary: "synthetic routing fault", Evidence: "synthetic independent opening observation", PrimaryResource: t1Ref("api"), Fingerprint: "routing/api"}}
	outcome.RepairActions = []*contracts.RepairActionReceipt{{ActionId: "sdo-act-repair", Kind: "kubectl-patch", Target: "Service/app/api", Summary: "restore target port", Details: "synthetic controller receipt", Success: true, Resources: []*contracts.ObjectRef{t1Ref("api")}, StartedAt: timestamppb.New(at), CompletedAt: timestamppb.New(at.Add(time.Second))}}
	outcome.FinalHealthDetectorState = []*contracts.DetectorEvaluation{{DetectorId: "routing", Status: "clear", EvaluatedAt: timestamppb.New(at.Add(2 * time.Second))}}
	accepted, err := repository.RecordOutcome(context.Background(), "first", outcome)
	if err != nil {
		t.Fatal(err)
	}
	proposal, err := repository.CreateProposalForPhase(context.Background(), "first", broker.ReflectionPhase)
	if err != nil {
		t.Fatal(err)
	}
	readme := filepath.Join(proposal.Path, ".sdo/playbooks/base/README.md")
	data, err := os.ReadFile(readme)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(readme, append(data, []byte("\nAdditional response advice.\n")...), 0644); err != nil {
		t.Fatal(err)
	}
	state := &contracts.BrokerLedger{IncidentId: "first", OutcomeCommit: accepted.Commit, ReflectionPhase: contracts.ReflectionPhase_REFLECTION_PHASE_REVIEW, ReflectionMaxRetries: 2, ReflectionWorkspace: &contracts.ProposalWorkspace{CheckoutPath: proposal.Path, BaseCommit: proposal.BaseCommit}, ReflectionDecision: &contracts.ReflectionDecision{LearningDecision: "updated", PlaybookIds: []string{"base"}}, ReflectionPaths: []string{".sdo/playbooks/base/README.md"}, OutcomeDocument: outcome}
	_, effects := lifecyclecore.StepReflection(state, lifecyclecore.ReflectionEvent{Kind: lifecyclecore.ReflectionDrive})
	driver := lifecycleio.ReflectionDriver{Repository: repository, Now: func() time.Time { return time.Unix(1, 0) }}
	reviewed, err := driver.Execute(context.Background(), effects[0])
	if err != nil || reviewed.Kind != lifecyclecore.ReflectionReviewed {
		t.Fatalf("metadata review: %v %v", reviewed, err)
	}
	state, _ = lifecyclecore.StepReflection(state, reviewed)
	_, effects = lifecyclecore.StepReflection(state, lifecyclecore.ReflectionEvent{Kind: lifecyclecore.ReflectionDrive})
	event, err := driver.Execute(context.Background(), effects[0])
	if err != nil || event.Kind != lifecyclecore.ReflectionPublished || len(event.CoverageGaps) != 1 || event.CoverageGaps[0].Rule != "reflection.program-required" {
		t.Fatalf("README-only repair must commit with a recorded gap: %+v %v", event, err)
	}
	next, _ := lifecyclecore.StepReflection(state, event)
	if len(next.ReflectionCoverageGaps) != 1 || next.ReflectionCoverageGaps[0].Rule != "reflection.program-required" {
		t.Fatalf("gap not recorded in the ledger: %v", next)
	}
}
