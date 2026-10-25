package runtime

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/runtime/lifecyclecore"
)

func TestReadmeOnlyVerifiedReflectionIsRejectedAndRetried(t *testing.T) {
	root := nativeRepository(t)
	copyFastloopTree(t, "../memory/validate/testdata/good/.sdo", filepath.Join(root, ".sdo"))
	if err := os.Chmod(filepath.Join(root, ".sdo/playbooks/base/scripts/fix.sh"), 0755); err != nil {
		t.Fatal(err)
	}
	nativeGit(t, root, "add", "-f", ".sdo")
	nativeGit(t, root, "commit", "-qm", "accepted memory")
	repository, err := broker.New(root, t.TempDir(), func(context.Context, string, []string) error {
		t.Fatal("uncovered verified cause reached compilation")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	outcome := &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "first", Classification: "success", ConfirmedRootCauses: []*contracts.ConfirmedRootCause{{Summary: "routing fault", Resources: []*contracts.ObjectRef{t1Ref("api")}}}}
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
	if err != nil || event.Kind != lifecyclecore.ReflectionRejected || len(event.Diagnostics) != 1 || event.Diagnostics[0].Rule != "reflection.program-required" {
		t.Fatalf("README-only repair was not rejected: %+v %v", event, err)
	}
	next, _ := lifecyclecore.StepReflection(state, event)
	attempt, diagnostics := lifecyclecore.ReflectionFeedback(next)
	if next.ReflectionPhase != contracts.ReflectionPhase_REFLECTION_PHASE_RESET || attempt != 2 || len(diagnostics) != 1 || diagnostics[0].Rule != "reflection.program-required" {
		t.Fatalf("missing typed retry diagnostics: %v", next)
	}
}
