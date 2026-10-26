package broker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/validate"
	"testing"
)

func TestWorkingProgramDraftSurvivesIntoReflectionWithoutPublication(t *testing.T) {
	ctx := context.Background()
	root := repository(t)
	memoryFixture(t, root)
	b, err := New(root, t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	proposal, err := b.CreateProposal(ctx, "working-code")
	if err != nil {
		t.Fatal(err)
	}
	path := ".sdo/playbooks/base/diagnose.go"
	draft := []byte("package repair\ninvalid editing work in progress\n")
	if err := os.WriteFile(filepath.Join(proposal.Path, path), draft, 0644); err != nil {
		t.Fatal(err)
	}
	head := git(t, root, "rev-parse", "HEAD")
	receipt, err := b.Publish(ctx, "working-code", Responder, ProposalPhase)
	if err != nil {
		t.Fatalf("draft-only response must not try to publish memory as source: %v", err)
	}
	if receipt.Commit != "" || git(t, root, "rev-parse", "HEAD") != head {
		t.Fatal("unvalidated draft reached accepted state")
	}
	reflection, err := b.CreateProposalForPhase(ctx, "working-code", ReflectionPhase)
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(reflection.Path, path))
	if err != nil || string(got) != string(draft) {
		t.Fatalf("resumed reflection lost working program: %s %v", got, err)
	}
	// A controller restart rejoins the same reflection proposal, preserving refinements.
	refined := []byte("package repair\n// refined in the resumed session\n")
	if err := os.WriteFile(filepath.Join(reflection.Path, path), refined, 0644); err != nil {
		t.Fatal(err)
	}
	reopened, err := New(root, b.workspaceRoot, nil)
	if err != nil {
		t.Fatal(err)
	}
	again, err := reopened.CreateProposalForPhase(ctx, "working-code", ReflectionPhase)
	if err != nil {
		t.Fatal(err)
	}
	got, err = os.ReadFile(filepath.Join(again.Path, path))
	if err != nil || string(got) != string(refined) {
		t.Fatal("rejoin overwrote reflection edits")
	}
}

func TestSourcePublicationValidatesRepairsAndDefersWorkingPlaybooks(t *testing.T) {
	ctx := context.Background()
	root := repository(t)
	if err := os.CopyFS(filepath.Join(root, ".sdo"), os.DirFS("../memory/validate/testdata/good/.sdo")); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(root, ".sdo/playbooks/base/scripts/fix.sh"), 0755); err != nil {
		t.Fatal(err)
	}
	git(t, root, "add", ".")
	git(t, root, "commit", "-qm", "accepted memory")
	operatorRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	compile := func(ctx context.Context, root string, _ []string) error {
		diagnostics, err := validate.CompileCandidate(ctx, validate.Request{Root: root, OperatorRoot: operatorRoot})
		if err != nil {
			return err
		}
		return validate.Err(diagnostics)
	}
	b, err := New(root, t.TempDir(), compile)
	if err != nil {
		t.Fatal(err)
	}
	proposal, err := b.CreateProposal(ctx, "working-source")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(proposal.Path, "app.txt"), []byte("validated source repair\n"), 0644); err != nil {
		t.Fatal(err)
	}
	const path = ".sdo/playbooks/base/diagnose.go"
	if err := os.WriteFile(filepath.Join(proposal.Path, path), []byte("package repair\ninvalid working edit\n"), 0644); err != nil {
		t.Fatal(err)
	}
	receipt, err := b.Publish(ctx, "working-source", Responder, ProposalPhase)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Commit == "" || git(t, root, "show", "HEAD:app.txt") != "validated source repair" {
		t.Fatal("source repair was not validated and accepted")
	}
	if _, err := os.Stat(filepath.Join(root, path)); !os.IsNotExist(err) {
		t.Fatal("source publication included working memory")
	}
	outcome := &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "working-source", Classification: "success"}
	if _, err := b.RecordOutcome(ctx, "working-source", outcome); err != nil {
		t.Fatal(err)
	}
	reflection, err := b.CreateProposalForPhase(ctx, "working-source", ReflectionPhase)
	if err != nil {
		t.Fatal(err)
	}
	decision := &contracts.ReflectionDecision{LearningDecision: "updated", PlaybookIds: []string{"base"}}
	diagnostics, err := validate.Check(ctx, validate.Request{Root: reflection.Path, Base: reflection.BaseCommit, Actor: Responder, Phase: ReflectionPhase, Decision: decision, Publication: &validate.PublicationContext{BaseCommit: reflection.BaseCommit, IncidentID: "working-source"}}, func(ctx context.Context, request validate.Request) ([]validate.Diagnostic, error) {
		return validate.CompileCandidate(ctx, request)
	})
	if err != nil {
		t.Fatal(err)
	}
	syntax := false
	for _, d := range diagnostics {
		syntax = syntax || d.Rule == validate.RuleGoSyntax
	}
	if !syntax {
		t.Fatalf("memory check missed working syntax error: %+v", diagnostics)
	}
	before := git(t, root, "rev-parse", "HEAD")
	_, err = b.PublishReflection(ctx, "working-source", decision)
	var rejected *validate.Failure
	if !errors.As(err, &rejected) {
		t.Fatalf("broker did not reject invalid reflection edit: %v", err)
	}
	if !reflect.DeepEqual(diagnostics, rejected.Diagnostics) {
		t.Fatalf("memory check and broker disagree: %v / %v", diagnostics, rejected.Diagnostics)
	}
	if git(t, root, "rev-parse", "HEAD") != before {
		t.Fatal("rejected reflection changed operational branch")
	}
}
