package broker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/validate"
)

func sharedMemoryBroker(t *testing.T, root string, runner Validator) *Broker {
	t.Helper()
	b, err := New(root, t.TempDir(), runner)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func reflectionProposal(t *testing.T, b *Broker, id string) Proposal {
	t.Helper()
	p, err := b.CreateProposalForPhase(context.Background(), id, ReflectionPhase)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func writeLearning(t *testing.T, p Proposal, id, text string) {
	t.Helper()
	path := filepath.Join(p.Path, ".sdo/playbooks", id, "README.md")
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	body := "---\nschema_version: 1\nowner: responder\nfault_class: " + id + "\noriginating_incident: synthetic\n---\n" + text + " <RESOURCE_NAME>\n"
	if err := os.WriteFile(path, []byte(body), 0644); err != nil {
		t.Fatal(err)
	}
}

func sharedReflectionFixture(t *testing.T) (string, *Broker, *Broker) {
	t.Helper()
	root := repository(t)
	memoryFixture(t, root)
	for _, id := range []string{"alpha", "beta"} {
		writeLearning(t, Proposal{Path: root}, id, "Original learning")
	}
	index := filepath.Join(root, ".sdo/playbooks/README.md")
	if err := os.WriteFile(index, []byte("# Playbooks\n\n- [base](base/README.md)\n- [alpha](alpha/README.md)\n- [beta](beta/README.md)\n"), 0644); err != nil {
		t.Fatal(err)
	}
	git(t, root, "add", ".")
	git(t, root, "commit", "-qm", "accepted synthetic playbooks")
	a := sharedMemoryBroker(t, root, func(context.Context, string, []string) error { return nil })
	b := sharedMemoryBroker(t, root, func(context.Context, string, []string) error { return nil })
	for _, id := range []string{"lane-a", "lane-b"} {
		if _, err := a.RecordOutcome(context.Background(), id, &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: id, Classification: "success"}); err != nil {
			t.Fatal(err)
		}
	}
	return root, a, b
}

func publishLearning(b *Broker, id, book string) (Receipt, error) {
	return b.PublishReflection(context.Background(), id, &contracts.ReflectionDecision{LearningDecision: "updated", PlaybookIds: []string{book}})
}

func TestConcurrentMemoryDisjointReflectionsValidateRebasedTree(t *testing.T) {
	root, a, b := sharedReflectionFixture(t)
	pa, pb := reflectionProposal(t, a, "lane-a"), reflectionProposal(t, b, "lane-b")
	writeLearning(t, pa, "alpha", "Alpha learning")
	writeLearning(t, pb, "beta", "Beta learning")
	git(t, pb.Path, "add", ".")
	git(t, pb.Path, "-c", "user.name=Agent", "-c", "user.email=agent@example.invalid", "commit", "-qm", "authored beta")
	first, err := publishLearning(a, "lane-a", "alpha")
	if err != nil {
		t.Fatal(err)
	}
	validated := false
	b.validator = func(_ context.Context, stage string, paths []string) error {
		validated = true
		for _, id := range []string{"alpha", "beta"} {
			if _, err := os.Stat(filepath.Join(stage, ".sdo/playbooks", id, "README.md")); err != nil {
				return err
			}
		}
		if head := git(t, stage, "rev-parse", "HEAD"); head != first.Commit {
			t.Errorf("validation base = %s, want %s", head, first.Commit)
		}
		if len(paths) != 1 || !strings.Contains(paths[0], "beta") {
			t.Errorf("foreign changes attributed to beta: %v", paths)
		}
		return nil
	}
	second, err := publishLearning(b, "lane-b", "beta")
	if err != nil {
		t.Fatal(err)
	}
	if !validated || second.Commit == "" || git(t, root, "rev-parse", "HEAD^") != first.Commit {
		t.Fatal("validated publications did not form a chain")
	}
}

func TestConcurrentMemoryConflictAndResolvedResubmit(t *testing.T) {
	root, a, b := sharedReflectionFixture(t)
	pa, pb := reflectionProposal(t, a, "lane-a"), reflectionProposal(t, b, "lane-b")
	writeLearning(t, pa, "base", "Alpha intent")
	writeLearning(t, pb, "base", "Beta intent")
	first, err := publishLearning(a, "lane-a", "base")
	if err != nil {
		t.Fatal(err)
	}
	_, err = publishLearning(b, "lane-b", "base")
	diagnostics, typed := RejectionDiagnostics(err)
	if !typed || len(diagnostics) == 0 || diagnostics[0].Rule != "memory.conflict" {
		t.Fatalf("expected typed MemoryConflict, got %v", err)
	}
	detail := diagnostics[0].GetMemoryConflict()
	if detail == nil || detail.GetBaseCommit() != pb.BaseCommit || detail.GetAcceptedCommit() != first.Commit || !slices.Contains(detail.GetConflictingPaths(), ".sdo/playbooks/base/README.md") || len(detail.GetAcceptedChanges()) != 1 || detail.GetAcceptedChanges()[0].GetIncidentId() != "lane-a" {
		t.Fatalf("incomplete typed conflict: %v", detail)
	}
	if !strings.Contains(diagnostics[0].Message, first.Commit) || !strings.Contains(diagnostics[0].Message, "lane-a") {
		t.Fatalf("conflict omitted accepted provenance: %v", diagnostics)
	}
	if head := git(t, root, "rev-parse", "HEAD"); head != first.Commit {
		t.Fatal("conflict changed accepted state")
	}
	if err := b.ReleaseProposal("lane-b", ReflectionPhase); err != nil {
		t.Fatal(err)
	}
	pb = reflectionProposal(t, b, "lane-b")
	writeLearning(t, pb, "base", "Alpha intent and Beta intent")
	if _, err := publishLearning(b, "lane-b", "base"); err != nil {
		t.Fatal(err)
	}
	if got := git(t, root, "show", "HEAD:.sdo/playbooks/base/README.md"); !strings.Contains(got, "Alpha intent and Beta intent") {
		t.Fatal(got)
	}
}

func TestConcurrentMemoryCleanReplayStillRequiresValidation(t *testing.T) {
	root, a, b := sharedReflectionFixture(t)
	pa, pb := reflectionProposal(t, a, "lane-a"), reflectionProposal(t, b, "lane-b")
	writeLearning(t, pa, "alpha", "Alpha learning")
	writeLearning(t, pb, "beta", "Beta learning")
	first, err := publishLearning(a, "lane-a", "alpha")
	if err != nil {
		t.Fatal(err)
	}
	b.validator = func(_ context.Context, stage string, _ []string) error {
		if _, err := os.Stat(filepath.Join(stage, ".sdo/playbooks/alpha/README.md")); err != nil {
			return err
		}
		return validate.Err([]validate.Diagnostic{{Rule: "synthetic.merged-tree", Message: "both playbooks violate the synthetic invariant"}})
	}
	_, err = publishLearning(b, "lane-b", "beta")
	ds, typed := RejectionDiagnostics(err)
	if !typed || len(ds) == 0 || ds[0].Rule != "memory.conflict" {
		t.Fatalf("merged validation failure lost conflict type: %v", err)
	}
	if got := ds[0].GetMemoryConflict().GetValidatorErrors(); len(got) != 1 || got[0].GetRule() != "synthetic.merged-tree" {
		t.Fatalf("typed validator errors lost: %v", got)
	}
	if !strings.Contains(err.Error(), "synthetic.merged-tree") {
		t.Fatalf("validator errors lost: %v", err)
	}
	var failure *validate.Failure
	if !errors.As(err, &failure) {
		t.Fatal("validator failure no longer inspectable")
	}
	if git(t, root, "rev-parse", "HEAD") != first.Commit {
		t.Fatal("invalid merged tree was accepted")
	}
}

func TestConcurrentMemorySeparateBrokerRootsNeverLoseAcceptedCommits(t *testing.T) {
	root, a, b := sharedReflectionFixture(t)
	pa, pb := reflectionProposal(t, a, "lane-a"), reflectionProposal(t, b, "lane-b")
	writeLearning(t, pa, "alpha", "Alpha learning")
	writeLearning(t, pb, "beta", "Beta learning")
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, lane := range []struct {
		b        *Broker
		id, book string
	}{{a, "lane-a", "alpha"}, {b, "lane-b", "beta"}} {
		wg.Add(1)
		go func() { defer wg.Done(); <-start; _, err := publishLearning(lane.b, lane.id, lane.book); errs <- err }()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Error(err)
		}
	}
	for _, id := range []string{"alpha", "beta"} {
		if _, err := os.Stat(filepath.Join(root, ".sdo/playbooks", id, "README.md")); err != nil {
			t.Error(err)
		}
	}
	if got := git(t, root, "log", "-2", "--format=%s"); !strings.Contains(got, "lane-a") || !strings.Contains(got, "lane-b") {
		t.Fatalf("lost accepted commit: %s", got)
	}
}

func TestConcurrentMemoryReplayPreservesSourceProjectionAndFileChanges(t *testing.T) {
	ctx := context.Background()
	root, a, b := sharedReflectionFixture(t)
	for name, body := range map[string]string{"remove.txt": "obsolete\n", "executable.sh": "#!/bin/sh\nexit 0\n"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(body), 0644); err != nil {
			t.Fatal(err)
		}
	}
	git(t, root, "add", ".")
	git(t, root, "commit", "-qm", "source fixture")
	pa, err := a.CreateProposal(ctx, "source-a")
	if err != nil {
		t.Fatal(err)
	}
	pb, err := b.CreateProposal(ctx, "source-b")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(pa.Path, "peer.txt"), []byte("accepted peer"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(pb.Path, "remove.txt")); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(pb.Path, "executable.sh"), 0755); err != nil {
		t.Fatal(err)
	}
	binary := []byte{0, 1, 2, 0, 255}
	if err := os.WriteFile(filepath.Join(pb.Path, "new.bin"), binary, 0644); err != nil {
		t.Fatal(err)
	}
	// An invalid working playbook remains isolated even when the source diff
	// must replay onto a newer accepted head.
	if err := os.WriteFile(filepath.Join(pb.Path, ".sdo/playbooks/base/diagnose.go"), []byte("invalid working program"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Publish(ctx, "source-a", Responder, ProposalPhase); err != nil {
		t.Fatal(err)
	}
	receipt, err := b.Publish(ctx, "source-b", Responder, ProposalPhase)
	if err != nil {
		t.Fatal(err)
	}
	if len(receipt.ChangedPaths) != 3 {
		t.Fatalf("source attribution leaked peer/draft changes: %v", receipt.ChangedPaths)
	}
	if _, err := os.Stat(filepath.Join(root, "remove.txt")); !os.IsNotExist(err) {
		t.Fatal("source deletion lost")
	}
	if info, err := os.Stat(filepath.Join(root, "executable.sh")); err != nil || info.Mode()&0111 == 0 {
		t.Fatal("executable mode lost")
	}
	if data, err := os.ReadFile(filepath.Join(root, "new.bin")); err != nil || string(data) != string(binary) {
		t.Fatal("binary/untracked source lost")
	}
	if _, err := os.Stat(filepath.Join(root, ".sdo/playbooks/base/diagnose.go")); !os.IsNotExist(err) {
		t.Fatal("unvalidated draft reached accepted memory")
	}
	if got := git(t, root, "show", "HEAD:peer.txt"); got != "accepted peer" {
		t.Fatal("source replay lost accepted peer")
	}
}

// A rejected proposal is carried onto the newest accepted head: hunks that
// merge cleanly are kept, and only colliding hunks are left as conflict
// markers, so a correction resolves the collision instead of redoing the work.
func TestConcurrentMemoryConflictRebasesRejectedProposal(t *testing.T) {
	ctx := context.Background()
	root, a, b := sharedReflectionFixture(t)
	pa, pb := reflectionProposal(t, a, "lane-a"), reflectionProposal(t, b, "lane-b")
	writeLearning(t, pa, "base", "Alpha intent")
	writeLearning(t, pb, "beta", "Beta extra")
	git(t, pb.Path, "add", ".")
	git(t, pb.Path, "-c", "user.name=Agent", "-c", "user.email=agent@example.invalid", "commit", "-qm", "authored beta")
	writeLearning(t, pb, "base", "Beta intent")
	first, err := publishLearning(a, "lane-a", "base")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := publishLearning(b, "lane-b", "base"); err == nil {
		t.Fatal("colliding proposal was accepted")
	}
	decision := []byte(`{"learning_decision":"updated","proposed_artifact_ids":["base","beta"]}`)
	if err := os.WriteFile(filepath.Join(pb.Path, ".git", validate.DecisionFile), decision, 0o600); err != nil {
		t.Fatal(err)
	}
	rebased, err := b.RebaseProposal(ctx, "lane-b", ReflectionPhase)
	if err != nil {
		t.Fatal(err)
	}
	if rebased.Path != pb.Path || rebased.BaseCommit != first.Commit || git(t, rebased.Path, "rev-parse", "HEAD") != first.Commit {
		t.Fatalf("proposal not rebased onto accepted head %s: %+v", first.Commit, rebased)
	}
	if info, err := b.ProposalInfo("lane-b", ReflectionPhase); err != nil || info != rebased {
		t.Fatalf("rebased proposal metadata = %+v, %v", info, err)
	}
	if kept, err := os.ReadFile(filepath.Join(rebased.Path, ".git", validate.DecisionFile)); err != nil || string(kept) != string(decision) {
		t.Fatalf("intended decision not kept: %s %v", kept, err)
	}
	if paths, err := b.ChangedPaths(ctx, "lane-b", ReflectionPhase); err != nil || !slices.Equal(paths, []string{".sdo/playbooks/base/README.md", ".sdo/playbooks/beta/README.md"}) {
		t.Fatalf("rebased changes = %v, %v", paths, err)
	}
	read := func(book string) string {
		data, err := os.ReadFile(filepath.Join(rebased.Path, ".sdo/playbooks", book, "README.md"))
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	if got := read("beta"); !strings.Contains(got, "Beta extra") || strings.Contains(got, "<<<<<<<") {
		t.Fatalf("clean hunk not carried forward: %s", got)
	}
	if got := read("base"); !strings.Contains(got, "<<<<<<<") || !strings.Contains(got, "Alpha intent") || !strings.Contains(got, "Beta intent") {
		t.Fatalf("colliding hunk not marked with both sides: %s", got)
	}
	both := &contracts.ReflectionDecision{LearningDecision: "updated", PlaybookIds: []string{"base", "beta"}}
	_, err = b.PublishReflection(ctx, "lane-b", both)
	if ds, _ := RejectionDiagnostics(err); err == nil || !slices.ContainsFunc(ds, func(d *contracts.MemoryDiagnostic) bool {
		return strings.Contains(d.GetMessage(), "conflict marker")
	}) {
		t.Fatalf("unresolved conflict marker was not rejected: %v", err)
	}
	writeLearning(t, rebased, "base", "Alpha intent and Beta intent")
	if again, err := b.RebaseProposal(ctx, "lane-b", ReflectionPhase); err != nil || again != rebased || !strings.Contains(read("base"), "Alpha intent and Beta intent") {
		t.Fatalf("rebase onto the same head is not a no-op: %+v %v", again, err)
	}
	accepted, err := b.PublishReflection(ctx, "lane-b", both)
	if err != nil {
		t.Fatal(err)
	}
	if git(t, root, "rev-parse", "HEAD^") != first.Commit || !strings.Contains(git(t, root, "show", accepted.Commit+":.sdo/playbooks/beta/README.md"), "Beta extra") {
		t.Fatal("carried learning was not accepted on top of the peer")
	}
}
